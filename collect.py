#!/usr/bin/env python3
"""
Alpine Pulse — Data Collector v2
Collects mentions of Fortress, Castle Mountain, Nakiska, Grande Cache,
and David Thompson Region from:
  - YouTube (free Data API v3)
  - Google News RSS
  - RSS.app feeds (social media aggregation)
  - Custom RSS feeds

Then uses Claude API for sentiment analysis + theme categorization.
Outputs a JSON data file consumed by the dashboard.

Schedule this to run Mon-Fri at 6:00 AM via GitHub Actions.

NOTE: Reddit integration removed for now. To re-enable later,
uncomment the Reddit sections marked with "# REDDIT:" comments.
"""

import os
import sys
import json
import re
import time
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --- CONFIGURATION -----------------------------------------------------------

CONFIG = {
    # ---- API Keys (set as environment variables or GitHub Secrets) ----
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),
    "YOUTUBE_API_KEY": os.environ.get("YOUTUBE_API_KEY", ""),

    # REDDIT: Uncomment these when you have Reddit API access
    # "REDDIT_CLIENT_ID": os.environ.get("REDDIT_CLIENT_ID", ""),
    # "REDDIT_CLIENT_SECRET": os.environ.get("REDDIT_CLIENT_SECRET", ""),
    # "REDDIT_USER_AGENT": os.environ.get("REDDIT_USER_AGENT", "AlpinePulse/1.0"),

    # ---- Email settings ----
    "EMAIL_ENABLED": os.environ.get("EMAIL_ENABLED", "true").lower() == "true",
    "SMTP_SERVER": os.environ.get("SMTP_SERVER", "smtp.gmail.com"),
    "SMTP_PORT": int(os.environ.get("SMTP_PORT", "587")),
    "SMTP_USER": os.environ.get("SMTP_USER", ""),
    "SMTP_PASSWORD": os.environ.get("SMTP_PASSWORD", ""),
    "EMAIL_TO": os.environ.get("EMAIL_TO", ""),

    # ---- Locations & search terms ----
    "RESORTS": {
        "fortress": {
            "name": "Fortress Mountain",
            "search_terms": [
                "Fortress Mountain", "Fortress ski", "Fortress resort",
                "Fortress Mountain Alberta"
            ],
        },
        "castle": {
            "name": "Castle Mountain",
            "search_terms": [
                "Castle Mountain Resort", "Castle Mountain ski",
                "Castle Mountain Alberta", "Castle ski area"
            ],
        },
        "nakiska": {
            "name": "Nakiska",
            "search_terms": [
                "Nakiska", "Nakiska ski", "Nakiska resort",
                "Nakiska Kananaskis"
            ],
        },
        "grandecache": {
            "name": "Grande Cache",
            "search_terms": [
                "Grande Cache Alberta", "Grande Cache trails",
                "Grande Cache recreation", "Grande Cache tourism",
                "Grande Cache outdoor"
            ],
        },
        "davidthompson": {
            "name": "David Thompson Region",
            "search_terms": [
                "David Thompson Alberta", "Clearwater County tourism",
                "Nordegg Alberta", "White Goat wilderness",
                "Saunders tourism node", "Abraham Lake recreation"
            ],
        },
    },

    # ---- Themes to categorize ----
    "THEMES": [
        "Snow Conditions",
        "Pricing & Value",
        "Summer Activities",
        "Staff & Service",
        "Lift Wait Times",
        "Trails & Recreation",
        "Facilities & Lodging",
        "Access & Transportation",
        "Environmental Impact",
        "Safety & Incidents",
        "Events & Promotions",
        "Family & Beginner Experience",
    ],

    # ---- Government of Alberta themes ----
    "GOV_THEMES": [
        "Regulatory Approvals & Land Use",
        "Tourism Strategy & Policy",
        "Environmental Assessment & Compliance",
    ],

    # ---- RSS Feeds ----
    "RSS_FEEDS": [
        # --- RSS.app feeds (social media + web aggregation) ---
        "https://rss.app/rss-feed?keyword=All%20Season%20resorts%20Alberta&region=US&lang=en",
        "https://rss.app/rss-feed?keyword=Fortress%20Mountain&region=US&lang=en",
        "https://rss.app/rss-feed?keyword=Nakiska&region=US&lang=en",
        "https://rss.app/rss-feed?keyword=Grande%20Cache&region=US&lang=en",
        "https://rss.app/rss-feed?keyword=Castle%20mountain&region=US&lang=en",
        "https://rss.app/rss-feed?keyword=Nordegg&region=US&lang=en",

        # --- Google News RSS ---
        "https://news.google.com/rss/search?q=%22Fortress+Mountain%22+Alberta&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Castle+Mountain+Resort%22+Alberta&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Nakiska%22+ski+Alberta&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22All+Season+Resorts%22+Alberta&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Grande+Cache%22+Alberta+tourism+recreation&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Nordegg%22+OR+%22David+Thompson%22+Alberta+tourism&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Clearwater+County%22+Alberta+tourism+recreation&hl=en-CA&gl=CA",

        # --- Government of Alberta RSS ---
        "https://news.google.com/rss/search?q=%22Government+of+Alberta%22+tourism+sport&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Alberta+Tourism%22+resort+regulatory&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Alberta+Environment%22+resort+assessment&hl=en-CA&gl=CA",
    ],

    # ---- File paths ----
    "DATA_DIR": os.environ.get("DATA_DIR", str(Path(__file__).parent / "data")),
    "LOOKBACK_HOURS": 26,  # covers overnight + prior evening
}

# --- UTILITY FUNCTIONS -------------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def make_id(text):
    return hashlib.md5(text.encode()).hexdigest()[:12]

def is_workday():
    return datetime.now().weekday() < 5

def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def detect_resort(text, config):
    """Determine which location a piece of text is about."""
    text_lower = text.lower()
    # Check each resort's search terms
    for rk, ri in config["RESORTS"].items():
        for term in ri["search_terms"]:
            if term.lower() in text_lower:
                return rk
    # Fallback keyword matching
    if any(w in text_lower for w in ["grande cache"]):
        return "grandecache"
    if any(w in text_lower for w in ["nordegg", "clearwater", "david thompson", "white goat", "saunders", "abraham lake"]):
        return "davidthompson"
    if any(w in text_lower for w in ["fortress"]):
        return "fortress"
    if any(w in text_lower for w in ["castle"]):
        return "castle"
    if any(w in text_lower for w in ["nakiska", "kananaskis"]):
        return "nakiska"
    return "fortress"  # default

def is_gov_related(text):
    """Check if text relates to Government of Alberta."""
    text_lower = text.lower()
    gov_keywords = [
        "government of alberta", "alberta government", "alberta tourism",
        "ministry of tourism", "tourism and sport", "alberta environment",
        "alberta parks", "regulatory", "land use bylaw", "environmental assessment",
        "environmental impact", "crown land", "public consultation", "tourism levy",
        "tourism strategy", "alberta policy", "minister of tourism",
        "protected areas", "provincial government"
    ]
    return any(k in text_lower for k in gov_keywords)

# --- YOUTUBE COLLECTOR -------------------------------------------------------

def collect_youtube(config):
    """Collect mentions from YouTube using the free Data API v3."""
    import requests

    mentions = []
    api_key = config["YOUTUBE_API_KEY"]

    if not api_key:
        log("⚠ YouTube: No API key configured, skipping.")
        return mentions

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=config["LOOKBACK_HOURS"])).strftime("%Y-%m-%dT%H:%M:%SZ")

    for resort_key, resort_info in config["RESORTS"].items():
        for term in resort_info["search_terms"][:2]:  # limit to avoid quota
            try:
                url = "https://www.googleapis.com/youtube/v3/search"
                params = {
                    "part": "snippet",
                    "q": term,
                    "type": "video",
                    "publishedAfter": cutoff,
                    "maxResults": 10,
                    "order": "date",
                    "key": api_key,
                }
                res = requests.get(url, params=params, timeout=10)
                if res.status_code != 200:
                    log(f"⚠ YouTube API error {res.status_code} for '{term}'")
                    continue

                for item in res.json().get("items", []):
                    snippet = item["snippet"]
                    text = f"{snippet['title']} {snippet.get('description', '')[:300]}"
                    video_id = item["id"].get("videoId", "")

                    mentions.append({
                        "id": make_id(video_id),
                        "source": "YouTube",
                        "resort": resort_key,
                        "text": text.strip(),
                        "url": f"https://youtube.com/watch?v={video_id}",
                        "date": snippet["publishedAt"],
                        "engagement": "Video",
                        "author": snippet.get("channelTitle", ""),
                    })
                time.sleep(0.5)
            except Exception as e:
                log(f"⚠ YouTube search error ({term}): {e}")
                continue

    log(f"✓ YouTube: collected {len(mentions)} mentions")
    return mentions

# --- RSS / NEWS COLLECTOR ----------------------------------------------------

def collect_rss(config):
    """Collect from Google News RSS, RSS.app feeds, and custom RSS feeds."""
    import requests
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    mentions = []

    for feed_url in config["RSS_FEEDS"]:
        try:
            res = requests.get(
                feed_url,
                timeout=15,
                headers={"User-Agent": "AlpinePulse/2.0 (sentiment monitor)"}
            )
            if res.status_code != 200:
                log(f"⚠ RSS feed returned {res.status_code}: {feed_url[:60]}...")
                continue

            root = ET.fromstring(res.content)

            # Handle RSS 2.0
            for item in root.findall(".//item"):
                title = item.findtext("title", "")
                description = item.findtext("description", "")
                link = item.findtext("link", "")
                pub_date = item.findtext("pubDate", "")
                source_el = item.find("source")
                source_name = source_el.text if source_el is not None and source_el.text else ""

                # Clean HTML from description
                description = re.sub(r'<[^>]+>', '', description)[:500]

                # Determine source display name
                if not source_name:
                    if "rss.app" in feed_url:
                        source_name = "Social/Web"
                    elif "news.google" in feed_url:
                        source_name = "News"
                    else:
                        source_name = "RSS"

                text = f"{title} {description}".strip()
                if not text:
                    continue

                # Detect which resort
                resort_key = detect_resort(text, config)

                # Parse date
                date_str = ""
                if pub_date:
                    try:
                        parsed = parsedate_to_datetime(pub_date)
                        date_str = parsed.isoformat()
                    except Exception:
                        date_str = pub_date

                mentions.append({
                    "id": make_id(link or title),
                    "source": source_name,
                    "resort": resort_key,
                    "text": text[:500],
                    "url": link,
                    "date": date_str,
                    "engagement": "Article" if "news" in feed_url.lower() else "Web mention",
                    "author": source_name,
                    "is_gov": is_gov_related(text),
                })

            log(f"  ✓ RSS feed loaded: {feed_url[:60]}...")
            time.sleep(0.5)  # be polite

        except ET.ParseError as e:
            log(f"⚠ RSS parse error ({feed_url[:50]}...): {e}")
            continue
        except Exception as e:
            log(f"⚠ RSS error ({feed_url[:50]}...): {e}")
            continue

    log(f"✓ RSS/News: collected {len(mentions)} total mentions")
    return mentions

# --- SENTIMENT ANALYSIS VIA CLAUDE ------------------------------------------

def analyze_mentions(mentions, config):
    """Use Claude API for sentiment + theme categorization."""
    import requests

    api_key = config["ANTHROPIC_API_KEY"]
    if not api_key:
        log("⚠ Claude API: No key configured. Using rule-based fallback.")
        return fallback_analysis(mentions, config)

    analyzed = []
    batch_size = 15

    for i in range(0, len(mentions), batch_size):
        batch = mentions[i:i + batch_size]
        batch_texts = []
        for j, m in enumerate(batch):
            gov_flag = " [GOV-RELATED]" if m.get("is_gov") else ""
            batch_texts.append(
                f"[{j}] SOURCE: {m['source']} | LOCATION: {m['resort']}{gov_flag} | TEXT: {m['text'][:400]}"
            )

        all_themes = config['THEMES'] + config['GOV_THEMES']

        prompt = f"""You are analyzing mentions about Alberta tourism locations (Fortress Mountain, Castle Mountain Resort, Nakiska, Grande Cache, and the David Thompson Region/Nordegg/Clearwater County) for an executive briefing.

Some mentions are flagged [GOV-RELATED] — these relate to Government of Alberta policy, regulatory decisions, or tourism strategy. For those, prefer using one of these government themes: {json.dumps(config['GOV_THEMES'])}

For each mention below, provide:
1. sentiment: "positive", "neutral", or "negative"
2. sentiment_score: 0-100 (0=very negative, 50=neutral, 100=very positive)
3. theme: The best matching theme from: {json.dumps(all_themes)}
4. takeaway: A 1-sentence executive summary of the key point

MENTIONS:
{chr(10).join(batch_texts)}

Respond ONLY with a JSON array. Each element: index, sentiment, sentiment_score, theme, takeaway.
No markdown, no explanation — just the JSON array."""

        try:
            res = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 2000,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=30,
            )

            if res.status_code != 200:
                log(f"⚠ Claude API error: {res.status_code} — {res.text[:200]}")
                analyzed.extend(fallback_analysis(batch, config))
                continue

            response_text = res.json()["content"][0]["text"]
            response_text = response_text.strip()
            if response_text.startswith("```"):
                response_text = re.sub(r"```json?\s*", "", response_text)
                response_text = response_text.rstrip("`").strip()

            results = json.loads(response_text)

            for result in results:
                idx = result["index"]
                if idx < len(batch):
                    mention = batch[idx].copy()
                    mention["sentiment"] = result["sentiment"]
                    mention["sentiment_score"] = result["sentiment_score"]
                    mention["theme"] = result["theme"]
                    mention["takeaway"] = result["takeaway"]
                    analyzed.append(mention)

            time.sleep(1)

        except Exception as e:
            log(f"⚠ Claude analysis error: {e}")
            analyzed.extend(fallback_analysis(batch, config))

    log(f"✓ Analyzed {len(analyzed)} mentions")
    return analyzed


def fallback_analysis(mentions, config):
    """Simple keyword-based fallback when Claude API is unavailable."""
    positive_words = {"great","amazing","love","best","incredible","awesome","excellent",
        "fantastic","perfect","beautiful","pristine","fresh","powder","recommend",
        "friendly","patient","spectacular","hidden gem","underrated","well maintained"}
    negative_words = {"bad","terrible","worst","wait","crowded","overpriced","expensive",
        "broken","closed","dangerous","icy","rough","complained","poor","rude","slow",
        "lost","frustrating","pothole","limited"}

    results = []
    for m in mentions:
        text_lower = m["text"].lower()
        pos = sum(1 for w in positive_words if w in text_lower)
        neg = sum(1 for w in negative_words if w in text_lower)

        if pos > neg:
            sentiment, score = "positive", min(50 + pos * 12, 95)
        elif neg > pos:
            sentiment, score = "negative", max(50 - neg * 12, 5)
        else:
            sentiment, score = "neutral", 50

        # Theme detection
        theme = "Trails & Recreation"  # default
        theme_keywords = {
            "Snow Conditions": ["snow","powder","conditions","fresh","base","coverage","grooming"],
            "Pricing & Value": ["price","cost","expensive","cheap","value","pass","ticket","afford"],
            "Summer Activities": ["summer","hiking","biking","mountain bike","camping"],
            "Staff & Service": ["staff","instructor","service","friendly","rude","helpful"],
            "Lift Wait Times": ["wait","line","queue","lift","crowded","busy","chair"],
            "Facilities & Lodging": ["lodge","food","restaurant","hotel","parking","washroom","burger"],
            "Trails & Recreation": ["trail","hike","path","recreation","outdoor","backcountry","route"],
            "Access & Transportation": ["road","drive","access","highway","shuttle","pothole","signage"],
            "Environmental Impact": ["environment","wildlife","ecosystem","sustainable","assessment"],
            "Safety & Incidents": ["accident","injury","rescue","closed","avalanche","danger"],
            "Events & Promotions": ["event","festival","promotion","discount","deal","night ski"],
            "Family & Beginner Experience": ["family","kids","beginner","lesson","learn","children"],
            "Regulatory Approvals & Land Use": ["regulatory","bylaw","land use","permit","zoning","crown land","approval"],
            "Tourism Strategy & Policy": ["tourism strategy","tourism levy","ministry","government","policy","consultation"],
            "Environmental Assessment & Compliance": ["environmental assessment","impact assessment","compliance","alberta environment"],
        }
        for t, keywords in theme_keywords.items():
            if any(k in text_lower for k in keywords):
                theme = t
                break

        mention = m.copy()
        mention["sentiment"] = sentiment
        mention["sentiment_score"] = score
        mention["theme"] = theme
        mention["takeaway"] = m["text"][:120]
        results.append(mention)

    return results

# --- AGGREGATION & DASHBOARD DATA -------------------------------------------

def build_dashboard_data(analyzed_mentions, config):
    """Build the JSON structure consumed by the dashboard."""

    today_str = datetime.now().strftime("%Y-%m-%d")
    data_dir = config["DATA_DIR"]
    ensure_dir(data_dir)

    # Load history
    history_file = os.path.join(data_dir, "history.json")
    if os.path.exists(history_file):
        with open(history_file) as f:
            history = json.load(f)
    else:
        history = {"daily": []}

    # Today's stats
    total = len(analyzed_mentions)
    if total == 0:
        log("⚠ No mentions to build dashboard from.")
        total = 1  # avoid division by zero

    pos_count = sum(1 for m in analyzed_mentions if m.get("sentiment") == "positive")
    neu_count = sum(1 for m in analyzed_mentions if m.get("sentiment") == "neutral")
    neg_count = sum(1 for m in analyzed_mentions if m.get("sentiment") == "negative")

    pos_pct = round(pos_count / total * 100)
    neu_pct = round(neu_count / total * 100)
    neg_pct = max(0, 100 - pos_pct - neu_pct)

    # Per-location breakdown
    resort_stats = {}
    for rk, ri in config["RESORTS"].items():
        rm = [m for m in analyzed_mentions if m.get("resort") == rk]
        rt = len(rm) or 1
        rp = sum(1 for m in rm if m.get("sentiment") == "positive")
        rn_neu = sum(1 for m in rm if m.get("sentiment") == "neutral")
        rn_neg = sum(1 for m in rm if m.get("sentiment") == "negative")
        resort_stats[rk] = {
            "name": ri["name"],
            "total": len(rm),
            "positive_pct": round(rp / rt * 100),
            "neutral_pct": round(rn_neu / rt * 100),
            "negative_pct": round(rn_neg / rt * 100),
        }

    # Theme breakdown
    all_themes = config["THEMES"] + config["GOV_THEMES"]
    theme_data = {}
    for theme in all_themes:
        tm = [m for m in analyzed_mentions if m.get("theme") == theme]
        if not tm:
            continue
        avg_score = sum(m.get("sentiment_score", 50) for m in tm) / len(tm)
        sentiment = "positive" if avg_score >= 60 else "negative" if avg_score <= 40 else "neutral"
        theme_data[theme] = {
            "mentions": len(tm),
            "avg_score": round(avg_score),
            "sentiment": sentiment,
        }

    sorted_themes = sorted(theme_data.items(), key=lambda x: x[1]["mentions"], reverse=True)

    # Government of Alberta items
    gov_mentions = [m for m in analyzed_mentions if m.get("is_gov") or m.get("theme") in config["GOV_THEMES"]]
    gov_items = []
    for gm in gov_mentions[:10]:
        gov_items.append({
            "title": gm.get("takeaway", gm["text"][:80]),
            "detail": gm["text"][:300],
            "theme": gm.get("theme", "Tourism Strategy & Policy"),
            "date": gm.get("date", ""),
            "sentiment": gm.get("sentiment", "neutral"),
        })

    # History
    today_entry = {
        "date": today_str,
        "total": len(analyzed_mentions),
        "positive_pct": pos_pct,
        "neutral_pct": neu_pct,
        "negative_pct": neg_pct,
        "themes": {k: v["mentions"] for k, v in theme_data.items()},
    }

    history["daily"] = [d for d in history["daily"] if d["date"] != today_str]
    history["daily"].append(today_entry)
    history["daily"] = history["daily"][-30:]

    # Trend data (last 7 days)
    last_7 = history["daily"][-7:]
    trend_labels = []
    for d in last_7:
        try:
            dt = datetime.strptime(d["date"], "%Y-%m-%d")
            trend_labels.append(dt.strftime("%a"))
        except Exception:
            trend_labels.append(d["date"][-5:])

    trend_pos = [d["positive_pct"] for d in last_7]
    trend_neu = [d["neutral_pct"] for d in last_7]
    trend_neg = [d["negative_pct"] for d in last_7]

    # Theme trends (last 7 days)
    top_theme_keys = [k for k, v in sorted_themes[:4]]
    theme_trends = {}
    for tk in top_theme_keys:
        theme_trends[tk] = [d.get("themes", {}).get(tk, 0) for d in last_7]

    # Feed (most recent first)
    feed = sorted(analyzed_mentions, key=lambda m: m.get("date", ""), reverse=True)[:50]
    feed_clean = []
    for m in feed:
        feed_clean.append({
            "source": m.get("source", "Unknown"),
            "resort": m.get("resort", "fortress"),
            "sentiment": m.get("sentiment", "neutral"),
            "text": m.get("text", "")[:300],
            "date": m.get("date", ""),
            "engagement": m.get("engagement", ""),
            "theme": m.get("theme", ""),
            "url": m.get("url", ""),
        })

    # Build final dashboard JSON
    dashboard = {
        "generated_at": datetime.now().isoformat(),
        "date": today_str,
        "summary": {
            "total_mentions": len(analyzed_mentions),
            "positive_pct": pos_pct,
            "neutral_pct": neu_pct,
            "negative_pct": neg_pct,
            "negative_count": neg_count,
        },
        "resort_stats": resort_stats,
        "themes": [{"name": k, **v} for k, v in sorted_themes],
        "gov_alberta": gov_items,
        "trends": {
            "labels": trend_labels,
            "positive": trend_pos,
            "neutral": trend_neu,
            "negative": trend_neg,
        },
        "theme_trends": theme_trends,
        "feed": feed_clean,
    }

    # Save
    dashboard_file = os.path.join(data_dir, "dashboard.json")
    with open(dashboard_file, "w") as f:
        json.dump(dashboard, f, indent=2)

    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)

    log(f"✓ Dashboard data saved: {dashboard_file}")
    return dashboard

# --- EMAIL BRIEFING ----------------------------------------------------------

def send_email_briefing(dashboard, config):
    """Send a formatted HTML email summary."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    if not config["EMAIL_ENABLED"]:
        log("⚠ Email disabled, skipping.")
        return
    if not config["SMTP_USER"] or not config["EMAIL_TO"]:
        log("⚠ Email: SMTP_USER or EMAIL_TO not configured, skipping.")
        return

    s = dashboard["summary"]
    date_str = datetime.now().strftime("%A, %B %d, %Y")

    # Theme rows
    theme_rows = ""
    for t in dashboard["themes"][:8]:
        color = "#34d399" if t["sentiment"]=="positive" else "#f87171" if t["sentiment"]=="negative" else "#94a3b8"
        theme_rows += f"""<tr>
          <td style="padding:8px 12px;border-bottom:1px solid #1e293b;color:#f1f5f9;font-weight:500">{t['name']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #1e293b;color:#94a3b8;text-align:center">{t['mentions']}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #1e293b;text-align:center">
            <span style="color:{color}">{t['sentiment'].title()}</span></td></tr>"""

    # Resort rows
    resort_rows = ""
    for rk, rs in dashboard["resort_stats"].items():
        resort_rows += f"""<tr>
          <td style="padding:6px 12px;border-bottom:1px solid #1e293b;color:#f1f5f9">{rs['name']}</td>
          <td style="padding:6px 12px;border-bottom:1px solid #1e293b;color:#94a3b8;text-align:center">{rs['total']}</td>
          <td style="padding:6px 12px;border-bottom:1px solid #1e293b;color:#34d399;text-align:center">{rs['positive_pct']}%</td>
          <td style="padding:6px 12px;border-bottom:1px solid #1e293b;color:#f87171;text-align:center">{rs['negative_pct']}%</td></tr>"""

    # Gov Alberta items
    gov_rows = ""
    for g in dashboard.get("gov_alberta", [])[:3]:
        gov_rows += f"""<tr><td style="padding:8px 12px;border-bottom:1px solid #1e293b">
          <div style="color:#fca5a5;font-size:11px;text-transform:uppercase;letter-spacing:1px">{g['theme']}</div>
          <div style="color:#e2e8f0;font-size:13px;margin-top:4px">{g['title']}</div></td></tr>"""

    # Feed items
    feed_rows = ""
    for item in dashboard["feed"][:6]:
        bar_color = "#34d399" if item["sentiment"]=="positive" else "#f87171" if item["sentiment"]=="negative" else "#94a3b8"
        feed_rows += f"""<tr><td style="padding:8px 12px;border-bottom:1px solid #1e293b;width:4px">
          <div style="width:4px;height:30px;background:{bar_color};border-radius:2px"></div></td>
          <td style="padding:8px 12px;border-bottom:1px solid #1e293b">
          <div style="color:#64748b;font-size:10px;text-transform:uppercase">{item['source']} · {item['resort'].upper()}</div>
          <div style="color:#e2e8f0;font-size:13px;margin-top:3px">{item['text'][:160]}...</div></td></tr>"""

    html = f"""<!DOCTYPE html><html><body style="margin:0;padding:0;background:#0B1120;font-family:Arial,sans-serif">
    <div style="max-width:600px;margin:0 auto;padding:28px 16px">
      <div style="text-align:center;margin-bottom:28px">
        <div style="font-size:24px;margin-bottom:6px">⛰</div>
        <h1 style="font-size:22px;color:#f1f5f9;margin:0">Alpine Pulse</h1>
        <p style="color:#64748b;font-size:11px;margin:4px 0 0;letter-spacing:1px;text-transform:uppercase">Daily Briefing</p>
        <p style="color:#94a3b8;font-size:13px;margin:6px 0 0">{date_str}</p>
      </div>
      <table style="width:100%;border-collapse:collapse;margin-bottom:20px"><tr>
        <td style="width:25%;text-align:center;padding:16px;background:#111827;border-radius:10px 0 0 10px;border-top:2px solid #38bdf8">
          <div style="color:#64748b;font-size:9px;text-transform:uppercase;letter-spacing:1px">Mentions</div>
          <div style="color:#f1f5f9;font-size:24px;font-weight:800;margin-top:3px">{s['total_mentions']}</div></td>
        <td style="width:25%;text-align:center;padding:16px;background:#111827;border-top:2px solid #34d399">
          <div style="color:#64748b;font-size:9px;text-transform:uppercase;letter-spacing:1px">Positive</div>
          <div style="color:#34d399;font-size:24px;font-weight:800;margin-top:3px">{s['positive_pct']}%</div></td>
        <td style="width:25%;text-align:center;padding:16px;background:#111827;border-top:2px solid #94a3b8">
          <div style="color:#64748b;font-size:9px;text-transform:uppercase;letter-spacing:1px">Neutral</div>
          <div style="color:#94a3b8;font-size:24px;font-weight:800;margin-top:3px">{s['neutral_pct']}%</div></td>
        <td style="width:25%;text-align:center;padding:16px;background:#111827;border-radius:0 10px 10px 0;border-top:2px solid #f87171">
          <div style="color:#64748b;font-size:9px;text-transform:uppercase;letter-spacing:1px">Negative</div>
          <div style="color:#f87171;font-size:24px;font-weight:800;margin-top:3px">{s['negative_pct']}%</div></td>
      </tr></table>
      <div style="background:#111827;border-radius:10px;padding:16px;margin-bottom:16px;border:1px solid #1e293b">
        <h2 style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin:0 0 12px">Locations</h2>
        <table style="width:100%;border-collapse:collapse"><tr>
          <th style="text-align:left;padding:6px 12px;color:#64748b;font-size:9px;text-transform:uppercase;border-bottom:1px solid #1e293b">Location</th>
          <th style="text-align:center;padding:6px 12px;color:#64748b;font-size:9px;text-transform:uppercase;border-bottom:1px solid #1e293b">Total</th>
          <th style="text-align:center;padding:6px 12px;color:#64748b;font-size:9px;text-transform:uppercase;border-bottom:1px solid #1e293b">Pos</th>
          <th style="text-align:center;padding:6px 12px;color:#64748b;font-size:9px;text-transform:uppercase;border-bottom:1px solid #1e293b">Neg</th>
        </tr>{resort_rows}</table></div>
      <div style="background:#111827;border-radius:10px;padding:16px;margin-bottom:16px;border:1px solid #1e293b">
        <h2 style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin:0 0 12px">Themes</h2>
        <table style="width:100%;border-collapse:collapse">{theme_rows}</table></div>
      {"<div style='background:linear-gradient(135deg,rgba(248,113,113,0.08),rgba(251,191,36,0.08));border-radius:10px;padding:16px;margin-bottom:16px;border:1px solid rgba(248,113,113,0.2)'><h2 style='color:#fca5a5;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin:0 0 12px'>🏛 Government of Alberta</h2><table style=\"width:100%;border-collapse:collapse\">" + gov_rows + "</table></div>" if gov_rows else ""}
      <div style="background:#111827;border-radius:10px;padding:16px;margin-bottom:16px;border:1px solid #1e293b">
        <h2 style="color:#94a3b8;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin:0 0 12px">Notable Mentions</h2>
        <table style="width:100%;border-collapse:collapse">{feed_rows}</table></div>
      <div style="text-align:center;padding:16px;color:#64748b;font-size:10px">
        Alpine Pulse — All Season Resorts Alberta<br>Generated {datetime.now().strftime('%I:%M %p MT')}</div>
    </div></body></html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"⛰ Alpine Pulse — {date_str} | {s['positive_pct']}% Positive · {s['total_mentions']} Mentions"
        msg["From"] = config["SMTP_USER"]
        msg["To"] = config["EMAIL_TO"]
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(config["SMTP_SERVER"], config["SMTP_PORT"]) as server:
            server.starttls()
            server.login(config["SMTP_USER"], config["SMTP_PASSWORD"])
            server.sendmail(config["SMTP_USER"], config["EMAIL_TO"].split(","), msg.as_string())

        log(f"✓ Email sent to {config['EMAIL_TO']}")
    except Exception as e:
        log(f"⚠ Email error: {e}")

# --- MAIN --------------------------------------------------------------------

def main():
    log("=" * 60)
    log("Alpine Pulse v2 — Starting daily collection")
    log("=" * 60)

    if not is_workday():
        log("Weekend detected. Use --force to override.")
        if "--force" not in sys.argv:
            return

    # Collect from all sources
    all_mentions = []
    all_mentions.extend(collect_youtube(CONFIG))
    all_mentions.extend(collect_rss(CONFIG))

    # REDDIT: Uncomment this when you have Reddit API access
    # all_mentions.extend(collect_reddit(CONFIG))

    # Deduplicate
    seen = set()
    unique = []
    for m in all_mentions:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique.append(m)
    all_mentions = unique

    log(f"Total unique mentions: {len(all_mentions)}")

    if not all_mentions:
        log("No mentions found. Check API keys and RSS feed URLs.")
        return

    # Analyze
    analyzed = analyze_mentions(all_mentions, CONFIG)

    # Build dashboard
    dashboard = build_dashboard_data(analyzed, CONFIG)

    # Send email
    send_email_briefing(dashboard, CONFIG)

    log("=" * 60)
    log("✓ Daily collection complete!")
    log("=" * 60)


if __name__ == "__main__":
    main()
