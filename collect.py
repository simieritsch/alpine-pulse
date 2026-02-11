#!/usr/bin/env python3
"""
Alpine Pulse — Data Collector
Collects mentions of Fortress, Castle Mountain, and Nakiska from:
  - Reddit (free API via requests)
  - YouTube (free Data API v3)
  - Google News RSS
  - Custom RSS feeds (local news, ski blogs)
  - Google Alerts RSS (if configured)

Then uses Claude API for sentiment analysis + theme categorization.
Outputs a JSON data file consumed by the dashboard.

Schedule this to run Mon-Fri at 6:00 AM via cron, GitHub Actions, or PythonAnywhere.
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
    # ---- API Keys (set these as environment variables) ----
    "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),
    "YOUTUBE_API_KEY": os.environ.get("YOUTUBE_API_KEY", ""),       # Free: https://console.cloud.google.com
    "REDDIT_CLIENT_ID": os.environ.get("REDDIT_CLIENT_ID", ""),     # Free: https://www.reddit.com/prefs/apps
    "REDDIT_CLIENT_SECRET": os.environ.get("REDDIT_CLIENT_SECRET", ""),
    "REDDIT_USER_AGENT": os.environ.get("REDDIT_USER_AGENT", "AlpinePulse/1.0"),

    # ---- Email settings (for daily briefing) ----
    "EMAIL_ENABLED": os.environ.get("EMAIL_ENABLED", "true").lower() == "true",
    "SMTP_SERVER": os.environ.get("SMTP_SERVER", "smtp.gmail.com"),
    "SMTP_PORT": int(os.environ.get("SMTP_PORT", "587")),
    "SMTP_USER": os.environ.get("SMTP_USER", ""),
    "SMTP_PASSWORD": os.environ.get("SMTP_PASSWORD", ""),           # Gmail: use App Password
    "EMAIL_TO": os.environ.get("EMAIL_TO", ""),

    # ---- Search terms ----
    "RESORTS": {
        "fortress": {
            "name": "Fortress Mountain",
            "search_terms": [
                "Fortress Mountain", "Fortress ski", "Fortress resort",
                "Fortress Mountain Alberta", "Fortress Mountain ski"
            ],
            "subreddits": ["skiing", "Calgary", "alberta", "snowboarding", "backcountry"],
        },
        "castle": {
            "name": "Castle Mountain",
            "search_terms": [
                "Castle Mountain Resort", "Castle Mountain ski",
                "Castle Mountain Alberta", "Castle ski area",
                "Pincher Creek ski"
            ],
            "subreddits": ["skiing", "Calgary", "alberta", "snowboarding", "Lethbridge"],
        },
        "nakiska": {
            "name": "Nakiska",
            "search_terms": [
                "Nakiska", "Nakiska ski", "Nakiska resort",
                "Nakiska ski area", "Nakiska Kananaskis"
            ],
            "subreddits": ["skiing", "Calgary", "alberta", "snowboarding", "Kananaskis"],
        },
    },

    # ---- Themes to categorize ----
    "THEMES": [
        "Snow Conditions",
        "Pricing & Value",
        "Summer Activities",
        "Staff & Service",
        "Lift Wait Times",
        "Facilities & Lodging",
        "Trail Maintenance",
        "Environmental Impact",
        "Safety & Incidents",
        "Events & Promotions",
        "Access & Transportation",
        "Family & Beginner Experience",
    ],

    # ---- RSS Feeds (customize to your region) ----
    "RSS_FEEDS": [
        # Google News RSS for each resort
        "https://news.google.com/rss/search?q=%22Fortress+Mountain%22+Alberta&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Castle+Mountain+Resort%22+Alberta&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22Nakiska%22+ski+Alberta&hl=en-CA&gl=CA",
        "https://news.google.com/rss/search?q=%22All+Season+Resorts%22+Alberta&hl=en-CA&gl=CA",
        # Add Google Alerts RSS URLs here if you set them up
        # "https://www.google.com/alerts/feeds/YOUR_ALERT_ID",
    ],

    # ---- File paths ----
    "DATA_DIR": os.environ.get("DATA_DIR", str(Path(__file__).parent / "data")),
    "LOOKBACK_HOURS": 26,  # how far back to look (covers overnight + prior evening)
}

# --- UTILITY FUNCTIONS -------------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def make_id(text):
    return hashlib.md5(text.encode()).hexdigest()[:12]

def is_workday():
    return datetime.now().weekday() < 5  # Mon=0, Fri=4

def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

# --- REDDIT COLLECTOR --------------------------------------------------------

def collect_reddit(config):
    """Collect mentions from Reddit using the free API."""
    import requests

    mentions = []
    client_id = config["REDDIT_CLIENT_ID"]
    client_secret = config["REDDIT_CLIENT_SECRET"]

    if not client_id or not client_secret:
        log("⚠ Reddit: No credentials configured, skipping.")
        return mentions

    # Authenticate
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "client_credentials"}
    headers = {"User-Agent": config["REDDIT_USER_AGENT"]}

    try:
        token_res = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=auth, data=data, headers=headers, timeout=10
        )
        token = token_res.json().get("access_token")
        if not token:
            log("⚠ Reddit: Auth failed")
            return mentions
    except Exception as e:
        log(f"⚠ Reddit auth error: {e}")
        return mentions

    headers["Authorization"] = f"bearer {token}"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=config["LOOKBACK_HOURS"])

    for resort_key, resort_info in config["RESORTS"].items():
        for term in resort_info["search_terms"][:2]:  # limit queries to avoid rate limits
            try:
                url = f"https://oauth.reddit.com/search?q={requests.utils.quote(term)}&sort=new&limit=25&t=day"
                res = requests.get(url, headers=headers, timeout=10)
                if res.status_code != 200:
                    continue

                for post in res.json().get("data", {}).get("children", []):
                    d = post["data"]
                    created = datetime.fromtimestamp(d["created_utc"], tz=timezone.utc)
                    if created < cutoff:
                        continue

                    text = f"{d.get('title', '')} {d.get('selftext', '')[:500]}"
                    mentions.append({
                        "id": make_id(d["id"]),
                        "source": "Reddit",
                        "resort": resort_key,
                        "text": text.strip(),
                        "url": f"https://reddit.com{d.get('permalink', '')}",
                        "timestamp": created.isoformat(),
                        "engagement": f"{d.get('score', 0)} upvotes · {d.get('num_comments', 0)} comments",
                        "author": d.get("author", ""),
                        "subreddit": d.get("subreddit", ""),
                    })
                time.sleep(1)  # rate limiting
            except Exception as e:
                log(f"⚠ Reddit search error ({term}): {e}")
                continue

    log(f"✓ Reddit: collected {len(mentions)} mentions")
    return mentions

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
        for term in resort_info["search_terms"][:2]:
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
                        "timestamp": snippet["publishedAt"],
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
    """Collect from Google News RSS and custom RSS feeds."""
    import requests
    import xml.etree.ElementTree as ET

    mentions = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=config["LOOKBACK_HOURS"])

    for feed_url in config["RSS_FEEDS"]:
        try:
            res = requests.get(feed_url, timeout=15, headers={"User-Agent": "AlpinePulse/1.0"})
            if res.status_code != 200:
                continue

            root = ET.fromstring(res.content)
            # Handle RSS 2.0
            for item in root.findall(".//item"):
                title = item.findtext("title", "")
                description = item.findtext("description", "")
                link = item.findtext("link", "")
                pub_date = item.findtext("pubDate", "")
                source_name = item.findtext("source", "News")

                # Determine which resort this is about
                text = f"{title} {description}".lower()
                resort_key = "fortress"  # default
                for rk, ri in config["RESORTS"].items():
                    if any(t.lower() in text for t in ri["search_terms"]):
                        resort_key = rk
                        break

                mentions.append({
                    "id": make_id(link or title),
                    "source": source_name if source_name != "News" else "News",
                    "resort": resort_key,
                    "text": f"{title} — {description[:300]}".strip(),
                    "url": link,
                    "timestamp": pub_date,
                    "engagement": "News article",
                    "author": source_name,
                })
        except Exception as e:
            log(f"⚠ RSS error ({feed_url[:50]}...): {e}")
            continue

    log(f"✓ RSS/News: collected {len(mentions)} mentions")
    return mentions

# --- SENTIMENT ANALYSIS VIA CLAUDE ------------------------------------------

def analyze_mentions(mentions, config):
    """
    Use Claude API to analyze each mention for:
    - Sentiment (positive / neutral / negative + score 0-100)
    - Theme categorization
    - Key quote/takeaway
    
    Processes in batches to be efficient.
    """
    import requests

    api_key = config["ANTHROPIC_API_KEY"]
    if not api_key:
        log("⚠ Claude API: No key configured. Using rule-based fallback.")
        return fallback_analysis(mentions, config)

    analyzed = []
    batch_size = 15  # process 15 mentions per API call to save tokens

    for i in range(0, len(mentions), batch_size):
        batch = mentions[i:i + batch_size]
        batch_texts = []
        for j, m in enumerate(batch):
            batch_texts.append(f"[{j}] SOURCE: {m['source']} | RESORT: {m['resort']} | TEXT: {m['text'][:400]}")

        prompt = f"""You are analyzing social media and news mentions about Alberta ski resorts (Fortress Mountain, Castle Mountain Resort, and Nakiska) for an executive briefing.

For each mention below, provide:
1. sentiment: "positive", "neutral", or "negative"
2. sentiment_score: 0-100 (0=very negative, 50=neutral, 100=very positive)
3. theme: The best matching theme from this list: {json.dumps(config['THEMES'])}
4. takeaway: A 1-sentence executive summary of the key point

MENTIONS:
{chr(10).join(batch_texts)}

Respond ONLY with a JSON array. Each element should have: index, sentiment, sentiment_score, theme, takeaway.
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
                log(f"⚠ Claude API error: {res.status_code}")
                analyzed.extend(fallback_analysis(batch, config))
                continue

            response_text = res.json()["content"][0]["text"]
            # Clean response
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

            time.sleep(1)  # rate limiting

        except Exception as e:
            log(f"⚠ Claude analysis error: {e}")
            analyzed.extend(fallback_analysis(batch, config))

    log(f"✓ Analyzed {len(analyzed)} mentions")
    return analyzed


def fallback_analysis(mentions, config):
    """Simple keyword-based fallback when Claude API is unavailable."""
    positive_words = {"great", "amazing", "love", "best", "incredible", "awesome", "excellent", "fantastic", "perfect", "beautiful", "pristine", "fresh", "powder", "recommend", "friendly", "patient"}
    negative_words = {"bad", "terrible", "worst", "wait", "crowded", "overpriced", "expensive", "broken", "closed", "dangerous", "icy", "rough", "complained", "poor", "rude", "slow"}

    results = []
    for m in mentions:
        text_lower = m["text"].lower()
        pos = sum(1 for w in positive_words if w in text_lower)
        neg = sum(1 for w in negative_words if w in text_lower)

        if pos > neg:
            sentiment = "positive"
            score = min(50 + pos * 12, 95)
        elif neg > pos:
            sentiment = "negative"
            score = max(50 - neg * 12, 5)
        else:
            sentiment = "neutral"
            score = 50

        # Simple theme detection
        theme = "Snow Conditions"  # default
        theme_keywords = {
            "Pricing & Value": ["price", "cost", "expensive", "cheap", "value", "pass", "ticket"],
            "Summer Activities": ["summer", "hiking", "biking", "mountain bike", "trail"],
            "Staff & Service": ["staff", "instructor", "service", "friendly", "rude", "helpful"],
            "Lift Wait Times": ["wait", "line", "queue", "lift", "crowded", "busy"],
            "Facilities & Lodging": ["lodge", "food", "restaurant", "hotel", "parking", "washroom"],
            "Trail Maintenance": ["grooming", "groomed", "trail", "run condition", "maintained"],
            "Environmental Impact": ["environment", "wildlife", "ecosystem", "sustainable"],
            "Safety & Incidents": ["accident", "injury", "rescue", "closed", "avalanche", "danger"],
            "Events & Promotions": ["event", "festival", "promotion", "discount", "deal", "sale"],
            "Access & Transportation": ["road", "drive", "access", "highway", "shuttle", "pothole"],
            "Family & Beginner Experience": ["family", "kids", "beginner", "lesson", "learn", "children"],
            "Snow Conditions": ["snow", "powder", "conditions", "fresh", "base", "coverage"],
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

    today = datetime.now().strftime("%Y-%m-%d")
    data_dir = config["DATA_DIR"]
    ensure_dir(data_dir)

    # Load historical data for trends
    history_file = os.path.join(data_dir, "history.json")
    if os.path.exists(history_file):
        with open(history_file) as f:
            history = json.load(f)
    else:
        history = {"daily": []}

    # Today's stats
    total = len(analyzed_mentions)
    pos_count = sum(1 for m in analyzed_mentions if m["sentiment"] == "positive")
    neu_count = sum(1 for m in analyzed_mentions if m["sentiment"] == "neutral")
    neg_count = sum(1 for m in analyzed_mentions if m["sentiment"] == "negative")

    pos_pct = round(pos_count / total * 100) if total else 0
    neu_pct = round(neu_count / total * 100) if total else 0
    neg_pct = 100 - pos_pct - neu_pct if total else 0

    # Per-resort breakdown
    resort_stats = {}
    for rk in config["RESORTS"]:
        rm = [m for m in analyzed_mentions if m["resort"] == rk]
        rt = len(rm)
        rp = sum(1 for m in rm if m["sentiment"] == "positive")
        rn_neu = sum(1 for m in rm if m["sentiment"] == "neutral")
        rn_neg = sum(1 for m in rm if m["sentiment"] == "negative")
        resort_stats[rk] = {
            "name": config["RESORTS"][rk]["name"],
            "total": rt,
            "positive_pct": round(rp / rt * 100) if rt else 0,
            "neutral_pct": round(rn_neu / rt * 100) if rt else 0,
            "negative_pct": round(rn_neg / rt * 100) if rt else 0,
        }

    # Theme breakdown
    theme_data = {}
    for theme in config["THEMES"]:
        tm = [m for m in analyzed_mentions if m.get("theme") == theme]
        if not tm:
            continue
        avg_score = sum(m["sentiment_score"] for m in tm) / len(tm)
        sentiment = "positive" if avg_score >= 60 else "negative" if avg_score <= 40 else "neutral"
        theme_data[theme] = {
            "mentions": len(tm),
            "avg_score": round(avg_score),
            "sentiment": sentiment,
        }

    # Sort themes by mention count
    sorted_themes = sorted(theme_data.items(), key=lambda x: x[1]["mentions"], reverse=True)

    # Add today to history
    today_entry = {
        "date": today,
        "total": total,
        "positive_pct": pos_pct,
        "neutral_pct": neu_pct,
        "negative_pct": neg_pct,
        "themes": {k: v["mentions"] for k, v in theme_data.items()},
        "resort_stats": resort_stats,
    }

    # Remove today if already present (re-run), keep last 30 entries
    history["daily"] = [d for d in history["daily"] if d["date"] != today]
    history["daily"].append(today_entry)
    history["daily"] = history["daily"][-30:]

    # Build trend data (last 20 workdays)
    trend_pos = [d["positive_pct"] for d in history["daily"][-20:]]
    trend_neu = [d["neutral_pct"] for d in history["daily"][-20:]]
    trend_neg = [d["negative_pct"] for d in history["daily"][-20:]]
    trend_labels = [d["date"] for d in history["daily"][-20:]]

    # Theme trends
    all_theme_keys = set()
    for d in history["daily"]:
        all_theme_keys.update(d.get("themes", {}).keys())

    theme_trends = {}
    for tk in all_theme_keys:
        theme_trends[tk] = [d.get("themes", {}).get(tk, 0) for d in history["daily"][-20:]]

    # Prepare feed (most recent first, limit 30)
    feed = sorted(analyzed_mentions, key=lambda m: m.get("timestamp", ""), reverse=True)[:30]
    feed_clean = []
    for m in feed:
        feed_clean.append({
            "source": m["source"],
            "resort": m["resort"],
            "sentiment": m["sentiment"],
            "text": m["text"][:300],
            "timestamp": m.get("timestamp", ""),
            "engagement": m.get("engagement", ""),
            "theme": m.get("theme", ""),
            "takeaway": m.get("takeaway", ""),
            "url": m.get("url", ""),
        })

    # Build the final dashboard JSON
    dashboard = {
        "generated_at": datetime.now().isoformat(),
        "date": today,
        "summary": {
            "total_mentions": total,
            "positive_pct": pos_pct,
            "neutral_pct": neu_pct,
            "negative_pct": neg_pct,
            "negative_count": neg_count,
        },
        "resort_stats": resort_stats,
        "themes": [{"name": k, **v} for k, v in sorted_themes],
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

    # Build theme rows
    theme_rows = ""
    for t in dashboard["themes"][:8]:
        color = "#34d399" if t["sentiment"] == "positive" else "#f87171" if t["sentiment"] == "negative" else "#94a3b8"
        theme_rows += f"""
        <tr>
          <td style="padding:10px 14px;border-bottom:1px solid #1e293b;color:#f1f5f9;font-weight:500">{t['name']}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1e293b;color:#94a3b8;text-align:center">{t['mentions']}</td>
          <td style="padding:10px 14px;border-bottom:1px solid #1e293b;text-align:center">
            <span style="background:{'rgba(52,211,153,0.15)' if t['sentiment']=='positive' else 'rgba(248,113,113,0.15)' if t['sentiment']=='negative' else 'rgba(148,163,184,0.15)'};color:{color};padding:3px 10px;border-radius:12px;font-size:12px">{t['sentiment'].title()}</span>
          </td>
        </tr>"""

    # Top feed items
    feed_rows = ""
    for item in dashboard["feed"][:6]:
        bar_color = "#34d399" if item["sentiment"] == "positive" else "#f87171" if item["sentiment"] == "negative" else "#94a3b8"
        feed_rows += f"""
        <tr>
          <td style="padding:10px 14px;border-bottom:1px solid #1e293b;width:4px">
            <div style="width:4px;height:36px;background:{bar_color};border-radius:2px"></div>
          </td>
          <td style="padding:10px 14px;border-bottom:1px solid #1e293b">
            <div style="color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:1px">{item['source']} · {item['resort'].upper()}</div>
            <div style="color:#cbd5e1;font-size:13px;margin-top:4px;line-height:1.5">{item['text'][:180]}...</div>
          </td>
        </tr>"""

    # Resort breakdown rows
    resort_rows = ""
    for rk, rs in dashboard["resort_stats"].items():
        resort_rows += f"""
        <tr>
          <td style="padding:8px 14px;border-bottom:1px solid #1e293b;color:#f1f5f9;font-weight:500">{rs['name']}</td>
          <td style="padding:8px 14px;border-bottom:1px solid #1e293b;color:#94a3b8;text-align:center">{rs['total']}</td>
          <td style="padding:8px 14px;border-bottom:1px solid #1e293b;color:#34d399;text-align:center">{rs['positive_pct']}%</td>
          <td style="padding:8px 14px;border-bottom:1px solid #1e293b;color:#94a3b8;text-align:center">{rs['neutral_pct']}%</td>
          <td style="padding:8px 14px;border-bottom:1px solid #1e293b;color:#f87171;text-align:center">{rs['negative_pct']}%</td>
        </tr>"""

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0;padding:0;background:#0B1120;font-family:Arial,Helvetica,sans-serif">
      <div style="max-width:640px;margin:0 auto;padding:32px 20px">

        <!-- Header -->
        <div style="text-align:center;margin-bottom:32px">
          <div style="font-size:28px;margin-bottom:8px">⛰</div>
          <h1 style="font-size:24px;color:#f1f5f9;margin:0;letter-spacing:-0.5px">Alpine Pulse</h1>
          <p style="color:#64748b;font-size:13px;margin:4px 0 0;letter-spacing:1px;text-transform:uppercase">Daily Sentiment Briefing</p>
          <p style="color:#94a3b8;font-size:14px;margin:8px 0 0">{date_str}</p>
        </div>

        <!-- Summary -->
        <table style="width:100%;border-collapse:collapse;margin-bottom:24px">
          <tr>
            <td style="width:25%;text-align:center;padding:20px;background:#111827;border-radius:12px 0 0 12px;border-top:2px solid #38bdf8">
              <div style="color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px">Mentions</div>
              <div style="color:#f1f5f9;font-size:28px;font-weight:800;margin-top:4px">{s['total_mentions']}</div>
            </td>
            <td style="width:25%;text-align:center;padding:20px;background:#111827;border-top:2px solid #34d399">
              <div style="color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px">Positive</div>
              <div style="color:#34d399;font-size:28px;font-weight:800;margin-top:4px">{s['positive_pct']}%</div>
            </td>
            <td style="width:25%;text-align:center;padding:20px;background:#111827;border-top:2px solid #94a3b8">
              <div style="color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px">Neutral</div>
              <div style="color:#94a3b8;font-size:28px;font-weight:800;margin-top:4px">{s['neutral_pct']}%</div>
            </td>
            <td style="width:25%;text-align:center;padding:20px;background:#111827;border-radius:0 12px 12px 0;border-top:2px solid #f87171">
              <div style="color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px">Negative</div>
              <div style="color:#f87171;font-size:28px;font-weight:800;margin-top:4px">{s['negative_pct']}%</div>
            </td>
          </tr>
        </table>

        <!-- Resort Breakdown -->
        <div style="background:#111827;border-radius:12px;padding:20px;margin-bottom:24px;border:1px solid #1e293b">
          <h2 style="color:#94a3b8;font-size:13px;text-transform:uppercase;letter-spacing:1px;margin:0 0 16px">Resort Breakdown</h2>
          <table style="width:100%;border-collapse:collapse">
            <tr>
              <th style="text-align:left;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Resort</th>
              <th style="text-align:center;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Total</th>
              <th style="text-align:center;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Pos</th>
              <th style="text-align:center;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Neu</th>
              <th style="text-align:center;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Neg</th>
            </tr>
            {resort_rows}
          </table>
        </div>

        <!-- Themes -->
        <div style="background:#111827;border-radius:12px;padding:20px;margin-bottom:24px;border:1px solid #1e293b">
          <h2 style="color:#94a3b8;font-size:13px;text-transform:uppercase;letter-spacing:1px;margin:0 0 16px">Theme Rankings</h2>
          <table style="width:100%;border-collapse:collapse">
            <tr>
              <th style="text-align:left;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Theme</th>
              <th style="text-align:center;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Mentions</th>
              <th style="text-align:center;padding:8px 14px;color:#64748b;font-size:10px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #1e293b">Sentiment</th>
            </tr>
            {theme_rows}
          </table>
        </div>

        <!-- Top Mentions -->
        <div style="background:#111827;border-radius:12px;padding:20px;margin-bottom:24px;border:1px solid #1e293b">
          <h2 style="color:#94a3b8;font-size:13px;text-transform:uppercase;letter-spacing:1px;margin:0 0 16px">Notable Mentions</h2>
          <table style="width:100%;border-collapse:collapse">
            {feed_rows}
          </table>
        </div>

        <!-- Footer -->
        <div style="text-align:center;padding:20px;color:#64748b;font-size:11px">
          Alpine Pulse — All Season Resorts Alberta<br>
          Automated sentiment intelligence · Generated {datetime.now().strftime('%I:%M %p MT')}
        </div>

      </div>
    </body>
    </html>
    """

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

# --- MAIN ENTRY POINT -------------------------------------------------------

def main():
    log("=" * 60)
    log("Alpine Pulse — Starting daily collection")
    log("=" * 60)

    if not is_workday():
        log("Weekend detected. Skipping (workdays only). Use --force to override.")
        if "--force" not in sys.argv:
            return

    # Collect from all sources
    all_mentions = []
    all_mentions.extend(collect_reddit(CONFIG))
    all_mentions.extend(collect_youtube(CONFIG))
    all_mentions.extend(collect_rss(CONFIG))

    # Deduplicate by ID
    seen = set()
    unique = []
    for m in all_mentions:
        if m["id"] not in seen:
            seen.add(m["id"])
            unique.append(m)
    all_mentions = unique

    log(f"Total unique mentions: {len(all_mentions)}")

    if not all_mentions:
        log("No mentions found. Check API keys and search terms.")
        return

    # Analyze sentiment & themes
    analyzed = analyze_mentions(all_mentions, CONFIG)

    # Build dashboard data
    dashboard = build_dashboard_data(analyzed, CONFIG)

    # Send email
    send_email_briefing(dashboard, CONFIG)

    log("=" * 60)
    log("✓ Daily collection complete!")
    log("=" * 60)


if __name__ == "__main__":
    main()
