"""Claude API and NewsAPI client for publishing queue analysis.

Pure functions — no DB, no Streamlit. All I/O is through parameters and return values.
"""

import json

import anthropic
from newsapi import NewsApiClient

SYSTEM_PROMPT = (
    "You are an editorial assistant for 'The Human Workforce' podcast, which covers "
    "AI, workforce transformation, career development, and the future of work. "
    "You analyze video content and current news to help the team decide which episodes "
    "to publish when they will resonate most with current events."
)


def classify_video_themes(
    client: anthropic.Anthropic,
    videos: list[dict],
) -> dict[str, str]:
    """Classify each unpublished video into a short theme tag.

    videos: list of dicts with keys video_id, title, description.
    Returns: {video_id: theme_tag}
    """
    if not videos:
        return {}

    video_list = "\n".join(
        f'ID: {v["video_id"]}\nTitle: {v["title"]}\nDescription: {v["description"][:300]}\n'
        for v in videos
    )

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{
            "role": "user",
            "content": (
                "For each video below, assign a short theme tag (3–6 words) capturing its core topic.\n\n"
                f"{video_list}\n"
                "Return ONLY a JSON object mapping video_id to theme_tag. Example:\n"
                '{"abc123": "AI workforce displacement", "def456": "future of remote work"}'
            ),
        }],
    )

    raw = response.content[0].text.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1:
        raw = raw[start:end + 1]
    return json.loads(raw)


def rank_videos_by_news(
    client: anthropic.Anthropic,
    videos_with_themes: list[dict],
    headlines: list[dict],
) -> list[dict]:
    """Rank unpublished videos by relevance to today's news headlines.

    videos_with_themes: list of dicts with keys video_id, title, theme.
    headlines: list of dicts with keys title, source, published_at.
    Returns: list of {rank, video_id, title, theme, relevance_score, why_now} sorted rank 1 first.
    """
    if not videos_with_themes:
        return []

    if not headlines:
        return [
            {
                "rank": i + 1,
                "video_id": v["video_id"],
                "title": v["title"],
                "theme": v["theme"],
                "relevance_score": 0,
                "why_now": "News data unavailable for ranking.",
            }
            for i, v in enumerate(videos_with_themes)
        ]

    video_list = "\n".join(
        f'ID: {v["video_id"]} | Title: {v["title"]} | Theme: {v["theme"]}'
        for v in videos_with_themes
    )
    headline_list = "\n".join(
        f'- {h["title"]} ({h.get("source", "")})'
        for h in headlines[:20]
    )

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=8192,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{
            "role": "user",
            "content": (
                "Rank these unpublished podcast episodes from most to least relevant to publish TODAY "
                "based on the current news headlines.\n\n"
                f"UNPUBLISHED EPISODES:\n{video_list}\n\n"
                f"TODAY'S TOP NEWS HEADLINES:\n{headline_list}\n\n"
                "For each episode assign:\n"
                "- relevance_score: 1–10 (10 = extremely timely, 1 = no connection to current news)\n"
                "- why_now: one sentence explaining the news connection (or why it is not timely)\n\n"
                "Return ONLY a JSON array sorted by relevance_score descending:\n"
                '[{"rank": 1, "video_id": "...", "title": "...", "theme": "...", '
                '"relevance_score": 9, "why_now": "..."}]'
            ),
        }],
    )

    raw = response.content[0].text.strip()
    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1:
        raw = raw[start:end + 1]
    return json.loads(raw)


def fetch_news_headlines(api_key: str, hours: int = 48) -> list[dict]:
    """Fetch up to 20 deduplicated top headlines from the past N hours via NewsAPI.

    Returns: list of {title, source, published_at}
    """
    from datetime import datetime, timezone, timedelta
    client = NewsApiClient(api_key=api_key)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    seen_titles: set[str] = set()
    headlines: list[dict] = []

    for query in ("AI workforce automation", "technology jobs future", "business economy workforce"):
        try:
            resp = client.get_everything(
                q=query,
                from_param=cutoff,
                language="en",
                sort_by="publishedAt",
                page_size=20,
            )
            for article in resp.get("articles", []):
                title = article.get("title") or ""
                if title and title not in seen_titles:
                    seen_titles.add(title)
                    headlines.append({
                        "title": title,
                        "source": article.get("source", {}).get("name", ""),
                        "published_at": article.get("publishedAt", ""),
                    })
        except Exception:
            continue

    return headlines[:20]
