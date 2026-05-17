# Publishing Queue Feature — Design Spec
**Date:** 2026-05-13
**Project:** human-workforce-analytics dashboard

## Goal

Add a "Publishing Queue" section to the bottom of the Streamlit dashboard that shows all unpublished (private/unlisted) YouTube videos ranked by relevance to top news stories from the past 48 hours. The team reviews this ranking each morning and manually schedules the most timely episode in YouTube Studio.

## Architecture

Two new components bolt onto the existing codebase without modifying working code paths:

- **`ai_client.py`** — pure functions for Claude API (theme classification, relevance ranking) and NewsAPI (headline fetch). No Streamlit, no DB imports. Testable in isolation.
- **`publishing_queue` table in `data.db`** — one row written per cron run, dashboard reads the latest row.

The existing `fetch_metrics.py` calls a new `write_publishing_queue()` function at the end of `main()`, after the retention loop.

## Data Flow (per cron run)

1. Re-fetch all video IDs from uploads playlist (already done in `main()`).
2. Call `fetch_video_details` with `part="snippet,statistics,contentDetails,status"` — filter results to videos where `status.privacyStatus != "public"`. These are the candidates.
3. If no unpublished videos → skip entire publishing queue step, log and return.
4. Call Claude Haiku via `ai_client.classify_video_themes(videos)` — one API call, batch prompt. Returns `{video_id: theme_tag}` for all candidates.
5. Call `ai_client.fetch_news_headlines(api_key, hours=48)` → top 20 headlines from NewsAPI (categories: technology, business, science).
6. If NewsAPI fails → store themes-only result with `news_available: false` flag, skip ranking.
7. Call Claude Haiku via `ai_client.rank_videos_by_news(videos_with_themes, headlines)` → ranked list with relevance score (1–10) and "why now" sentence per video.
8. Write one row to `publishing_queue`: `(analyzed_at TEXT, videos_analyzed INT, news_stories_count INT, result_json TEXT)`.

## DB Schema

```sql
CREATE TABLE IF NOT EXISTS publishing_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyzed_at TEXT NOT NULL,
    videos_analyzed INTEGER NOT NULL DEFAULT 0,
    news_stories_count INTEGER NOT NULL DEFAULT 0,
    result_json TEXT NOT NULL
);
```

No changes to the `videos` table. `privacy_status` is fetched at analysis time and used transiently — not persisted separately.

## result_json Shape

```json
{
  "news_available": true,
  "ranked_videos": [
    {
      "rank": 1,
      "video_id": "abc123",
      "title": "When AI Replaces Managers",
      "theme": "AI workforce displacement",
      "relevance_score": 9,
      "why_now": "Microsoft announced 6,000 layoffs driven by AI automation today..."
    }
  ],
  "news_headlines": [
    { "title": "...", "source": "Reuters", "published_at": "2026-05-13T08:00:00Z" }
  ]
}
```

## Claude API Usage

- **Model:** `claude-haiku-4-5-20251001` — sufficient for classification/ranking, low cost.
- **Prompt caching:** system prompt marked with `cache_control: {"type": "ephemeral"}` on both calls.
- **Call 1 — Theme classification:** batch all unpublished video titles + first 300 chars of description in a single prompt. Returns JSON `{video_id: theme}`.
- **Call 2 — Relevance ranking:** provide themes + 20 headlines. Returns ranked JSON array with score and reasoning.
- **Graceful failure:** if either Claude call raises an exception, log and skip — `write_publishing_queue()` is wrapped in try/except in `main()`, matching the retention loop pattern.

## NewsAPI

- Endpoint: `https://newsapi.org/v2/top-headlines`
- Parameters: `language=en`, `pageSize=20`, `from=<48h ago>`, categories iterated: technology, business, science (three separate calls, deduplicated by title).
- Key stored as `NEWS_API_KEY` env var and GitHub secret.

## New Secrets Required

| Secret | Where |
|--------|--------|
| `ANTHROPIC_API_KEY` | GH Actions secret + `.streamlit/secrets.toml` |
| `NEWS_API_KEY` | GH Actions secret + `.streamlit/secrets.toml` |

The dashboard only reads pre-computed results from DB — it does NOT call Claude or NewsAPI directly. No secrets needed in the Streamlit runtime beyond what's already there.

## Dashboard Section

Location: bottom of `app.py`, after Growth Projections.

**Section header:** "Publishing Queue"
**Caption:** "Unpublished episodes ranked by relevance to today's top news stories. Updated every 4 hours."

Layout per ranked video (top-20 max, ordered rank 1 first):
- Rank badge (`#1`, `#2`, …)
- Video title (bold)
- Theme tag (colored `st.badge` or small pill)
- Relevance score bar (1–10, `st.progress`)
- "Why now" text (italicized, gray)

Below the ranked list: collapsible `st.expander("News headlines used")` showing the 20 headlines and sources.

If no unpublished videos → show `st.info("No unpublished videos in queue.")`.
If analysis is stale (>8h) → show a warning badge next to the timestamp.

## New Files

| File | Purpose |
|------|---------|
| `ai_client.py` | Claude API + NewsAPI pure functions |
| `tests/test_ai_client.py` | Unit tests with mocked API calls |

## Modified Files

| File | Change |
|------|--------|
| `db.py` | Add `publishing_queue` table to `init_db()` |
| `fetch_metrics.py` | Add `write_publishing_queue()` function + call it in `main()` |
| `youtube_client.py` | Add `status` to `fetch_video_details` parts; expose `privacy_status` in returned dict |
| `app.py` | New Publishing Queue section at bottom |
| `requirements.txt` | Add `anthropic`, `newsapi-python` |
| `.github/workflows/fetch-analytics.yml` | Add `ANTHROPIC_API_KEY` and `NEWS_API_KEY` to env block |

## Error Handling

- NewsAPI down → themes stored, ranking skipped, dashboard shows themes-only list with note "News unavailable — showing themes only."
- Claude API down → entire publishing queue step skipped, dashboard shows last successful result with stale warning.
- No unpublished videos → step skipped entirely, dashboard shows `st.info`.
- All errors are caught and logged; they never crash the main fetch pipeline.

## Testing

- `test_ai_client.py`: mock `anthropic.Anthropic` and `newsapi.NewsApiClient`; verify prompt structure, JSON parsing, graceful failure paths.
- No integration tests against live APIs (cost + flakiness).
