# Publishing Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Publishing Queue section to the Streamlit dashboard that classifies unpublished YouTube videos by theme, fetches today's top news via NewsAPI, and uses Claude Haiku to rank which episode to publish first based on relevance to current events.

**Architecture:** A new `ai_client.py` module holds all Claude API and NewsAPI logic as pure functions. A new `write_publishing_queue()` function in `fetch_metrics.py` orchestrates the analysis and writes one JSON blob per cron run to a new `publishing_queue` SQLite table. The dashboard reads the latest row and renders a ranked card list — no AI calls at render time.

**Tech Stack:** `anthropic>=0.40.0` (Claude Haiku 4.5, prompt caching), `newsapi-python>=0.2.7`, existing SQLite/Streamlit stack.

---

## File Map

| Action | File | What changes |
|--------|------|-------------|
| Modify | `requirements.txt` | Add `anthropic`, `newsapi-python` |
| Modify | `db.py` | Add `publishing_queue` table to `SCHEMA` |
| Modify | `youtube_client.py` | Add `status` to `fetch_video_details` parts; return `privacy_status` |
| Create | `ai_client.py` | `fetch_news_headlines`, `classify_video_themes`, `rank_videos_by_news` |
| Create | `tests/test_ai_client.py` | Unit tests (all mocked) |
| Modify | `fetch_metrics.py` | Add `import json`, `import anthropic`, imports from `ai_client`; add `write_publishing_queue()`; call it in `main()` |
| Modify | `tests/test_fetch_metrics.py` | 3 new tests for `write_publishing_queue` |
| Modify | `tests/test_db.py` | 1 new test for `publishing_queue` table |
| Modify | `tests/test_youtube_client.py` | 1 new test for `privacy_status` in `fetch_video_details` |
| Modify | `app.py` | Add `import json`; load `publishing_queue`; render new section |
| Modify | `.github/workflows/fetch-analytics.yml` | Add `ANTHROPIC_API_KEY` and `NEWS_API_KEY` to env block |
| Modify | `.streamlit/secrets.toml.example` | Document new secret keys |

---

## Task 1: Add Dependencies

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add new packages to requirements.txt**

The file currently ends with `pytest>=8.0.0`. Add two lines after it:

```
anthropic>=0.40.0
newsapi-python>=0.2.7
```

Final `requirements.txt`:
```
streamlit>=1.40.0
google-api-python-client>=2.150.0
google-auth>=2.36.0
google-auth-oauthlib>=1.2.0
google-auth-httplib2>=0.2.0
pandas>=2.2.0
plotly>=5.24.0
python-dateutil>=2.9.0
pytest>=8.0.0
anthropic>=0.40.0
newsapi-python>=0.2.7
```

- [ ] **Step 2: Install the new packages**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
.venv/bin/pip install anthropic newsapi-python
```

Expected: both packages install without error.

- [ ] **Step 3: Verify import works**

```bash
.venv/bin/python -c "import anthropic; import newsapi; print('ok')"
```

Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "chore: add anthropic and newsapi-python dependencies"
```

---

## Task 2: Add publishing_queue Table to DB

**Files:**
- Modify: `db.py`
- Modify: `tests/test_db.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_db.py`:

```python
def test_publishing_queue_table_created():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='publishing_queue'"
                )
                assert cursor.fetchone() is not None


def test_publishing_queue_autoincrement_and_columns():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db
            db.init_db()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO publishing_queue(analyzed_at, videos_analyzed, news_stories_count, result_json) "
                    "VALUES ('2026-05-13T10:00:00Z', 3, 20, '{\"ranked_videos\": []}')"
                )
                row = conn.execute("SELECT * FROM publishing_queue").fetchone()
                assert row[0] == 1          # id
                assert row[1] == "2026-05-13T10:00:00Z"  # analyzed_at
                assert row[2] == 3          # videos_analyzed
                assert row[3] == 20         # news_stories_count
                assert "ranked_videos" in row[4]  # result_json
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
.venv/bin/python -m pytest tests/test_db.py::test_publishing_queue_table_created tests/test_db.py::test_publishing_queue_autoincrement_and_columns -v
```

Expected: FAIL — "no such table: publishing_queue"

- [ ] **Step 3: Add the table to db.py SCHEMA**

In `db.py`, append to the `SCHEMA` string just before the closing `"""`, after the last `CREATE INDEX` statement:

```python
CREATE TABLE IF NOT EXISTS publishing_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analyzed_at TEXT NOT NULL,
    videos_analyzed INTEGER NOT NULL DEFAULT 0,
    news_stories_count INTEGER NOT NULL DEFAULT 0,
    result_json TEXT NOT NULL
);
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_db.py -v
```

Expected: all tests PASS including the two new ones.

- [ ] **Step 5: Commit**

```bash
git add db.py tests/test_db.py
git commit -m "feat: add publishing_queue table to schema"
```

---

## Task 3: Expose privacy_status in fetch_video_details

**Files:**
- Modify: `youtube_client.py`
- Modify: `tests/test_youtube_client.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_youtube_client.py`:

```python
def _fake_data_service(items):
    """Return a mock that mimics data_service().videos().list().execute()."""
    service = MagicMock()
    service.videos().list().execute.return_value = {"items": items}
    return service


def test_fetch_video_details_includes_privacy_status():
    fake_item = {
        "id": "v1",
        "snippet": {
            "title": "Test Video",
            "description": "A description",
            "publishedAt": "2026-01-01T00:00:00Z",
            "thumbnails": {"high": {"url": "http://thumb.jpg"}},
        },
        "statistics": {"viewCount": "100", "likeCount": "10", "commentCount": "2"},
        "contentDetails": {"duration": "PT10M30S"},
        "status": {"privacyStatus": "private"},
    }
    with patch("youtube_client.data_service") as mock_svc:
        mock_svc.return_value = _fake_data_service([fake_item])
        result = youtube_client.fetch_video_details(["v1"])
    assert len(result) == 1
    assert result[0]["privacy_status"] == "private"
    assert result[0]["video_id"] == "v1"
    assert result[0]["view_count"] == 100
```

- [ ] **Step 2: Run test to verify it fails**

```bash
.venv/bin/python -m pytest tests/test_youtube_client.py::test_fetch_video_details_includes_privacy_status -v
```

Expected: FAIL — KeyError on `status` key.

- [ ] **Step 3: Update fetch_video_details in youtube_client.py**

Find the `fetch_video_details` function. Change the `videos().list(...)` call and the `details.append(...)` block:

```python
def fetch_video_details(video_ids: list[str]) -> list[dict]:
    yt = data_service()
    details = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = yt.videos().list(
            part="snippet,statistics,contentDetails,status",
            id=",".join(batch),
        ).execute()
        for item in resp["items"]:
            details.append({
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "description": item["snippet"].get("description", ""),
                "published_at": item["snippet"]["publishedAt"],
                "thumbnail_url": item["snippet"]["thumbnails"].get("high", {}).get("url", ""),
                "duration": item["contentDetails"]["duration"],
                "view_count": int(item["statistics"].get("viewCount", 0)),
                "like_count": int(item["statistics"].get("likeCount", 0)),
                "comment_count": int(item["statistics"].get("commentCount", 0)),
                "privacy_status": item["status"]["privacyStatus"],
            })
    return details
```

- [ ] **Step 4: Run all tests to verify nothing broke**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add youtube_client.py tests/test_youtube_client.py
git commit -m "feat: expose privacy_status in fetch_video_details"
```

---

## Task 4: Create ai_client.py with Tests

**Files:**
- Create: `ai_client.py`
- Create: `tests/test_ai_client.py`

- [ ] **Step 1: Write all failing tests first**

Create `tests/test_ai_client.py`:

```python
import json
from unittest.mock import MagicMock, patch

import pytest

import ai_client
from ai_client import classify_video_themes, fetch_news_headlines, rank_videos_by_news


# --- classify_video_themes ---

def test_classify_video_themes_returns_theme_per_video():
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text='{"v1": "AI workforce displacement", "v2": "remote work future"}')]
    )
    videos = [
        {"video_id": "v1", "title": "When AI Takes Your Job", "description": "About automation."},
        {"video_id": "v2", "title": "Remote Work in 2026", "description": "Hybrid teams."},
    ]
    result = classify_video_themes(mock_client, videos)
    assert result == {"v1": "AI workforce displacement", "v2": "remote work future"}


def test_classify_video_themes_empty_input_returns_empty_without_api_call():
    mock_client = MagicMock()
    result = classify_video_themes(mock_client, [])
    assert result == {}
    mock_client.messages.create.assert_not_called()


def test_classify_video_themes_strips_markdown_fences():
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text='```json\n{"v1": "AI hiring"}\n```')]
    )
    videos = [{"video_id": "v1", "title": "T", "description": "D"}]
    result = classify_video_themes(mock_client, videos)
    assert result == {"v1": "AI hiring"}


def test_classify_video_themes_truncates_description_to_300_chars():
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text='{"v1": "theme"}')]
    )
    long_desc = "x" * 1000
    videos = [{"video_id": "v1", "title": "T", "description": long_desc}]
    classify_video_themes(mock_client, videos)
    call_content = mock_client.messages.create.call_args[1]["messages"][0]["content"]
    assert "x" * 301 not in call_content


# --- rank_videos_by_news ---

def test_rank_videos_by_news_returns_ranked_list():
    mock_client = MagicMock()
    ranked = [
        {"rank": 1, "video_id": "v1", "title": "T", "theme": "AI", "relevance_score": 9, "why_now": "Big news."},
    ]
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=json.dumps(ranked))]
    )
    videos = [{"video_id": "v1", "title": "T", "theme": "AI"}]
    headlines = [{"title": "Google lays off 1000", "source": "Reuters", "published_at": "2026-05-13T10:00:00Z"}]
    result = rank_videos_by_news(mock_client, videos, headlines)
    assert result[0]["rank"] == 1
    assert result[0]["relevance_score"] == 9
    mock_client.messages.create.assert_called_once()


def test_rank_videos_by_news_empty_headlines_skips_api_and_scores_zero():
    mock_client = MagicMock()
    videos = [{"video_id": "v1", "title": "T", "theme": "AI"}]
    result = rank_videos_by_news(mock_client, videos, [])
    assert result[0]["relevance_score"] == 0
    assert result[0]["why_now"] == "News data unavailable for ranking."
    mock_client.messages.create.assert_not_called()


def test_rank_videos_by_news_empty_videos_returns_empty():
    mock_client = MagicMock()
    result = rank_videos_by_news(mock_client, [], [{"title": "headline"}])
    assert result == []
    mock_client.messages.create.assert_not_called()


def test_rank_videos_by_news_strips_markdown_fences():
    mock_client = MagicMock()
    ranked = [{"rank": 1, "video_id": "v1", "title": "T", "theme": "AI", "relevance_score": 7, "why_now": "Now."}]
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=f"```json\n{json.dumps(ranked)}\n```")]
    )
    videos = [{"video_id": "v1", "title": "T", "theme": "AI"}]
    result = rank_videos_by_news(mock_client, videos, [{"title": "h"}])
    assert result[0]["relevance_score"] == 7


# --- fetch_news_headlines ---

def test_fetch_news_headlines_returns_deduped_list():
    with patch("ai_client.NewsApiClient") as MockNews:
        instance = MockNews.return_value
        instance.get_top_headlines.return_value = {
            "articles": [
                {"title": "AI takes jobs", "source": {"name": "BBC"}, "publishedAt": "2026-05-13T10:00:00Z"},
                {"title": "AI takes jobs", "source": {"name": "CNN"}, "publishedAt": "2026-05-13T11:00:00Z"},
            ]
        }
        result = fetch_news_headlines("fake_key")
    # Same title appears across categories — deduplicated
    titles = [h["title"] for h in result]
    assert titles.count("AI takes jobs") == 1


def test_fetch_news_headlines_caps_at_20():
    with patch("ai_client.NewsApiClient") as MockNews:
        instance = MockNews.return_value
        instance.get_top_headlines.return_value = {
            "articles": [
                {"title": f"Story {i}", "source": {"name": "BBC"}, "publishedAt": "2026-05-13T10:00:00Z"}
                for i in range(30)
            ]
        }
        result = fetch_news_headlines("fake_key")
    assert len(result) <= 20


def test_fetch_news_headlines_category_error_continues():
    with patch("ai_client.NewsApiClient") as MockNews:
        instance = MockNews.return_value
        def side_effect(language, category, page_size):
            if category == "technology":
                raise Exception("API error")
            return {"articles": [
                {"title": "Business story", "source": {"name": "Reuters"}, "publishedAt": "2026-05-13T10:00:00Z"}
            ]}
        instance.get_top_headlines.side_effect = side_effect
        result = fetch_news_headlines("fake_key")
    # Should still return business/science stories
    assert any(h["title"] == "Business story" for h in result)
```

- [ ] **Step 2: Run tests to verify they all fail**

```bash
.venv/bin/python -m pytest tests/test_ai_client.py -v
```

Expected: ImportError — `ai_client` not found.

- [ ] **Step 3: Create ai_client.py**

Create `ai_client.py` in the project root:

```python
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
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


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
        max_tokens=2048,
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
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def fetch_news_headlines(api_key: str, hours: int = 48) -> list[dict]:
    """Fetch up to 20 deduplicated top headlines from the past N hours via NewsAPI.

    Returns: list of {title, source, published_at}
    """
    client = NewsApiClient(api_key=api_key)
    seen_titles: set[str] = set()
    headlines: list[dict] = []

    for category in ("technology", "business", "science"):
        try:
            resp = client.get_top_headlines(
                language="en",
                category=category,
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
```

- [ ] **Step 4: Run all tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_ai_client.py -v
```

Expected: all 12 tests PASS.

- [ ] **Step 5: Run the full test suite to make sure nothing broke**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add ai_client.py tests/test_ai_client.py
git commit -m "feat: add ai_client with Claude theme classification and news ranking"
```

---

## Task 5: Add write_publishing_queue to fetch_metrics.py

**Files:**
- Modify: `fetch_metrics.py`
- Modify: `tests/test_fetch_metrics.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_fetch_metrics.py` (keep existing imports, add `json` and `sqlite3`):

```python
import json
import sqlite3

# --- write_publishing_queue tests ---

def test_write_publishing_queue_skips_when_no_unpublished_videos():
    """If all videos are public, skip without calling Claude."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_publishing_queue
            with patch("fetch_metrics.classify_video_themes") as mock_classify:
                write_publishing_queue([
                    {"video_id": "v1", "privacy_status": "public", "title": "T", "description": "D"}
                ])
                mock_classify.assert_not_called()


def test_write_publishing_queue_skips_without_anthropic_key(monkeypatch):
    """If ANTHROPIC_API_KEY is not set, skip gracefully without writing to DB."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_publishing_queue
            write_publishing_queue([
                {"video_id": "v1", "privacy_status": "private", "title": "T", "description": "D"}
            ])
            with sqlite3.connect(db_path) as conn:
                count = conn.execute("SELECT COUNT(*) FROM publishing_queue").fetchone()[0]
                assert count == 0


def test_write_publishing_queue_writes_result_json(monkeypatch):
    """Happy path: unpublished videos + API key → row written to publishing_queue."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test_key")
    monkeypatch.delenv("NEWS_API_KEY", raising=False)
    ranked = [{"rank": 1, "video_id": "v1", "title": "T", "theme": "AI", "relevance_score": 0, "why_now": "No news."}]
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        with patch("db.DB_PATH", db_path):
            import db as db_module
            db_module.init_db()
            from fetch_metrics import write_publishing_queue
            with patch("fetch_metrics.classify_video_themes", return_value={"v1": "AI theme"}), \
                 patch("fetch_metrics.rank_videos_by_news", return_value=ranked), \
                 patch("fetch_metrics.anthropic.Anthropic"):
                write_publishing_queue([
                    {"video_id": "v1", "privacy_status": "private", "title": "T", "description": "D"}
                ])
            with sqlite3.connect(db_path) as conn:
                row = conn.execute("SELECT videos_analyzed, result_json FROM publishing_queue").fetchone()
                assert row[0] == 1
                result = json.loads(row[1])
                assert len(result["ranked_videos"]) == 1
                assert result["ranked_videos"][0]["video_id"] == "v1"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_fetch_metrics.py::test_write_publishing_queue_skips_when_no_unpublished_videos tests/test_fetch_metrics.py::test_write_publishing_queue_skips_without_anthropic_key tests/test_fetch_metrics.py::test_write_publishing_queue_writes_result_json -v
```

Expected: FAIL — `ImportError: cannot import name 'write_publishing_queue'`

- [ ] **Step 3: Add imports to fetch_metrics.py**

At the top of `fetch_metrics.py`, after the existing imports, add:

```python
import json

import anthropic

from ai_client import classify_video_themes, fetch_news_headlines, rank_videos_by_news
```

- [ ] **Step 4: Add write_publishing_queue function to fetch_metrics.py**

Add this function after `write_retention_rolling_windows` and before `main()`:

```python
def write_publishing_queue(videos: list[dict]) -> None:
    """Classify unpublished video themes, rank by news relevance, persist to DB."""
    unpublished = [v for v in videos if v.get("privacy_status") != "public"]
    if not unpublished:
        print("  No unpublished videos, skipping publishing queue.")
        return

    print(f"  Found {len(unpublished)} unpublished videos.")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        print("  ANTHROPIC_API_KEY not set, skipping publishing queue.")
        return

    ai = anthropic.Anthropic(api_key=anthropic_key)
    analyzed_at = datetime.now(timezone.utc).isoformat()

    print("  Classifying video themes...")
    themes = classify_video_themes(ai, unpublished)
    videos_with_themes = [
        {**v, "theme": themes.get(v["video_id"], "General workforce topics")}
        for v in unpublished
    ]

    headlines: list[dict] = []
    news_key = os.environ.get("NEWS_API_KEY")
    if news_key:
        print("  Fetching news headlines...")
        headlines = fetch_news_headlines(news_key)
    else:
        print("  NEWS_API_KEY not set, skipping news fetch.")

    print("  Ranking videos by news relevance...")
    ranked = rank_videos_by_news(ai, videos_with_themes, headlines)

    result = {
        "news_available": bool(headlines),
        "ranked_videos": ranked,
        "news_headlines": headlines,
    }

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO publishing_queue(analyzed_at, videos_analyzed, news_stories_count, result_json) "
            "VALUES (?, ?, ?, ?)",
            (analyzed_at, len(unpublished), len(headlines), json.dumps(result)),
        )
    print(f"  Publishing queue written: {len(ranked)} videos ranked against {len(headlines)} headlines.")
```

- [ ] **Step 5: Call write_publishing_queue in main()**

At the end of `main()`, after the `write_retention_rolling_windows` call:

```python
    print("Analyzing publishing queue...")
    try:
        write_publishing_queue(videos)
    except Exception as e:
        print(f"  Publishing queue failed ({e.__class__.__name__}), skipping.")
```

- [ ] **Step 6: Run the new tests to verify they pass**

```bash
.venv/bin/python -m pytest tests/test_fetch_metrics.py -v
```

Expected: all 5 tests PASS (2 existing + 3 new).

- [ ] **Step 7: Run the full test suite**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add fetch_metrics.py tests/test_fetch_metrics.py
git commit -m "feat: add write_publishing_queue to fetch pipeline"
```

---

## Task 6: Add Publishing Queue Section to app.py

**Files:**
- Modify: `app.py`

No new tests for this task — the rendering logic is Streamlit UI code and is verified by visual inspection in the browser.

- [ ] **Step 1: Add json import to app.py**

At the top of `app.py`, after the existing imports, add:

```python
import json
```

- [ ] **Step 2: Add publishing_queue data load**

In `app.py`, after the `retention_buckets = load(...)` call (around line 96–99), add:

```python
publishing_queue = load(
    "SELECT analyzed_at, videos_analyzed, news_stories_count, result_json "
    "FROM publishing_queue ORDER BY analyzed_at DESC LIMIT 1"
)
```

- [ ] **Step 3: Add the Publishing Queue section at the bottom of app.py**

After the Growth Projections section (the final `st.plotly_chart` call), append:

```python
# --- Publishing Queue ---

st.subheader("Publishing Queue")
st.caption(
    "Unpublished episodes ranked by relevance to today's top news stories. "
    "Updated every 4 hours. Use this to decide which story to schedule in YouTube Studio."
)

if publishing_queue.empty:
    st.info(
        "No publishing queue data yet. "
        "Set ANTHROPIC_API_KEY and NEWS_API_KEY, then run `python fetch_metrics.py`."
    )
else:
    pq = publishing_queue.iloc[0]
    analyzed_at = pd.to_datetime(pq["analyzed_at"]).tz_localize(None)
    hours_ago = (pd.Timestamp.utcnow().tz_localize(None) - analyzed_at).total_seconds() / 3600

    meta_col, warn_col = st.columns([4, 1])
    with meta_col:
        st.caption(
            f"Analyzed {analyzed_at.strftime('%b %d, %H:%M UTC')} · "
            f"{int(pq['videos_analyzed'])} unpublished videos · "
            f"{int(pq['news_stories_count'])} news stories"
        )
    with warn_col:
        if hours_ago > 8:
            st.warning(f"⚠ {hours_ago:.0f}h stale")

    result = json.loads(pq["result_json"])
    ranked = result.get("ranked_videos", [])

    if not result.get("news_available"):
        st.warning("News headlines unavailable — videos shown by theme only, not ranked by current events.")

    if not ranked:
        st.info("No unpublished videos in queue.")
    else:
        for item in ranked:
            score = item.get("relevance_score", 0)
            with st.container(border=True):
                left, right = st.columns([5, 1])
                with left:
                    st.markdown(f"**#{item['rank']} — {item['title']}**")
                    st.caption(f"🏷 {item.get('theme', '')}")
                    st.markdown(f"*{item.get('why_now', '')}*")
                with right:
                    st.metric("Relevance", f"{score}/10")
                    st.progress(score / 10)

    headlines = result.get("news_headlines", [])
    if headlines:
        with st.expander(f"News headlines used ({len(headlines)})"):
            for h in headlines:
                st.markdown(f"- **{h['title']}** — {h.get('source', '')}")
```

- [ ] **Step 4: Verify the app starts without errors**

```bash
.venv/bin/python -c "
import ast, sys
with open('app.py') as f:
    src = f.read()
try:
    ast.parse(src)
    print('app.py parses OK')
except SyntaxError as e:
    print(f'SyntaxError: {e}')
    sys.exit(1)
"
```

Expected: `app.py parses OK`

- [ ] **Step 5: Run the full test suite one final time**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add app.py
git commit -m "feat: add Publishing Queue section to dashboard"
```

---

## Task 7: Wire Up Secrets and Deploy

**Files:**
- Modify: `.github/workflows/fetch-analytics.yml`
- Modify: `.streamlit/secrets.toml.example` (if it exists, otherwise skip)

- [ ] **Step 1: Add new env vars to the workflow**

In `.github/workflows/fetch-analytics.yml`, find the `env:` block under the `Run fetch` step and add the two new secrets:

```yaml
      - name: Run fetch
        env:
          YT_CLIENT_ID: ${{ secrets.YT_CLIENT_ID }}
          YT_CLIENT_SECRET: ${{ secrets.YT_CLIENT_SECRET }}
          YT_REFRESH_TOKEN: ${{ secrets.YT_REFRESH_TOKEN }}
          YT_CHANNEL_ID: ${{ secrets.YT_CHANNEL_ID }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          NEWS_API_KEY: ${{ secrets.NEWS_API_KEY }}
```

- [ ] **Step 2: Document secrets in the example file**

Open `.streamlit/secrets.toml.example`. Add:

```toml
# AI + News for Publishing Queue
ANTHROPIC_API_KEY = "your-anthropic-api-key"
NEWS_API_KEY = "your-newsapi-org-api-key"
```

- [ ] **Step 3: Add secrets to GitHub repository**

Get an Anthropic API key from console.anthropic.com and a NewsAPI key from newsapi.org (free tier).

```bash
gh secret set ANTHROPIC_API_KEY --repo cjmurphy4810/human-workforce-analytics
# paste key when prompted

gh secret set NEWS_API_KEY --repo cjmurphy4810/human-workforce-analytics
# paste key when prompted
```

- [ ] **Step 4: Commit workflow changes**

```bash
git add .github/workflows/fetch-analytics.yml .streamlit/secrets.toml.example
git commit -m "chore: add ANTHROPIC_API_KEY and NEWS_API_KEY to fetch workflow"
```

- [ ] **Step 5: Push all commits**

```bash
git push origin main
```

If rejected (cron ran while you were working):

```bash
git pull --rebase origin main && git push origin main
```

- [ ] **Step 6: Verify with a local test run**

Set both keys locally and run the full fetch:

```bash
export ANTHROPIC_API_KEY="your-key"
export NEWS_API_KEY="your-key"
export YT_CLIENT_ID=$(python3 -c "import json; print(json.load(open('.oauth_credentials.json'))['client_id'])")
export YT_CLIENT_SECRET=$(python3 -c "import json; print(json.load(open('.oauth_credentials.json'))['client_secret'])")
export YT_REFRESH_TOKEN=$(python3 -c "import json; print(json.load(open('.oauth_credentials.json'))['refresh_token'])")
export YT_CHANNEL_ID=UCHDU3z8f5_HJzJL1w2J2EaQ
.venv/bin/python fetch_metrics.py
```

Expected output includes:
```
Analyzing publishing queue...
  Found N unpublished videos.
  Classifying video themes...
  Fetching news headlines...
  Ranking videos by news relevance...
  Publishing queue written: N videos ranked against 20 headlines.
```

- [ ] **Step 7: Commit the updated data.db and push**

```bash
git add data.db
git commit -m "chore: populate publishing queue with initial analysis"
git push origin main
```

After push, Streamlit Cloud redeploys (~30s) and the Publishing Queue section appears at the bottom of the dashboard.
