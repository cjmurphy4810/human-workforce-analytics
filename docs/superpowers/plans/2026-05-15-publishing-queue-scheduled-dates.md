# Publishing Queue — Scheduled & Recommended Dates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a scheduled publish date (from YouTube Studio) and a recommended publish date (rank-based, always fresh) to each card in the Publishing Queue dashboard section.

**Architecture:** `youtube_client.fetch_video_details` extracts `status.publishAt` → it flows through `write_publishing_queue` automatically via `{**v, ...}` dict spread → `ai_client.rank_videos_by_news` merges it onto the Claude-ranked output by `video_id` → `app.py` computes `recommended_date = date.today() + timedelta(days=rank)` at render time and displays both dates. No new tables. No changes to `fetch_metrics.py`.

**Tech Stack:** Python 3.12, SQLite, Streamlit, YouTube Data API v3 (existing), `pandas`, `datetime`.

---

## File Map

| Action | File | What changes |
|--------|------|-------------|
| Modify | `youtube_client.py` | Add `scheduled_at` extraction from `status.publishAt` |
| Modify | `tests/test_youtube_client.py` | Update existing test fixture; add 2 new tests |
| Modify | `ai_client.py` | Build `scheduled_lookup`, merge `scheduled_at` onto ranked output in both code paths |
| Modify | `tests/test_ai_client.py` | Update 1 existing test; add 2 new tests |
| Modify | `app.py` | Add scheduled/recommended date row to each ranked card |

---

## Task 1: Extract `scheduled_at` from `fetch_video_details`

**Files:**
- Modify: `youtube_client.py` (line 119)
- Modify: `tests/test_youtube_client.py`

### Background

`fetch_video_details` already requests `part="snippet,statistics,contentDetails,status"`. The `status` object contains `publishAt` (an ISO 8601 datetime string) when the video is scheduled for auto-publishing in YouTube Studio. For private videos with no scheduled date, `publishAt` is absent — use `.get("publishAt")` which returns `None`.

The function currently extracts only `privacy_status` from `item["status"]`. We add one more field.

- [ ] **Step 1: Update the existing privacy_status test to also cover `scheduled_at`**

In `tests/test_youtube_client.py`, update `test_fetch_video_details_includes_privacy_status` to add `"publishAt"` to the `status` dict and assert `scheduled_at` is returned:

```python
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
        "status": {"privacyStatus": "private", "publishAt": "2026-06-15T18:00:00Z"},
    }
    with patch("youtube_client.data_service") as mock_svc:
        mock_svc.return_value = _fake_data_service([fake_item])
        result = youtube_client.fetch_video_details(["v1"])
    assert len(result) == 1
    assert result[0]["privacy_status"] == "private"
    assert result[0]["scheduled_at"] == "2026-06-15T18:00:00Z"
    assert result[0]["video_id"] == "v1"
    assert result[0]["view_count"] == 100
```

- [ ] **Step 2: Add a test for a video with no `publishAt` in status**

Add directly below the previous test:

```python
def test_fetch_video_details_scheduled_at_is_none_when_not_set():
    fake_item = {
        "id": "v2",
        "snippet": {
            "title": "Unscheduled Video",
            "description": "",
            "publishedAt": "2026-01-01T00:00:00Z",
            "thumbnails": {},
        },
        "statistics": {"viewCount": "0", "likeCount": "0", "commentCount": "0"},
        "contentDetails": {"duration": "PT5M"},
        "status": {"privacyStatus": "private"},
    }
    with patch("youtube_client.data_service") as mock_svc:
        mock_svc.return_value = _fake_data_service([fake_item])
        result = youtube_client.fetch_video_details(["v2"])
    assert result[0]["scheduled_at"] is None
```

- [ ] **Step 3: Run both new/updated tests to verify they fail**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
.venv/bin/python -m pytest tests/test_youtube_client.py::test_fetch_video_details_includes_privacy_status tests/test_youtube_client.py::test_fetch_video_details_scheduled_at_is_none_when_not_set -v
```

Expected: FAIL — `KeyError: 'scheduled_at'`

- [ ] **Step 4: Add `scheduled_at` to `fetch_video_details` in `youtube_client.py`**

Find lines 108–120 in `youtube_client.py` (the `details.append({...})` block). Replace only the `"privacy_status"` line — add `"scheduled_at"` immediately after it:

```python
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
                "scheduled_at": item["status"].get("publishAt"),
            })
```

- [ ] **Step 5: Run the full test suite**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add youtube_client.py tests/test_youtube_client.py
git commit -m "feat: extract scheduled_at from status.publishAt in fetch_video_details"
```

---

## Task 2: Merge `scheduled_at` onto ranked output in `ai_client.rank_videos_by_news`

**Files:**
- Modify: `ai_client.py` (lines 59–121)
- Modify: `tests/test_ai_client.py`

### Background

`rank_videos_by_news` receives `videos_with_themes` which will now include `scheduled_at` (from Task 1 flowing through `write_publishing_queue`'s `{**v, ...}` spread). Claude's ranked output does NOT include `scheduled_at` — we merge it back after the API call by matching `video_id`.

There are two code paths in this function:
1. **No headlines** — returns early with a manually built list (score=0). Must include `scheduled_at` here too.
2. **With headlines** — calls Claude, parses JSON, then merges `scheduled_at` onto each item.

- [ ] **Step 1: Update `test_rank_videos_by_news_returns_ranked_list` to pass and assert `scheduled_at`**

In `tests/test_ai_client.py`, find `test_rank_videos_by_news_returns_ranked_list` and replace it:

```python
def test_rank_videos_by_news_returns_ranked_list():
    mock_client = MagicMock()
    ranked = [
        {"rank": 1, "video_id": "v1", "title": "T", "theme": "AI", "relevance_score": 9, "why_now": "Big news."},
    ]
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=json.dumps(ranked))]
    )
    videos = [{"video_id": "v1", "title": "T", "theme": "AI", "scheduled_at": "2026-06-15T18:00:00Z"}]
    headlines = [{"title": "Google lays off 1000", "source": "Reuters", "published_at": "2026-05-13T10:00:00Z"}]
    result = rank_videos_by_news(mock_client, videos, headlines)
    assert result[0]["rank"] == 1
    assert result[0]["relevance_score"] == 9
    assert result[0]["scheduled_at"] == "2026-06-15T18:00:00Z"
    mock_client.messages.create.assert_called_once()
```

- [ ] **Step 2: Add a test for `scheduled_at` = None when not present in input**

Add after the test above:

```python
def test_rank_videos_by_news_scheduled_at_none_when_not_set():
    mock_client = MagicMock()
    ranked = [
        {"rank": 1, "video_id": "v1", "title": "T", "theme": "AI", "relevance_score": 5, "why_now": "Somewhat timely."},
    ]
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=json.dumps(ranked))]
    )
    videos = [{"video_id": "v1", "title": "T", "theme": "AI"}]
    headlines = [{"title": "Some headline", "source": "BBC", "published_at": "2026-05-15T10:00:00Z"}]
    result = rank_videos_by_news(mock_client, videos, headlines)
    assert result[0]["scheduled_at"] is None
```

- [ ] **Step 3: Add a test for `scheduled_at` in the no-headlines path**

Add after the test above:

```python
def test_rank_videos_by_news_empty_headlines_includes_scheduled_at():
    mock_client = MagicMock()
    videos = [{"video_id": "v1", "title": "T", "theme": "AI", "scheduled_at": "2026-07-01T12:00:00Z"}]
    result = rank_videos_by_news(mock_client, videos, [])
    assert result[0]["relevance_score"] == 0
    assert result[0]["scheduled_at"] == "2026-07-01T12:00:00Z"
    mock_client.messages.create.assert_not_called()
```

- [ ] **Step 4: Run the three new/updated tests to verify they fail**

```bash
.venv/bin/python -m pytest tests/test_ai_client.py::test_rank_videos_by_news_returns_ranked_list tests/test_ai_client.py::test_rank_videos_by_news_scheduled_at_none_when_not_set tests/test_ai_client.py::test_rank_videos_by_news_empty_headlines_includes_scheduled_at -v
```

Expected: FAIL — `KeyError: 'scheduled_at'` or `AssertionError`

- [ ] **Step 5: Update `rank_videos_by_news` in `ai_client.py`**

Replace the entire `rank_videos_by_news` function (lines 59–121) with:

```python
def rank_videos_by_news(
    client: anthropic.Anthropic,
    videos_with_themes: list[dict],
    headlines: list[dict],
) -> list[dict]:
    """Rank unpublished videos by relevance to today's news headlines.

    videos_with_themes: list of dicts with keys video_id, title, theme, scheduled_at.
    headlines: list of dicts with keys title, source, published_at.
    Returns: list of {rank, video_id, title, theme, relevance_score, why_now, scheduled_at} sorted rank 1 first.
    """
    if not videos_with_themes:
        return []

    scheduled_lookup = {v["video_id"]: v.get("scheduled_at") for v in videos_with_themes}

    if not headlines:
        return [
            {
                "rank": i + 1,
                "video_id": v["video_id"],
                "title": v["title"],
                "theme": v["theme"],
                "relevance_score": 0,
                "why_now": "News data unavailable for ranking.",
                "scheduled_at": v.get("scheduled_at"),
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
    ranked = json.loads(raw)
    for item in ranked:
        item["scheduled_at"] = scheduled_lookup.get(item.get("video_id"))
    return ranked
```

- [ ] **Step 6: Run the full test suite**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add ai_client.py tests/test_ai_client.py
git commit -m "feat: merge scheduled_at onto ranked output in rank_videos_by_news"
```

---

## Task 3: Display Scheduled and Recommended Dates in `app.py`

**Files:**
- Modify: `app.py` (lines 422–437, the `for item in ranked:` loop)

No new tests for this task — Streamlit rendering is verified visually.

### Background

`date` and `timedelta` are already imported at the top of `app.py` (`from datetime import date, timedelta`). `pd` (pandas) is also available. No new imports needed.

The recommended date formula: `date.today() + timedelta(days=rank)` — so rank 1 gets tomorrow, rank 2 gets the day after, etc. Compute `today` once before the loop.

The `⚡ Earlier` indicator appears when `rec_date < scheduled_date` and `scheduled_at` is not None.

- [ ] **Step 1: Replace the `for item in ranked:` loop in `app.py`**

Find the existing loop at lines 422–437:

```python
        for item in ranked:
            raw_score = item.get("relevance_score", 0)
            try:
                score = float(raw_score)
            except (TypeError, ValueError):
                score = 0.0
            score = max(0.0, min(10.0, score))
            with st.container(border=True):
                left, right = st.columns([5, 1])
                with left:
                    st.markdown(f"**#{item.get('rank', '?')} — {item.get('title', 'Untitled')}**")
                    st.caption(f"🏷 {item.get('theme', '')}")
                    st.markdown(f"<span style='color:gray; font-style:italic;'>{html.escape(item.get('why_now', ''))}</span>", unsafe_allow_html=True)
                with right:
                    st.metric("Relevance", f"{score:.0f}/10")
                    st.progress(score / 10)
```

Replace it with:

```python
        today = date.today()
        for item in ranked:
            rank = item.get("rank", "?")
            raw_score = item.get("relevance_score", 0)
            try:
                score = float(raw_score)
            except (TypeError, ValueError):
                score = 0.0
            score = max(0.0, min(10.0, score))

            scheduled_raw = item.get("scheduled_at")
            if scheduled_raw:
                scheduled_str = pd.to_datetime(scheduled_raw).strftime("%b %d, %Y")
            else:
                scheduled_str = "Not scheduled"

            try:
                rec_date = today + timedelta(days=int(rank))
                rec_str = rec_date.strftime("%b %d, %Y")
            except (TypeError, ValueError):
                rec_date = None
                rec_str = "—"

            show_earlier = (
                scheduled_raw is not None
                and rec_date is not None
                and rec_date < pd.to_datetime(scheduled_raw).date()
            )

            with st.container(border=True):
                left, right = st.columns([5, 1])
                with left:
                    st.markdown(f"**#{rank} — {item.get('title', 'Untitled')}**")
                    st.caption(f"🏷 {item.get('theme', '')}")
                    date_line = f"📅 Scheduled: {scheduled_str} → Recommend: {rec_str}"
                    if show_earlier:
                        date_line += " ⚡ Earlier"
                    st.caption(date_line)
                    st.markdown(f"<span style='color:gray; font-style:italic;'>{html.escape(item.get('why_now', ''))}</span>", unsafe_allow_html=True)
                with right:
                    st.metric("Relevance", f"{score:.0f}/10")
                    st.progress(score / 10)
```

- [ ] **Step 2: Verify `app.py` parses cleanly**

```bash
cd "/Users/zdjimas/VS Code Projects/human-workforce-analytics"
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

- [ ] **Step 3: Run the full test suite**

```bash
.venv/bin/python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add app.py
git commit -m "feat: show scheduled and recommended publish dates in Publishing Queue cards"
```

---

## Final Step: Re-run fetch and push

After all three tasks are done, re-run `fetch_metrics.py` locally to populate a new `publishing_queue` row that includes `scheduled_at` in the JSON, then push.

```bash
ANTHROPIC_API_KEY="..." NEWS_API_KEY="..." \
  YT_CLIENT_ID=$(python3 -c "import json; print(json.load(open('.oauth_credentials.json'))['client_id'])") \
  YT_CLIENT_SECRET=$(python3 -c "import json; print(json.load(open('.oauth_credentials.json'))['client_secret'])") \
  YT_REFRESH_TOKEN=$(python3 -c "import json; print(json.load(open('.oauth_credentials.json'))['refresh_token'])") \
  YT_CHANNEL_ID=UCHDU3z8f5_HJzJL1w2J2EaQ \
  .venv/bin/python fetch_metrics.py

git add data.db
git commit -m "chore: refresh publishing queue with scheduled_at data"
git push origin main
```
