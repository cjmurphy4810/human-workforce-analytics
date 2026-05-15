# Publishing Queue — Scheduled & Recommended Dates Design Spec
**Date:** 2026-05-15
**Project:** human-workforce-analytics dashboard

## Goal

Enhance the Publishing Queue dashboard section to show each unpublished video's current scheduled publish date (from YouTube Studio) alongside a recommended publish date based on today's relevance ranking. The ranked order already reflects news relevance — this feature makes the actionable "when to publish" question explicit.

## Architecture

No new tables. `scheduled_at` is extracted from the existing `status.publishAt` field (already fetched via the YouTube Data API `status` part) and stored inside the existing `result_json` blob in `publishing_queue`. `recommended_date` is never stored — it is computed at display time as `date.today() + timedelta(days=rank)` so it is always current regardless of when the cron ran.

## Data Flow

1. `youtube_client.fetch_video_details` extracts `status.publishAt` and exposes it as `scheduled_at` (ISO 8601 string or `None` if not scheduled).
2. `fetch_metrics.write_publishing_queue` passes `scheduled_at` through in `videos_with_themes`.
3. `ai_client.rank_videos_by_news` receives `scheduled_at` in the input list but does **not** include it in the Claude prompt. After Claude returns the ranked array, the function merges `scheduled_at` onto each item by matching `video_id` from the input list. This keeps the Claude prompt clean and prevents hallucinated dates.
4. `app.py` computes `recommended_date = date.today() + timedelta(days=item["rank"])` at render time and displays both dates on each card.

## Modified Files

| File | Change |
|------|--------|
| `youtube_client.py` | Extract `status.publishAt` → `scheduled_at` in `fetch_video_details` |
| `ai_client.py` | Merge `scheduled_at` onto ranked output by `video_id` after Claude call |
| `app.py` | Display `scheduled_at` and `recommended_date` on each card; show `⚡ Earlier` indicator when recommended is before scheduled |

No changes to `db.py`, `fetch_metrics.py`, `requirements.txt`, or the GitHub workflow.

## Dashboard Card Layout

Each ranked video card shows (in order):

1. Rank badge + bold title
2. Theme tag (existing)
3. **Scheduled / Recommend date row** (new):
   - `📅 Scheduled: Jun 15, 2026` (from YouTube Studio, or `Not scheduled` if `None`)
   - `→ Recommend: May 16, 2026` (always `today + rank`)
   - `⚡ Earlier` badge if recommended date is strictly before scheduled date
4. Why now text in italic gray (existing)
5. Relevance score metric + progress bar (existing)

## Recommend Date Logic

```
recommended_date = date.today() + timedelta(days=item["rank"])
```

- Rank 1 → tomorrow
- Rank 2 → day after tomorrow
- Rank N → today + N days
- Recomputed fresh on every page load; shifts automatically each cron refresh as rankings change

## scheduled_at Handling

- `status.publishAt` is only present on videos scheduled for auto-publishing in YouTube Studio. Private videos with no scheduled date return `None` → displayed as "Not scheduled".
- Stored as an ISO 8601 string in `result_json`; formatted to `Mon DD, YYYY` at display time.
- If `scheduled_at` is `None`, no `⚡ Earlier` indicator is shown (nothing to compare against).

## Error Handling

- `status.publishAt` key may be absent from the API response — use `.get("publishAt")` with `None` default.
- If `video_id` in a ranked item has no match in the input list (should not happen, but defensive), `scheduled_at` defaults to `None`.
- These changes are additive — if `scheduled_at` is missing from an existing `result_json` row, the display falls back to "Not scheduled" without error.

## Testing

- `test_youtube_client.py`: update `_fake_data_service` fixture to include `"publishAt": "2026-06-15T18:00:00Z"` in the `status` dict; assert `result[0]["scheduled_at"] == "2026-06-15T18:00:00Z"`.
- `test_youtube_client.py`: add a test for a video with no `publishAt` in status; assert `result[0]["scheduled_at"] is None`.
- `test_ai_client.py`: update `test_rank_videos_by_news_returns_ranked_list` to pass `scheduled_at` in the input and assert it appears in the output.
- No tests for `app.py` rendering (visual verification).
