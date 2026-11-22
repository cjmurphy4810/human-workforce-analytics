# Analytics Database Publishing Repair

## Intent

Restore daily statistics updates for every configured YouTube channel and keep the repository-backed SQLite database publishable over time.

## Root cause

The collector still succeeds, but `data.db` reached GitHub's 100 MiB single-file limit. The workflow then fails in its commit/push step, so the newly collected data never reaches Streamlit. The main avoidable growth comes from rolling retention rows, publishing-queue snapshots, and content-intelligence score batches that production reads only at their latest point.

## Goals

- Keep the latest rolling-retention row for each channel, video, and window kind.
- Keep the latest publishing-queue row and latest content-intelligence score batch for each channel.
- Preserve one year plus a small safety margin of channel and video trend snapshots.
- Compact SQLite after pruning and verify integrity and file size before publication.
- Keep all channel cleanup strictly partitioned by channel.

## Non-goals

- Change YouTube API collection semantics, credentials, quotas, or dashboard calculations.
- Replace SQLite or move production data to a new service.
- Remove the trend history the dashboard exposes.

## Approaches considered

1. Move `data.db` to Git LFS or external object storage. This removes the GitHub blob limit but adds deployment infrastructure and does not stop unbounded table growth.
2. Split one database per channel. This delays the limit but complicates every dashboard query and still permits unbounded growth.
3. Prune data the production UI never reads, cap visible trend history, compact, and enforce a publish-size gate. This is selected because it fixes the present failure with the least architectural change and bounds future growth.

## Design

A focused database-maintenance module owns retention rules, compaction, integrity checks, and the publish-size assertion. `fetch_metrics.py` runs it once after all requested channels finish, so cross-channel collection behavior remains unchanged. The GitHub Actions workflow runs the same health check immediately before committing `data.db`. The checked-in database is compacted once as part of this repair so the next deployment starts safely below the limit.

## Constraints

- Keep all five existing channel configurations functional.
- Preserve the latest row per channel/video/window for rolling retention.
- Preserve the latest queue row and latest score batch per channel.
- Preserve 370 days of channel and per-video snapshots.
- Fail before `git push` if integrity is not `ok` or the file exceeds 95 MiB.
