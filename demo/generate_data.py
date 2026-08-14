"""Build the deterministic, entirely fictional database used by the public demo."""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from demo.config import DEMO_AS_OF, DEMO_CHANNEL_KEY, DEMO_CHANNEL_NAME, DEMO_DB_PATH
from demo.db import connect_demo_db


PLAYLISTS = {
    "pl_agent": "Reliable AI Agents",
    "pl_eval": "Evaluation Systems in Practice",
    "pl_retrieval": "Production Retrieval Architecture",
    "pl_observe": "AI Observability Lab",
    "pl_secure": "Secure Automation",
    "pl_short": "Engineering Shorts",
    "pl_visual": "Visual Engineering Briefings",
    "pl_hd": "Deep-Dive Workshops",
    "pl_original": "Studio Originals",
}

_EPISODES = {
    "pl_agent": (
        "Designing Agents That Fail Gracefully",
        "State Machines for Long-Running Assistants",
        "A Practical Tool-Calling Contract",
        "Recovery Paths for Multi-Step Workflows",
        "Human Checkpoints in Autonomous Systems",
        "Testing Agent Memory Without Guesswork",
    ),
    "pl_eval": (
        "Build a Small but Trustworthy Eval Set",
        "Regression Gates for Prompt Changes",
        "Scoring Retrieval Answers with Evidence",
        "When Pairwise Evaluation Beats Rubrics",
        "Calibrating Automated Reviewers",
        "From Failure Clusters to Better Tests",
    ),
    "pl_retrieval": (
        "Chunking Technical Manuals for Search",
        "Hybrid Retrieval Beyond the Prototype",
        "Metadata Filters That Stay Maintainable",
        "Reranking Under a Tight Latency Budget",
        "Index Refreshes Without Downtime",
        "Diagnosing Empty Retrieval Results",
    ),
    "pl_observe": (
        "Trace Every Step of an AI Request",
        "Useful Metrics for Model Pipelines",
        "Debugging a Sudden Quality Drop",
        "Sampling Production Traces Responsibly",
        "Dashboards for Retrieval Health",
        "Turning User Feedback into Signals",
    ),
    "pl_secure": (
        "Threat Modeling an Automation Workflow",
        "Secrets Boundaries for Tool-Using Models",
        "Defending Retrieval from Poisoned Inputs",
        "Least Privilege for AI Integrations",
        "Audit Trails People Can Actually Read",
        "Safe Defaults for External Actions",
    ),
    "pl_short": (
        "One-Minute Eval Triage",
        "A Cleaner Retry Loop",
        "Three Retrieval Metrics Explained",
        "Fast Prompt Versioning",
        "The Smallest Useful Trace",
        "A Safer Approval Gate",
    ),
    "pl_visual": (
        "How an Agent Run Moves Through a System",
        "A Visual Map of Hybrid Search",
        "Reading a Model Trace End to End",
        "The Anatomy of an Evaluation Pipeline",
        "Where Latency Hides in Retrieval",
        "A Diagram of Secure Tool Access",
    ),
    "pl_hd": (
        "Workshop: Build an Evaluation Harness",
        "Workshop: Production-Grade Retrieval",
        "Workshop: Observable Agent Workflows",
        "Workshop: Secure Automation Boundaries",
        "Workshop: Diagnose Quality Regressions",
        "Workshop: Ship a Reliable AI Feature",
    ),
    "pl_original": (
        "The Reliability Review",
        "Signals from the Evaluation Desk",
        "Architecture Notes: Retrieval at Scale",
        "Inside the Observability Lab",
        "The Secure Systems Briefing",
        "Engineering Decisions That Aged Well",
    ),
}

_TRAFFIC_SOURCES = (
    ("YT_SEARCH", 32),
    ("RELATED_VIDEO", 27),
    ("BROWSE_FEATURES", 24),
    ("EXTERNAL", 12),
    ("ADVERTISING", 5),
)
_GEOGRAPHIES = (("US", 58), ("IN", 14), ("GB", 11), ("CA", 9), ("DE", 8))
_CHANNEL_TABLES = (
    "channel_snapshots",
    "videos",
    "video_snapshots",
    "daily_video_metrics",
    "daily_channel_metrics",
    "retention_buckets",
    "daily_geo_metrics",
    "publishing_queue",
    "playlists",
    "playlist_videos",
    "queue_recommendations",
    "video_traffic_source_metrics",
    "channel_traffic_sources",
    "ci_video_scores",
    "ci_content_assets",
)


def _catalog() -> list[dict[str, object]]:
    catalog: list[dict[str, object]] = []
    start = DEMO_AS_OF - timedelta(days=180)
    sequence = 0
    for playlist_id in sorted(PLAYLISTS):
        for position, title in enumerate(_EPISODES[playlist_id]):
            sequence += 1
            if playlist_id == "pl_short":
                duration = 42 + position * 3
            elif playlist_id == "pl_hd":
                duration = 1_440 + position * 96
            elif playlist_id == "pl_visual":
                duration = 660 + position * 42
            elif playlist_id == "pl_original":
                duration = 900 + position * 54
            else:
                duration = 480 + ((sequence * 47) % 330)
            catalog.append(
                {
                    "video_id": f"aeg_demo_{sequence:03d}",
                    "playlist_id": playlist_id,
                    "position": position,
                    "title": title,
                    "published": start + timedelta(days=(sequence - 1) * 3),
                    "duration": duration,
                }
            )
    return sorted(catalog, key=lambda item: str(item["video_id"]))


def _split(total: int, weighted_keys: tuple[tuple[str, int], ...]) -> dict[str, int]:
    result: dict[str, int] = {}
    remaining = total
    for key, weight in weighted_keys[:-1]:
        value = total * weight // 100
        result[key] = value
        remaining -= value
    result[weighted_keys[-1][0]] = remaining
    return result


def _insert_many(conn: sqlite3.Connection, sql: str, rows: list[tuple]) -> None:
    conn.executemany(sql, sorted(rows))


def build_demo_database(path: Path, *, seed: int = 8142026) -> None:
    """Generate a validated demo database and atomically install it at ``path``."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    if temporary.exists():
        temporary.unlink()

    rng = random.Random(seed)
    dates = [DEMO_AS_OF - timedelta(days=offset) for offset in reversed(range(184))]
    catalog = _catalog()

    video_rows: list[tuple] = []
    playlist_rows: list[tuple] = []
    playlist_video_rows: list[tuple] = []
    daily_video_rows: list[tuple] = []
    daily_channel_rows: list[tuple] = []
    geo_rows: list[tuple] = []
    video_traffic_rows: list[tuple] = []
    channel_traffic_rows: list[tuple] = []
    channel_snapshot_rows: list[tuple] = []
    video_snapshot_rows: list[tuple] = []

    for item in catalog:
        video_rows.append(
            (
                DEMO_CHANNEL_KEY,
                item["video_id"],
                item["title"],
                f"A fictional {DEMO_CHANNEL_NAME} lesson created only for the public demo.",
                f"{item['published'].isoformat()}T15:00:00Z",
                item["duration"],
                f"demo://thumbnail/{item['video_id']}",
            )
        )
        playlist_video_rows.append(
            (DEMO_CHANNEL_KEY, item["playlist_id"], item["video_id"], item["position"])
        )

    for playlist_id, title in sorted(PLAYLISTS.items()):
        item_count = sum(item["playlist_id"] == playlist_id for item in catalog)
        playlist_rows.append(
            (
                DEMO_CHANNEL_KEY,
                playlist_id,
                title,
                "A fictional collection generated for the public analytics demo.",
                "2026-02-01T15:00:00Z",
                item_count,
                f"demo://playlist/{playlist_id}",
            )
        )

    cumulative_views: defaultdict[str, int] = defaultdict(int)
    cumulative_likes: defaultdict[str, int] = defaultdict(int)
    cumulative_comments: defaultdict[str, int] = defaultdict(int)
    cumulative_subscribers = 0
    cumulative_channel_views = 0
    recent_views: defaultdict[str, list[tuple[date, int]]] = defaultdict(list)

    weekly_pct = (94, 97, 101, 105, 113, 122, 108)
    for day_index, metric_date in enumerate(dates):
        growth_pct = 100 + day_index * 38 // (len(dates) - 1)
        day_views = 0
        day_minutes = 0.0
        day_likes = 0
        day_subscribers = 0
        traffic_totals: defaultdict[str, int] = defaultdict(int)
        traffic_minutes: defaultdict[str, float] = defaultdict(float)
        active = [item for item in catalog if item["published"] <= metric_date]

        for item in active:
            video_index = int(str(item["video_id"])[-3:])
            age_days = (metric_date - item["published"]).days
            base = 28 + (video_index * 19 % 74)
            launch_pct = max(38, 132 - age_days // 2)
            noise_pct = 100 + rng.randint(-16, 16)
            views = max(
                1,
                base
                * weekly_pct[metric_date.weekday()]
                * growth_pct
                * launch_pct
                * noise_pct
                // 100_000_000,
            )
            duration = int(item["duration"])
            completion_permille = 430 + (video_index * 23 % 170) + rng.randint(-25, 25)
            average_duration = round(duration * completion_permille / 1_000, 1)
            minutes = round(views * average_duration / 60.0, 2)
            likes = views * (38 + video_index % 29) // 1_000
            subscribers = views * (3 + video_index % 6) // 1_000
            comments = likes // 18

            daily_video_rows.append(
                (
                    metric_date.isoformat(),
                    DEMO_CHANNEL_KEY,
                    item["video_id"],
                    views,
                    minutes,
                    average_duration,
                    likes,
                    subscribers,
                )
            )
            day_views += views
            day_minutes += minutes
            day_likes += likes
            day_subscribers += subscribers
            cumulative_views[str(item["video_id"])] += views
            cumulative_likes[str(item["video_id"])] += likes
            cumulative_comments[str(item["video_id"])] += comments
            recent_views[str(item["video_id"])].append((metric_date, views))

            source_views = _split(views, _TRAFFIC_SOURCES)
            for source, _ in _TRAFFIC_SOURCES:
                source_average = average_duration * (82 if source == "ADVERTISING" else 100) / 100
                source_minutes = round(source_views[source] * source_average / 60.0, 2)
                video_traffic_rows.append(
                    (
                        metric_date.isoformat(),
                        DEMO_CHANNEL_KEY,
                        item["video_id"],
                        source,
                        source_views[source],
                        source_minutes,
                        round(source_average, 1),
                    )
                )
                traffic_totals[source] += source_views[source]
                traffic_minutes[source] += source_minutes

        subscribers_lost = day_subscribers // 9
        daily_channel_rows.append(
            (
                metric_date.isoformat(),
                DEMO_CHANNEL_KEY,
                day_views,
                round(day_minutes, 2),
                day_subscribers,
                subscribers_lost,
            )
        )

        geo_views = _split(day_views, _GEOGRAPHIES)
        geo_subscribers = _split(day_subscribers, _GEOGRAPHIES)
        geo_likes = _split(day_likes, _GEOGRAPHIES)
        for country, _ in _GEOGRAPHIES:
            geo_rows.append(
                (
                    metric_date.isoformat(),
                    DEMO_CHANNEL_KEY,
                    country,
                    geo_views[country],
                    geo_subscribers[country],
                    geo_likes[country],
                )
            )

        for source, _ in _TRAFFIC_SOURCES:
            channel_traffic_rows.append(
                (
                    metric_date.isoformat(),
                    DEMO_CHANNEL_KEY,
                    source,
                    traffic_totals[source],
                    round(traffic_minutes[source], 2),
                )
            )

        cumulative_channel_views += day_views
        cumulative_subscribers += day_subscribers - subscribers_lost
        captured_at = f"{metric_date.isoformat()}T23:00:00Z"
        channel_snapshot_rows.append(
            (
                captured_at,
                DEMO_CHANNEL_KEY,
                "UC_DEMO_AI_ENGINEERING_GENIUS",
                cumulative_subscribers,
                cumulative_channel_views,
                len(active),
            )
        )
        if day_index % 7 == 0 or metric_date == DEMO_AS_OF:
            for item in active:
                video_id = str(item["video_id"])
                video_snapshot_rows.append(
                    (
                        captured_at,
                        DEMO_CHANNEL_KEY,
                        video_id,
                        cumulative_views[video_id],
                        cumulative_likes[video_id],
                        cumulative_comments[video_id],
                    )
                )

    retention_rows: list[tuple] = []
    score_rows: list[tuple] = []
    cutoff = DEMO_AS_OF - timedelta(days=27)
    for item in catalog:
        video_id = str(item["video_id"])
        video_index = int(video_id[-3:])
        retention_25 = round((610 + video_index * 7 % 220 + rng.randint(-18, 18)) / 1_000, 3)
        retention_75 = round(retention_25 * (430 + video_index * 11 % 270) / 1_000, 3)
        retention_rows.append(
            (
                DEMO_CHANNEL_KEY,
                video_id,
                cutoff.isoformat(),
                DEMO_AS_OF.isoformat(),
                "LAST_28_DAYS",
                sum(views for day, views in recent_views[video_id] if day >= cutoff),
                retention_25,
                retention_75,
                f"{DEMO_AS_OF.isoformat()}T23:30:00Z",
            )
        )
        overall = round(48 + (video_index * 13 % 46) + rng.randint(-3, 3), 1)
        tier = "PROMOTE" if overall >= 78 else "GROW" if overall >= 62 else "MONITOR"
        total_views = cumulative_views[video_id]
        score_rows.append(
            (
                DEMO_AS_OF.isoformat(),
                DEMO_CHANNEL_KEY,
                video_id,
                tier,
                round(min(99.0, overall + 2.4), 1),
                round(min(99.0, overall + 5.1), 1),
                round(max(0.0, overall - 4.3), 1),
                round(min(99.0, overall + 1.7), 1),
                overall,
                total_views,
                round(42 + video_index % 19, 1),
                round(3.8 + video_index % 27 / 10, 2),
                round(0.3 + video_index % 8 / 10, 2),
                round(0.02 + video_index % 5 / 100, 2),
            )
        )

    recommended = sorted(score_rows, key=lambda row: (-float(row[8]), str(row[2])))[:8]
    recommendation_rows: list[tuple] = []
    for rank, score in enumerate(recommended, start=1):
        recommendation_rows.append(
            (
                DEMO_CHANNEL_KEY,
                score[2],
                f"{DEMO_AS_OF.isoformat()}T18:00:00Z",
                (DEMO_AS_OF + timedelta(days=rank * 2)).isoformat(),
                rank,
                round(float(score[8]) / 100, 3),
                "Reliable AI systems",
                "Strong durable engagement in this fictional demo cohort.",
            )
        )

    title_by_id = {str(item["video_id"]): str(item["title"]) for item in catalog}
    asset_rows: list[tuple] = []
    for index, score in enumerate(recommended[:6], start=1):
        video_id = str(score[2])
        asset_rows.append(
            (
                f"demo_asset_{index:02d}",
                DEMO_CHANNEL_KEY,
                video_id,
                title_by_id[video_id],
                "community_post",
                f"A closer look at {title_by_id[video_id]}",
                "Draft a practical discussion prompt about the engineering "
                "tradeoffs in this fictional lesson.",
                f"{DEMO_AS_OF.isoformat()}T19:{index:02d}:00Z",
                "draft",
                None,
                None,
                "Synthetic public-demo asset; not approved for publishing.",
            )
        )

    queue_payload = json.dumps(
        {
            "channel": DEMO_CHANNEL_KEY,
            "generated_for": DEMO_AS_OF.isoformat(),
            "recommended_video_ids": [row[1] for row in recommendation_rows],
            "themes": ["evaluation", "observability", "reliable agents"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    publishing_rows = [
        (
            f"{DEMO_AS_OF.isoformat()}T18:00:00Z",
            DEMO_CHANNEL_KEY,
            len(catalog),
            7,
            queue_payload,
        )
    ]

    try:
        with connect_demo_db(temporary) as conn:
            conn.execute("PRAGMA journal_mode = DELETE")
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("PRAGMA user_version = 1")
            _insert_many(
                conn,
                "INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?, ?)",
                video_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO playlists VALUES (?, ?, ?, ?, ?, ?, ?)",
                playlist_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO playlist_videos VALUES (?, ?, ?, ?)",
                playlist_video_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO daily_video_metrics "
                "(metric_date, channel, video_id, views, estimated_minutes_watched, "
                "average_view_duration, likes, subscribers_gained) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                daily_video_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO daily_channel_metrics VALUES (?, ?, ?, ?, ?, ?)",
                daily_channel_rows,
            )
            _insert_many(conn, "INSERT INTO daily_geo_metrics VALUES (?, ?, ?, ?, ?, ?)", geo_rows)
            _insert_many(
                conn,
                "INSERT INTO video_traffic_source_metrics VALUES (?, ?, ?, ?, ?, ?, ?)",
                video_traffic_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO channel_traffic_sources VALUES (?, ?, ?, ?, ?)",
                channel_traffic_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO channel_snapshots "
                "(captured_at, channel, channel_id, subscriber_count, view_count, video_count) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                channel_snapshot_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO video_snapshots "
                "(captured_at, channel, video_id, view_count, like_count, comment_count) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                video_snapshot_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO retention_buckets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                retention_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO ci_video_scores "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                score_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO queue_recommendations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                recommendation_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO ci_content_assets "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                asset_rows,
            )
            _insert_many(
                conn,
                "INSERT INTO publishing_queue "
                "(analyzed_at, channel, videos_analyzed, news_stories_count, result_json) "
                "VALUES (?, ?, ?, ?, ?)",
                publishing_rows,
            )
            conn.commit()

        errors = validate_demo_database(temporary)
        if errors:
            raise ValueError("invalid generated demo database: " + "; ".join(errors))
        temporary.replace(path)
    except Exception:
        if temporary.exists():
            temporary.unlink()
        raise


def validate_demo_database(path: Path) -> list[str]:
    """Return human-readable integrity errors for a generated demo database."""
    errors: list[str] = []
    with sqlite3.connect(path) as conn:
        if conn.execute("PRAGMA foreign_key_check").fetchall():
            errors.append("foreign key violations")
        channels = {
            row[0]
            for row in conn.execute("SELECT DISTINCT channel FROM daily_channel_metrics")
        }
        if channels != {DEMO_CHANNEL_KEY}:
            errors.append(f"unexpected channels: {sorted(channels)}")
        days = conn.execute("SELECT COUNT(*) FROM daily_channel_metrics").fetchone()[0]
        if days < 184:
            errors.append(f"insufficient history: {days} days")
        orphan_count = conn.execute(
            "SELECT COUNT(*) FROM daily_video_metrics d "
            "LEFT JOIN videos v ON v.channel=d.channel AND v.video_id=d.video_id "
            "WHERE v.video_id IS NULL"
        ).fetchone()[0]
        if orphan_count:
            errors.append(f"orphan video metrics: {orphan_count}")

        for table in _CHANNEL_TABLES:
            wrong = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE channel != ?", (DEMO_CHANNEL_KEY,)
            ).fetchone()[0]
            if wrong:
                errors.append(f"unexpected channel rows in {table}: {wrong}")

        channel_previous: dict[str, tuple[int, int, int]] = {}
        channel_regressions = 0
        for channel, subscribers, views, videos in conn.execute(
            "SELECT channel, subscriber_count, view_count, video_count "
            "FROM channel_snapshots ORDER BY channel, captured_at, id"
        ):
            if subscribers is None or views is None or videos is None:
                channel_regressions += 1
                continue
            current = (int(subscribers), int(views), int(videos))
            previous = channel_previous.get(channel)
            if previous is not None and any(now < before for now, before in zip(current, previous)):
                channel_regressions += 1
            channel_previous[channel] = current
        if channel_regressions:
            errors.append(f"non-monotonic channel snapshots: {channel_regressions}")

        video_previous: dict[tuple[str, str], tuple[int, int, int]] = {}
        video_regressions = 0
        for channel, video_id, views, likes, comments in conn.execute(
            "SELECT channel, video_id, view_count, like_count, comment_count "
            "FROM video_snapshots ORDER BY channel, video_id, captured_at, id"
        ):
            if views is None or likes is None or comments is None:
                video_regressions += 1
                continue
            key = (channel, video_id)
            current = (int(views), int(likes), int(comments))
            previous = video_previous.get(key)
            if previous is not None and any(now < before for now, before in zip(current, previous)):
                video_regressions += 1
            video_previous[key] = current
        if video_regressions:
            errors.append(f"non-monotonic video snapshots: {video_regressions}")

        invalid_retention = conn.execute(
            "SELECT COUNT(*) FROM retention_buckets "
            "WHERE retention_at_25 < 0 OR retention_at_25 > 1 "
            "OR retention_at_75 < 0 OR retention_at_75 > retention_at_25"
        ).fetchone()[0]
        if invalid_retention:
            errors.append(f"invalid retention bounds: {invalid_retention}")

        playlist_mismatches = conn.execute(
            "SELECT COUNT(*) FROM playlists p "
            "LEFT JOIN (SELECT channel, playlist_id, COUNT(*) AS actual_count "
            "FROM playlist_videos GROUP BY channel, playlist_id) pv "
            "ON pv.channel=p.channel AND pv.playlist_id=p.playlist_id "
            "WHERE COALESCE(p.item_count, -1) != COALESCE(pv.actual_count, 0)"
        ).fetchone()[0]
        if playlist_mismatches:
            errors.append(f"playlist item count mismatch: {playlist_mismatches}")

        snapshot_count_mismatches = conn.execute(
            "SELECT COUNT(*) FROM channel_snapshots s "
            "WHERE COALESCE(s.video_count, -1) != ("
            "SELECT COUNT(*) FROM videos v WHERE v.channel=s.channel "
            "AND date(v.published_at) <= date(s.captured_at))"
        ).fetchone()[0]
        if snapshot_count_mismatches:
            errors.append(f"channel snapshot video count mismatch: {snapshot_count_mismatches}")
    return errors


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEMO_DB_PATH)
    parser.add_argument("--seed", type=int, default=8142026)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    build_demo_database(arguments.output, seed=arguments.seed)
