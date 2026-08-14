"""Build the deterministic, entirely fictional database used by the public demo."""

from __future__ import annotations

import argparse
import json
import math
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

_ORGANIC_TRAFFIC_SOURCES = (
    ("YT_SEARCH", 35),
    ("RELATED_VIDEO", 30),
    ("BROWSE_FEATURES", 25),
    ("EXTERNAL", 10),
)
_PROMOTED_TRAFFIC_SOURCES = (
    ("YT_SEARCH", 30),
    ("RELATED_VIDEO", 27),
    ("BROWSE_FEATURES", 23),
    ("EXTERNAL", 15),
    ("ADVERTISING", 5),
)
_REQUIRED_TRAFFIC_SOURCES = {
    source for source, _ in _PROMOTED_TRAFFIC_SOURCES
}
_RETENTION_WINDOWS = (
    (7, "rolling7"),
    (30, "rolling30"),
    (90, "rolling90"),
    (365, "rolling365"),
)
_CANONICAL_CI_TIERS = {
    "top_episode",
    "subscriber_magnet",
    "hidden_gem",
    "average",
    "underperformer",
}
_VALID_ASSET_TYPES = {
    "community_post",
    "executive_poll",
    "quote_card",
    "executive_tip",
    "discussion_question",
    "linkedin_post",
    "blog_outline",
    "newsletter_summary",
    "course_lesson",
    "assessment_question",
    "infographic_text",
    "image_prompt",
    "short_video_hook",
}
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


def _split(
    total: int,
    weighted_keys: tuple[tuple[str, int], ...],
    *,
    residual_key: str | None = None,
) -> dict[str, int]:
    result = {key: total * weight // 100 for key, weight in weighted_keys}
    residual_key = residual_key or weighted_keys[-1][0]
    result[residual_key] += total - sum(result.values())
    return result


def _insert_many(conn: sqlite3.Connection, sql: str, rows: list[tuple]) -> None:
    conn.executemany(sql, sorted(rows))


def _percentile_ranks(values: dict[str, float]) -> dict[str, float]:
    ordered = sorted(values.values())
    denominator = max(len(ordered) - 1, 1)
    return {
        video_id: ordered.index(value) / denominator * 100
        for video_id, value in values.items()
    }


def _classify_ci_tier(
    overall: float,
    views_percentile: float,
    subscriber_magnet: float,
    hidden_gem: float,
) -> str:
    if overall >= 70 and views_percentile >= 60:
        return "top_episode"
    if subscriber_magnet >= 70:
        return "subscriber_magnet"
    if hidden_gem >= 65 and views_percentile < 40:
        return "hidden_gem"
    if overall >= 30:
        return "average"
    return "underperformer"


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
    cumulative_minutes: defaultdict[str, float] = defaultdict(float)
    cumulative_likes: defaultdict[str, int] = defaultdict(int)
    cumulative_video_subscribers: defaultdict[str, int] = defaultdict(int)
    cumulative_ad_views: defaultdict[str, int] = defaultdict(int)
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
            cumulative_minutes[str(item["video_id"])] += minutes
            cumulative_likes[str(item["video_id"])] += likes
            cumulative_video_subscribers[str(item["video_id"])] += subscribers
            cumulative_comments[str(item["video_id"])] += comments
            recent_views[str(item["video_id"])].append((metric_date, views))

            is_promoted = video_index % 4 == 0
            source_weights = (
                _PROMOTED_TRAFFIC_SOURCES if is_promoted else _ORGANIC_TRAFFIC_SOURCES
            )
            source_views = _split(views, source_weights, residual_key="YT_SEARCH")
            if is_promoted:
                rounded_ad_views = (views * 5 + 50) // 100
                ad_adjustment = rounded_ad_views - source_views["ADVERTISING"]
                source_views["ADVERTISING"] = rounded_ad_views
                source_views["YT_SEARCH"] -= ad_adjustment
            source_details: dict[str, tuple[float, float]] = {}
            allocated_minutes = 0.0
            duration_pct = {
                "RELATED_VIDEO": 103,
                "BROWSE_FEATURES": 98,
                "EXTERNAL": 90,
                "ADVERTISING": 82,
            }
            for source, _ in source_weights:
                if source == "YT_SEARCH":
                    continue
                source_average = average_duration * duration_pct[source] / 100
                source_minutes = round(source_views[source] * source_average / 60.0, 2)
                source_details[source] = (source_minutes, round(source_average, 1))
                allocated_minutes += source_minutes
            search_minutes = round(minutes - allocated_minutes, 2)
            search_views = source_views["YT_SEARCH"]
            search_average = search_minutes * 60.0 / search_views if search_views else 0.0
            source_details["YT_SEARCH"] = (search_minutes, round(search_average, 1))

            for source, _ in source_weights:
                source_minutes, source_average = source_details[source]
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
                if source == "ADVERTISING":
                    cumulative_ad_views[str(item["video_id"])] += source_views[source]

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

        for source in sorted(_REQUIRED_TRAFFIC_SOURCES):
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
    for item in catalog:
        video_id = str(item["video_id"])
        video_index = int(video_id[-3:])
        for window_index, (window_days, window_kind) in enumerate(_RETENTION_WINDOWS):
            window_start = DEMO_AS_OF - timedelta(days=window_days)
            retention_25 = round(
                (
                    610
                    + video_index * 7 % 220
                    - window_index * 8
                    + rng.randint(-12, 12)
                )
                / 1_000,
                3,
            )
            retention_75 = round(
                retention_25 * (430 + video_index * 11 % 270) / 1_000,
                3,
            )
            retention_rows.append(
                (
                    DEMO_CHANNEL_KEY,
                    video_id,
                    window_start.isoformat(),
                    DEMO_AS_OF.isoformat(),
                    window_kind,
                    sum(
                        views
                        for day, views in recent_views[video_id]
                        if window_start <= day <= DEMO_AS_OF
                    ),
                    retention_25,
                    retention_75,
                    f"{DEMO_AS_OF.isoformat()}T23:30:00Z",
                )
            )
    views_values = {video_id: float(views) for video_id, views in cumulative_views.items()}
    watch_rates = {
        str(item["video_id"]): (
            cumulative_minutes[str(item["video_id"])]
            * 60.0
            / max(
                cumulative_views[str(item["video_id"])] * int(item["duration"]),
                1,
            )
            * 100
        )
        for item in catalog
    }
    like_rates = {
        video_id: cumulative_likes[video_id] / max(cumulative_views[video_id], 1) * 100
        for video_id in cumulative_views
    }
    subscriber_rates = {
        video_id: cumulative_video_subscribers[video_id]
        / max(cumulative_views[video_id], 1)
        * 100
        for video_id in cumulative_views
    }
    promotion_ratios = {
        video_id: cumulative_ad_views[video_id] / max(cumulative_views[video_id], 1)
        for video_id in cumulative_views
    }
    views_percentiles = _percentile_ranks(views_values)
    watch_percentiles = _percentile_ranks(watch_rates)
    like_percentiles = _percentile_ranks(like_rates)
    subscriber_percentiles = _percentile_ranks(subscriber_rates)
    organic_percentiles = _percentile_ranks(
        {video_id: 1.0 - ratio for video_id, ratio in promotion_ratios.items()}
    )

    for item in catalog:
        video_id = str(item["video_id"])
        engagement = (
            0.40 * watch_percentiles[video_id]
            + 0.40 * like_percentiles[video_id]
            + 0.20 * subscriber_percentiles[video_id]
        )
        evergreen = (
            0.50 * watch_percentiles[video_id]
            + 0.30 * views_percentiles[video_id]
            + 0.20 * organic_percentiles[video_id]
        )
        subscriber_magnet = (
            0.60 * subscriber_percentiles[video_id]
            + 0.20 * organic_percentiles[video_id]
            + 0.20 * watch_percentiles[video_id]
        )
        hidden_gem = 0.60 * engagement + 0.40 * (100.0 - views_percentiles[video_id])
        overall = (
            0.40 * engagement
            + 0.30 * evergreen
            + 0.20 * subscriber_magnet
            + 0.10 * views_percentiles[video_id]
        )
        tier = _classify_ci_tier(
            round(overall, 1),
            views_percentiles[video_id],
            round(subscriber_magnet, 1),
            round(hidden_gem, 1),
        )
        score_rows.append(
            (
                DEMO_AS_OF.isoformat(),
                DEMO_CHANNEL_KEY,
                video_id,
                tier,
                round(engagement, 1),
                round(evergreen, 1),
                round(subscriber_magnet, 1),
                round(hidden_gem, 1),
                round(overall, 1),
                cumulative_views[video_id],
                round(watch_rates[video_id], 1),
                round(like_rates[video_id], 4),
                round(subscriber_rates[video_id], 5),
                round(promotion_ratios[video_id], 4),
            )
        )

    recommended = sorted(score_rows, key=lambda row: (-float(row[8]), str(row[2])))[:8]
    published_by_id = {
        str(item["video_id"]): item["published"] for item in catalog
    }
    recommendation_rows: list[tuple] = []
    for rank, score in enumerate(recommended, start=1):
        published = published_by_id[str(score[2])]
        recommendation_rows.append(
            (
                DEMO_CHANNEL_KEY,
                score[2],
                f"{(published - timedelta(days=2)).isoformat()}T12:00:00Z",
                published.isoformat(),
                rank,
                round(10.0 - rank * 0.6, 1),
                "Reliable AI systems",
                "A fictional engineering briefing made this release timely.",
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
                "community_post" if index % 2 else "quote_card",
                f"A closer look at {title_by_id[video_id]}",
                (
                    "Draft a practical discussion prompt about the engineering "
                    "tradeoffs in this fictional lesson."
                    if index % 2
                    else "A concise fictional insight about building reliable AI systems."
                ),
                f"{DEMO_AS_OF.isoformat()}T19:{index:02d}:00Z",
                "draft",
                None,
                None,
                "Synthetic public-demo asset; not approved for publishing.",
            )
        )

    queue_titles = (
        "An Approval Gate for High-Impact Agent Actions",
        "Evaluating Long-Context Retrieval in Production",
        "A Field Guide to Model Trace Sampling",
        "Secure Connectors for Internal Knowledge Systems",
        "Designing a Retrieval Incident Review",
        "When an Automated Evaluator Drifts",
    )
    ranked_candidates = [
        {
            "rank": index,
            "video_id": f"aeg_queue_{index:03d}",
            "title": title,
            "theme": (
                "Reliable agent operations"
                if index % 2
                else "Evaluation and retrieval quality"
            ),
            "relevance_score": round(10.0 - index * 0.7, 1),
            "why_now": "A fictional demo news prompt makes this draft timely.",
            "scheduled_at": (
                DEMO_AS_OF + timedelta(days=10 + index * 3)
            ).isoformat(),
        }
        for index, title in enumerate(queue_titles, start=1)
    ]
    news_headlines = [
        {
            "title": "Fictional briefing: teams standardize agent approval gates",
            "source": "Demo Engineering Wire",
            "published_at": f"{DEMO_AS_OF.isoformat()}T14:00:00Z",
        },
        {
            "title": "Fictional briefing: retrieval evaluations move into release checks",
            "source": "Synthetic Systems Journal",
            "published_at": f"{DEMO_AS_OF.isoformat()}T12:30:00Z",
        },
        {
            "title": "Fictional briefing: observability practices focus on trace quality",
            "source": "Demo Engineering Wire",
            "published_at": f"{DEMO_AS_OF.isoformat()}T11:00:00Z",
        },
    ]
    queue_payload = json.dumps(
        {
            "news_available": True,
            "ranked_videos": ranked_candidates,
            "news_headlines": news_headlines,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    publishing_rows = [
        (
            f"{DEMO_AS_OF.isoformat()}T18:00:00Z",
            DEMO_CHANNEL_KEY,
            len(ranked_candidates),
            len(news_headlines),
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
        expected_dates = [
            (DEMO_AS_OF - timedelta(days=offset)).isoformat()
            for offset in reversed(range(184))
        ]
        actual_dates = [
            row[0]
            for row in conn.execute(
                "SELECT metric_date FROM daily_channel_metrics "
                "WHERE channel=? ORDER BY metric_date",
                (DEMO_CHANNEL_KEY,),
            )
        ]
        if actual_dates != expected_dates:
            errors.append("daily channel date coverage mismatch")

        video_count = conn.execute(
            "SELECT COUNT(*) FROM videos WHERE channel=?", (DEMO_CHANNEL_KEY,)
        ).fetchone()[0]
        if video_count < 48:
            errors.append("insufficient video population")
        playlist_ids = {
            row[0]
            for row in conn.execute(
                "SELECT playlist_id FROM playlists WHERE channel=?", (DEMO_CHANNEL_KEY,)
            )
        }
        if playlist_ids != set(PLAYLISTS):
            errors.append("playlist population mismatch")

        expected_countries = {country for country, _ in _GEOGRAPHIES}
        geo_by_date: defaultdict[str, set[str]] = defaultdict(set)
        for metric_date, country in conn.execute(
            "SELECT metric_date, country_code FROM daily_geo_metrics "
            "WHERE channel=? ORDER BY metric_date, country_code",
            (DEMO_CHANNEL_KEY,),
        ):
            geo_by_date[metric_date].add(country)
        if any(geo_by_date[metric_date] != expected_countries for metric_date in expected_dates):
            errors.append("geography coverage mismatch")

        traffic_by_date: defaultdict[str, set[str]] = defaultdict(set)
        for metric_date, source in conn.execute(
            "SELECT metric_date, traffic_source_type FROM channel_traffic_sources "
            "WHERE channel=? ORDER BY metric_date, traffic_source_type",
            (DEMO_CHANNEL_KEY,),
        ):
            traffic_by_date[metric_date].add(source)
        if any(
            traffic_by_date[metric_date] != _REQUIRED_TRAFFIC_SOURCES
            for metric_date in expected_dates
        ):
            errors.append("traffic source coverage mismatch")

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

        channel_snapshot_dates = [
            row[0]
            for row in conn.execute(
                "SELECT date(captured_at) FROM channel_snapshots "
                "WHERE channel=? ORDER BY captured_at, id",
                (DEMO_CHANNEL_KEY,),
            )
        ]
        if channel_snapshot_dates != expected_dates:
            errors.append("channel snapshot coverage mismatch")

        channel_cumulative_mismatches = conn.execute(
            "SELECT COUNT(*) FROM channel_snapshots s WHERE "
            "COALESCE(s.view_count, -1) != COALESCE(("
            "SELECT SUM(d.views) FROM daily_channel_metrics d "
            "WHERE d.channel=s.channel AND d.metric_date <= date(s.captured_at)), 0) "
            "OR COALESCE(s.subscriber_count, -1) != COALESCE(("
            "SELECT SUM(d.subscribers_gained - d.subscribers_lost) "
            "FROM daily_channel_metrics d WHERE d.channel=s.channel "
            "AND d.metric_date <= date(s.captured_at)), 0)"
        ).fetchone()[0]
        if channel_cumulative_mismatches:
            errors.append("channel snapshot cumulative mismatch")

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

        scheduled_snapshot_dates = [
            metric_date
            for index, metric_date in enumerate(expected_dates)
            if index % 7 == 0 or metric_date == DEMO_AS_OF.isoformat()
        ]
        published_videos = conn.execute(
            "SELECT video_id, date(published_at) FROM videos "
            "WHERE channel=? ORDER BY video_id",
            (DEMO_CHANNEL_KEY,),
        ).fetchall()
        expected_video_snapshots = sorted(
            (snapshot_date, video_id)
            for video_id, published_date in published_videos
            for snapshot_date in scheduled_snapshot_dates
            if published_date <= snapshot_date
        )
        actual_video_snapshots = conn.execute(
            "SELECT date(captured_at), video_id FROM video_snapshots "
            "WHERE channel=? ORDER BY date(captured_at), video_id, id",
            (DEMO_CHANNEL_KEY,),
        ).fetchall()
        if actual_video_snapshots != expected_video_snapshots:
            errors.append("video snapshot coverage mismatch")

        video_cumulative_mismatches = conn.execute(
            "SELECT COUNT(*) FROM video_snapshots s WHERE "
            "COALESCE(s.view_count, -1) != COALESCE(("
            "SELECT SUM(d.views) FROM daily_video_metrics d "
            "WHERE d.channel=s.channel AND d.video_id=s.video_id "
            "AND d.metric_date <= date(s.captured_at)), 0) "
            "OR COALESCE(s.like_count, -1) != COALESCE(("
            "SELECT SUM(d.likes) FROM daily_video_metrics d "
            "WHERE d.channel=s.channel AND d.video_id=s.video_id "
            "AND d.metric_date <= date(s.captured_at)), 0)"
        ).fetchone()[0]
        if video_cumulative_mismatches:
            errors.append("video snapshot cumulative mismatch")

        daily_reconciliation_mismatches = conn.execute(
            "SELECT COUNT(*) FROM daily_channel_metrics c LEFT JOIN ("
            "SELECT channel, metric_date, SUM(views) views, "
            "SUM(estimated_minutes_watched) minutes, "
            "SUM(subscribers_gained) subscribers FROM daily_video_metrics "
            "GROUP BY channel, metric_date) v "
            "ON v.channel=c.channel AND v.metric_date=c.metric_date "
            "WHERE c.views != COALESCE(v.views, 0) "
            "OR ABS(c.estimated_minutes_watched - COALESCE(v.minutes, 0)) > 0.05 "
            "OR c.subscribers_gained != COALESCE(v.subscribers, 0)"
        ).fetchone()[0]
        if daily_reconciliation_mismatches:
            errors.append("daily channel/video reconciliation mismatch")

        geo_reconciliation_mismatches = conn.execute(
            "SELECT COUNT(*) FROM daily_channel_metrics c LEFT JOIN ("
            "SELECT channel, metric_date, SUM(views) views, "
            "SUM(subscribers_gained) subscribers, SUM(likes) likes "
            "FROM daily_geo_metrics GROUP BY channel, metric_date) g "
            "ON g.channel=c.channel AND g.metric_date=c.metric_date LEFT JOIN ("
            "SELECT channel, metric_date, SUM(likes) likes FROM daily_video_metrics "
            "GROUP BY channel, metric_date) v "
            "ON v.channel=c.channel AND v.metric_date=c.metric_date "
            "WHERE c.views != COALESCE(g.views, -1) "
            "OR c.subscribers_gained != COALESCE(g.subscribers, -1) "
            "OR COALESCE(v.likes, 0) != COALESCE(g.likes, -1)"
        ).fetchone()[0]
        if geo_reconciliation_mismatches:
            errors.append("geography reconciliation mismatch")

        video_traffic_mismatches = conn.execute(
            "SELECT COUNT(*) FROM daily_video_metrics d LEFT JOIN ("
            "SELECT channel, metric_date, video_id, SUM(views) views, "
            "SUM(estimated_minutes_watched) minutes "
            "FROM video_traffic_source_metrics "
            "GROUP BY channel, metric_date, video_id) t "
            "ON t.channel=d.channel AND t.metric_date=d.metric_date "
            "AND t.video_id=d.video_id WHERE d.views != COALESCE(t.views, -1) "
            "OR ABS(d.estimated_minutes_watched - COALESCE(t.minutes, -1)) > 0.05"
        ).fetchone()[0]
        if video_traffic_mismatches:
            errors.append("video traffic reconciliation mismatch")

        channel_traffic_mismatches = conn.execute(
            "SELECT COUNT(*) FROM channel_traffic_sources c LEFT JOIN ("
            "SELECT channel, metric_date, traffic_source_type, SUM(views) views, "
            "SUM(estimated_minutes_watched) minutes "
            "FROM video_traffic_source_metrics "
            "GROUP BY channel, metric_date, traffic_source_type) v "
            "ON v.channel=c.channel AND v.metric_date=c.metric_date "
            "AND v.traffic_source_type=c.traffic_source_type "
            "WHERE c.views != COALESCE(v.views, 0) "
            "OR ABS(c.estimated_minutes_watched - COALESCE(v.minutes, 0)) > 0.05"
        ).fetchone()[0]
        if channel_traffic_mismatches:
            errors.append("channel traffic reconciliation mismatch")

        promoted_video_count = conn.execute(
            "SELECT COUNT(DISTINCT video_id) FROM video_traffic_source_metrics "
            "WHERE channel=? AND traffic_source_type='ADVERTISING'",
            (DEMO_CHANNEL_KEY,),
        ).fetchone()[0]
        if not 0 < promoted_video_count < video_count:
            errors.append("advertising subset mismatch")
        promoted_ad_share = conn.execute(
            "SELECT 100.0 * SUM(CASE WHEN traffic_source_type='ADVERTISING' "
            "THEN views ELSE 0 END) / NULLIF(SUM(views), 0) "
            "FROM video_traffic_source_metrics WHERE channel=? AND video_id IN ("
            "SELECT DISTINCT video_id FROM video_traffic_source_metrics "
            "WHERE channel=? AND traffic_source_type='ADVERTISING')",
            (DEMO_CHANNEL_KEY, DEMO_CHANNEL_KEY),
        ).fetchone()[0]
        if promoted_ad_share is None or not 4.5 <= promoted_ad_share <= 5.5:
            errors.append("advertising share mismatch")

        invalid_retention = conn.execute(
            "SELECT COUNT(*) FROM retention_buckets "
            "WHERE retention_at_25 < 0 OR retention_at_25 > 1 "
            "OR retention_at_75 < 0 OR retention_at_75 > retention_at_25"
        ).fetchone()[0]
        if invalid_retention:
            errors.append(f"invalid retention bounds: {invalid_retention}")

        retention_by_video: defaultdict[str, dict[str, tuple[str, str]]] = defaultdict(dict)
        for video_id, start, end, kind in conn.execute(
            "SELECT video_id, window_start, window_end, window_kind "
            "FROM retention_buckets WHERE channel=?",
            (DEMO_CHANNEL_KEY,),
        ):
            retention_by_video[video_id][kind] = (start, end)
        required_windows = {
            kind: (
                (DEMO_AS_OF - timedelta(days=window_days)).isoformat(),
                DEMO_AS_OF.isoformat(),
            )
            for window_days, kind in _RETENTION_WINDOWS
        }
        video_ids = {
            row[0]
            for row in conn.execute(
                "SELECT video_id FROM videos WHERE channel=?", (DEMO_CHANNEL_KEY,)
            )
        }
        if any(retention_by_video[video_id] != required_windows for video_id in video_ids):
            errors.append("retention window coverage mismatch")

        retention_view_mismatches = conn.execute(
            "SELECT COUNT(*) FROM retention_buckets r WHERE r.views != ("
            "SELECT COALESCE(SUM(d.views), 0) FROM daily_video_metrics d "
            "WHERE d.channel=r.channel AND d.video_id=r.video_id "
            "AND d.metric_date BETWEEN r.window_start AND r.window_end)"
        ).fetchone()[0]
        if retention_view_mismatches:
            errors.append("retention view reconciliation mismatch")

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

        queue_rows = conn.execute(
            "SELECT videos_analyzed, news_stories_count, result_json "
            "FROM publishing_queue WHERE channel=?",
            (DEMO_CHANNEL_KEY,),
        ).fetchall()
        queue_valid = len(queue_rows) == 1
        if queue_valid:
            videos_analyzed, news_count, payload_text = queue_rows[0]
            try:
                payload = json.loads(payload_text)
                queue_valid = isinstance(payload, dict)
                ranked = payload.get("ranked_videos") if queue_valid else None
                headlines = payload.get("news_headlines") if queue_valid else None
                ranked_are_objects = isinstance(ranked, list) and all(
                    isinstance(item, dict) for item in ranked
                )
                headlines_are_objects = isinstance(headlines, list) and all(
                    isinstance(headline, dict) for headline in headlines
                )
                queue_valid = (
                    queue_valid
                    and payload.get("news_available") is True
                    and ranked_are_objects
                    and headlines_are_objects
                    and videos_analyzed == len(ranked) > 0
                    and news_count == len(headlines) > 0
                )
                if queue_valid:
                    ranked_ids = [item["video_id"] for item in ranked]
                    queue_valid = (
                        len(ranked_ids) == len(set(ranked_ids))
                        and set(ranked_ids).isdisjoint(video_ids)
                        and [item["rank"] for item in ranked]
                        == list(range(1, len(ranked) + 1))
                        and all(
                            math.isfinite(float(item["relevance_score"]))
                            and 0 <= float(item["relevance_score"]) <= 10
                            and item.get("title")
                            and item.get("theme")
                            and item.get("why_now")
                            and item.get("scheduled_at")
                            for item in ranked
                        )
                        and all(
                            headline.get("title")
                            and headline.get("source")
                            and headline.get("published_at")
                            for headline in headlines
                        )
                    )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                queue_valid = False
        if not queue_valid:
            errors.append("publishing queue payload mismatch")

        recommendation_mismatches = conn.execute(
            "SELECT COUNT(*) FROM queue_recommendations q JOIN videos v "
            "ON v.channel=q.channel AND v.video_id=q.video_id "
            "WHERE (julianday(v.published_at) - "
            "julianday(q.recommended_publish_date)) * 24 NOT BETWEEN -72 AND 96 "
            "OR datetime(q.first_recommended_at) > datetime(q.recommended_publish_date) "
            "OR q.rank_at_recommendation < 1 "
            "OR q.relevance_score < 0 OR q.relevance_score > 10"
        ).fetchone()[0]
        recommendation_count = conn.execute(
            "SELECT COUNT(*) FROM queue_recommendations WHERE channel=?",
            (DEMO_CHANNEL_KEY,),
        ).fetchone()[0]
        if recommendation_mismatches or recommendation_count == 0:
            errors.append("historical recommendation mismatch")

        score_rows = conn.execute(
            "SELECT video_id, tier, engagement_score, evergreen_score, "
            "subscriber_magnet_score, hidden_gem_score, overall_score, total_views, "
            "watch_rate_pct, like_rate_pct, sub_rate_pct, promotion_ratio "
            "FROM ci_video_scores "
            "WHERE channel=? AND scored_at=?",
            (DEMO_CHANNEL_KEY, DEMO_AS_OF.isoformat()),
        ).fetchall()
        tiers = {row[1] for row in score_rows}
        numeric_values_valid = all(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
            for row in score_rows
            for value in row[2:]
        )
        if not numeric_values_valid:
            errors.append("CI numeric values mismatch")
        coherent_tiers = False
        if numeric_values_valid:
            score_views = {row[0]: float(row[7]) for row in score_rows}
            score_view_percentiles = _percentile_ranks(score_views) if score_views else {}
            coherent_tiers = all(
                row[1]
                == _classify_ci_tier(
                    float(row[6]),
                    score_view_percentiles[row[0]],
                    float(row[4]),
                    float(row[5]),
                )
                for row in score_rows
            )
        if (
            len(score_rows) != video_count
            or tiers != _CANONICAL_CI_TIERS
            or not coherent_tiers
        ):
            errors.append("CI tier coverage mismatch")

        asset_rows = conn.execute(
            "SELECT asset_type, status FROM ci_content_assets WHERE channel=?",
            (DEMO_CHANNEL_KEY,),
        ).fetchall()
        asset_types = {row[0] for row in asset_rows}
        statuses = {row[1] for row in asset_rows}
        if (
            not {"community_post", "quote_card"} <= asset_types
            or not asset_types <= _VALID_ASSET_TYPES
            or statuses != {"draft"}
        ):
            errors.append("CI asset coverage mismatch")
    return errors


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEMO_DB_PATH)
    parser.add_argument("--seed", type=int, default=8142026)
    return parser.parse_args()


if __name__ == "__main__":
    arguments = _parse_args()
    build_demo_database(arguments.output, seed=arguments.seed)
