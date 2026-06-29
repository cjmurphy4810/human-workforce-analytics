"""Tests for content asset generation (mocked Anthropic client)."""
import json
from unittest.mock import MagicMock

import pytest

from content_intelligence.generation.drafts import _make_title, generate_asset
from content_intelligence.models import ContentAsset, VideoScore


def _video_score(**kwargs) -> VideoScore:
    defaults = dict(
        video_id="v1", title="AI and the Future of Work", scored_at="2026-06-29",
        total_views=500, watch_rate_pct=65.0, like_rate_pct=2.5,
        sub_rate_pct=0.4, promotion_ratio=0.3,
        engagement_score=70.0, evergreen_score=60.0,
        subscriber_magnet_score=55.0, hidden_gem_score=45.0,
        overall_score=63.0, tier="top_episode",
    )
    defaults.update(kwargs)
    return VideoScore(**defaults)


def _mock_client(response_text: str) -> MagicMock:
    client = MagicMock()
    client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=response_text)]
    )
    return client


def test_generate_community_post_returns_asset():
    client = _mock_client("Great post text here about the future of work.")
    vs = _video_score()
    asset = generate_asset(client, vs, "community_post")
    assert isinstance(asset, ContentAsset)
    assert asset.asset_type == "community_post"
    assert asset.status == "draft"
    assert asset.video_id == "v1"
    assert asset.body == "Great post text here about the future of work."


def test_generate_poll_validates_json():
    payload = json.dumps({"question": "What changes work?", "options": ["A", "B", "C", "D"]})
    client = _mock_client(payload)
    vs = _video_score()
    asset = generate_asset(client, vs, "poll")
    assert asset.asset_type == "poll"
    parsed = json.loads(asset.body)
    assert "question" in parsed
    assert len(parsed["options"]) == 4


def test_generate_poll_invalid_json_raises():
    client = _mock_client("not valid json")
    vs = _video_score()
    with pytest.raises((ValueError, json.JSONDecodeError)):
        generate_asset(client, vs, "poll")


def test_generate_strips_markdown_fences():
    client = _mock_client("```\nClean text here.\n```")
    vs = _video_score()
    asset = generate_asset(client, vs, "community_post")
    assert "```" not in asset.body
    assert asset.body == "Clean text here."


def test_generate_asset_id_is_unique():
    client = _mock_client("Some text")
    vs = _video_score()
    a1 = generate_asset(client, vs, "quote_card")
    a2 = generate_asset(client, vs, "quote_card")
    assert a1.asset_id != a2.asset_id


def test_generate_uses_prompt_caching():
    client = _mock_client("Text")
    vs = _video_score()
    generate_asset(client, vs, "community_post")
    call_kwargs = client.messages.create.call_args[1]
    system_block = call_kwargs["system"][0]
    assert system_block.get("cache_control") == {"type": "ephemeral"}


def test_make_title_truncates_long_titles():
    long = "A" * 60
    result = _make_title(long, "community_post")
    assert "…" in result
    assert len(result) < len(long) + 30


def test_make_title_short_title_no_ellipsis():
    result = _make_title("Short", "poll")
    assert "…" not in result
    assert "Poll" in result


def test_generate_course_idea_validates_json():
    payload = json.dumps({
        "course_title": "AI at Work",
        "target_audience": "HR leaders",
        "estimated_duration": "3 hours",
        "modules": [{"title": "Intro", "description": "Overview of AI."}],
    })
    client = _mock_client(payload)
    vs = _video_score()
    asset = generate_asset(client, vs, "course_idea")
    parsed = json.loads(asset.body)
    assert "course_title" in parsed
    assert "modules" in parsed
