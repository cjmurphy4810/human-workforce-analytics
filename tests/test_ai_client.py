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
        instance.get_everything.return_value = {
            "articles": [
                {"title": "AI takes jobs", "source": {"name": "BBC"}, "publishedAt": "2026-05-13T10:00:00Z"},
                {"title": "AI takes jobs", "source": {"name": "CNN"}, "publishedAt": "2026-05-13T11:00:00Z"},
            ]
        }
        result = fetch_news_headlines("fake_key")
    titles = [h["title"] for h in result]
    assert titles.count("AI takes jobs") == 1


def test_fetch_news_headlines_caps_at_20():
    with patch("ai_client.NewsApiClient") as MockNews:
        instance = MockNews.return_value
        instance.get_everything.return_value = {
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
        call_count = [0]
        def side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("API error")
            return {"articles": [
                {"title": "Business story", "source": {"name": "Reuters"}, "publishedAt": "2026-05-13T10:00:00Z"}
            ]}
        instance.get_everything.side_effect = side_effect
        result = fetch_news_headlines("fake_key")
    assert any(h["title"] == "Business story" for h in result)


def test_classify_video_themes_uses_prompt_caching():
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text='{"v1": "AI theme"}')]
    )
    videos = [{"video_id": "v1", "title": "T", "description": "D"}]
    classify_video_themes(mock_client, videos)
    call_kwargs = mock_client.messages.create.call_args[1]
    system_block = call_kwargs["system"][0]
    assert system_block.get("cache_control") == {"type": "ephemeral"}
