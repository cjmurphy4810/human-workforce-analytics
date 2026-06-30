"""Generate draft content assets using Claude Haiku."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import anthropic
from anthropic.types import TextBlock

from content_intelligence.models import AssetType, LegacyContentAsset, VideoScore
from content_intelligence.prompts.templates import SYSTEM_PROMPT, TEMPLATES


def generate_asset(
    client: anthropic.Anthropic,
    video: VideoScore,
    asset_type: AssetType,
    channel_name: str = "",
) -> LegacyContentAsset:
    """
    Generate one draft ContentAsset for the given video and asset type.

    channel_name is optional context; passing "" avoids hardcoding channel
    identity in core logic.

    Raises ValueError if the API returns invalid JSON for structured types
    (poll, course_idea). Callers should handle this.
    """
    template = TEMPLATES[asset_type]
    prompt = template.format(
        title=video.title,
        views=video.total_views,
        watch_rate=video.watch_rate_pct,
        like_rate=video.like_rate_pct,
        channel=channel_name,
    )

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": prompt}],
    )

    block = response.content[0]
    if not isinstance(block, TextBlock):
        raise ValueError(f"Unexpected response block type: {type(block)}")
    raw = block.text.strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        raw = "\n".join(inner).strip()

    if asset_type in ("poll", "course_idea"):
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1:
            raw = raw[start : end + 1]
        json.loads(raw)  # validate; raises ValueError on bad JSON

    return LegacyContentAsset(
        asset_id=uuid.uuid4().hex,
        video_id=video.video_id,
        video_title=video.title,
        asset_type=asset_type,
        title=_make_title(video.title, asset_type),
        body=raw,
        generated_at=datetime.now(timezone.utc).isoformat(),
        status="draft",
    )


def _make_title(video_title: str, asset_type: AssetType) -> str:
    short = video_title[:40] + ("…" if len(video_title) > 40 else "")
    labels: dict[str, str] = {
        "community_post": "Community Post",
        "poll": "Poll",
        "quote_card": "Quote Card",
        "short_hook": "Short Hook Script",
        "linkedin_post": "LinkedIn Post",
        "course_idea": "Course Concept",
    }
    return f"{labels[asset_type]}: {short}"
