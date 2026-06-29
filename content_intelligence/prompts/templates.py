"""Prompt templates for content asset generation.

The {channel} placeholder is optional context injected at call time.
Core logic never hardcodes channel identity here.
"""
from __future__ import annotations

SYSTEM_PROMPT = (
    "You are a content strategist for a podcast channel about AI, workforce transformation, "
    "career development, and the future of work. You help repurpose video content into "
    "engaging community and social assets. Be conversational, authentic, and audience-first. "
    "Never sound corporate or generic."
)

TEMPLATES: dict[str, str] = {
    "community_post": (
        "Write a YouTube Community post (100-150 words) based on this video.\n\n"
        "Video title: {title}\n"
        "Key metrics: {views:,} views, {watch_rate:.0f}% average watch rate, {like_rate:.2f}% like rate\n\n"
        "Start with a thought-provoking question or surprising statement related to the video topic.\n"
        "Reference the video naturally. End with one clear call to action.\n"
        "Conversational tone. No hashtags. No emojis unless it flows naturally.\n\n"
        "Return ONLY the post text."
    ),
    "poll": (
        "Create a YouTube Community poll based on this video.\n\n"
        "Video title: {title}\n\n"
        "Write a genuinely thought-provoking question the audience would want to vote on.\n"
        "Provide exactly 4 answer options that are distinct and free of obvious right answers.\n\n"
        'Return ONLY valid JSON: {{"question": "...", "options": ["A", "B", "C", "D"]}}'
    ),
    "quote_card": (
        "Create a shareable quote card for this video.\n\n"
        "Video title: {title}\n\n"
        "Generate a single powerful, stand-alone insight (1-2 sentences, max 30 words) that:\n"
        "- Works without needing video context\n"
        "- Would stop someone scrolling if they saw it on social media\n"
        "- Captures a non-obvious truth about AI, work, or human potential\n\n"
        "Return ONLY the quote text (no attribution, no quotation marks)."
    ),
    "short_hook": (
        "Write a 60-second hook script (~150 words) for a YouTube Short based on this video.\n\n"
        "Video title: {title}\n\n"
        "Structure:\n"
        "1. Open with a bold question or counterintuitive claim (5 sec)\n"
        "2. Challenge the obvious assumption (10 sec)\n"
        "3. Tease the insight from the full video (15 sec)\n"
        "4. Call to action: watch the full video or subscribe (5 sec)\n\n"
        "Conversational, punchy, direct. Write as a spoken script.\n\n"
        "Return ONLY the script text."
    ),
    "linkedin_post": (
        "Write a LinkedIn post (250-300 words) based on this video.\n\n"
        "Video title: {title}\n"
        "Audience: HR professionals, business leaders, knowledge workers navigating AI.\n\n"
        "Structure:\n"
        "- Line 1: Bold, provocative opening claim\n"
        "- Lines 2-4: Brief context or observation\n"
        "- Lines 5-12: 3-5 punchy insights (one per line, heavy use of line breaks)\n"
        "- Final lines: Reflective question to drive comments\n\n"
        "Professional but not stiff. Use line breaks liberally. No bullet-point lists.\n"
        "Add 2-3 hashtags as a separate final block.\n\n"
        "Return ONLY the post text."
    ),
    "course_idea": (
        "Suggest an online course concept based on this video topic.\n\n"
        "Video title: {title}\n\n"
        "Provide:\n"
        "- A compelling course title\n"
        "- 4-5 module titles with one-sentence descriptions\n"
        "- Target audience (one sentence)\n"
        "- Estimated total learning time\n\n"
        "Return ONLY valid JSON:\n"
        "{{\n"
        '  "course_title": "...",\n'
        '  "target_audience": "...",\n'
        '  "estimated_duration": "...",\n'
        '  "modules": [\n'
        '    {{"title": "...", "description": "..."}}\n'
        "  ]\n"
        "}}"
    ),
}
