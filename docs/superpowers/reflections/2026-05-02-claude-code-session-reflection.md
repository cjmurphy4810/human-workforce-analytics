# Building & Shipping a YouTube Analytics Feature with Claude Code

**Session date:** 2026-05-02
**Project:** Human Workforce Analytics — a Streamlit dashboard backed by the YouTube Analytics API
**Goal of session:** Add audience-retention bucket charts (0–25% / 25–75% / 75–100% drop-off splits) to the dashboard, both channel-wide and per-video, and ship to the public Streamlit Cloud deployment

---

## At a Glance

- **Total elapsed time:** ~2 hours 30 minutes (≈09:30 → 12:00 PT)
- **Total active build time (first task to last commit):** ~2 hours 7 minutes
- **Pull requests shipped:** 2 (feature + bug fix)
- **Files changed:** 13 created/modified
- **Tests written:** 19 (all passing)
- **Live deploys:** 2 (initial feature + bug fix)
- **YouTube videos analyzed:** 75
- **API calls in production fetch:** ~225 retention + ~225 view-count = ~450 per scheduled run
- **Bugs caught and fixed in-session:** 1 (~5× view over-count)

---

## Session Timeline

| Time (PT) | Phase | What Happened |
|---|---|---|
| 09:30 | Framing | I asked a meta-question ("can Claude work on multiple windows at once?") and got a clear, useful answer |
| 09:35 | Feature request | Stated the need: retention buckets across all videos plus per-video |
| 09:40–09:53 | Brainstorming | Claude asked 3 targeted questions (data type, bucket framing, chart format), each with explicit tradeoffs and a recommendation |
| 09:53 | Spec written | Design spec committed to `docs/superpowers/specs/` |
| 10:07 | Plan written | 12-task TDD plan committed — every step had real code, exact commands, and expected output |
| 10:14–10:25 | TDD build (Tasks 1–5) | Test infrastructure → DB migration → API fetcher → aggregation module → fetch integration. Each task: write failing test, watch it fail, implement, watch it pass, commit. ~11 minutes for 5 tasks |
| 10:25–10:45 | Auth blocker | OAuth refresh token had expired. Claude diagnosed it from the error trace, told me what to do, found my `client_secret.json` in `~/Downloads`, ran the live fetch after I refreshed the token |
| 10:45–11:07 | UI build (Tasks 6–7) | Channel-wide retention section + per-video block. Streamlit blank page solved (was a missing local secrets file). Saw the data, gave feedback ("add percentages to headers, mirror in deep dive"), Claude refactored into a single helper function and re-rendered |
| 11:14–11:16 | Ship to production | Resolved a `data.db` merge conflict, merged PR #1 (squash), Streamlit Cloud auto-deployed |
| 11:17–11:21 | Operations | Updated the GitHub Actions secret via `gh` CLI (token piped, never echoed), triggered a manual fetch, waited for green |
| 11:25–11:48 | Bug discovery | I noticed dashboard showed ~400K views vs. YouTube Studio's ~100K. Claude queried the DB, confirmed a ~5× over-count, traced the cause (summing trailing-90-day snapshots across multiple fetch dates) |
| 11:48–12:00 | Bug fix shipped | New `fetch_video_views_in_window` API call replaces the broken SUM. PR #2 → merged → deployed |

---

## Interaction Types Observed

**1. Question-and-answer with tradeoffs (~10 min total)**
Three clarifying questions during brainstorming, each presenting 2–3 options with a clear recommendation. Average answer time on my side: well under a minute. The questions weren't abstract — they were "this drives the data-fetching strategy, choose now."

**2. Specification + plan generation (~25 min)**
Two long-form documents written and committed to git: the spec (architecture + tradeoffs), then the plan (12 bite-sized TDD tasks with code, commands, expected output). Each was self-reviewed by Claude before I saw it.

**3. TDD implementation cycles (~20 min for 5 tasks)**
Tight loop: red test → minimal code → green test → commit. Each cycle was 2–4 minutes. I didn't write any code; my job was to read diffs and verify the data was sane.

**4. Operational / blocker handling (~25 min total across the session)**
Auth expiry, missing local secrets, two `data.db` merge conflicts, GitHub Actions secret rotation, Streamlit Cloud redeploys. Claude diagnosed each from logs/output and either fixed it directly or told me the one specific action I needed to take (e.g., "find your `client_secret.json`").

**5. UI feedback iteration (~10 min)**
I previewed the dashboard, said "perfect, but add percentages to headers and mirror in deep dive." Claude refactored 60 lines into a single helper function, fixed a misleading green up-arrow I hadn't even mentioned (Streamlit was treating the percentage as a positive delta), and shipped the update.

**6. Bug investigation & fix (~15 min)**
The view-count discrepancy is the most interesting moment of the session. Claude queried the DB, computed the actual over-count factor, explained *why* the existing schema caused it, proposed a fix, implemented and tested it, ran a fresh fetch, confirmed the corrected numbers matched YouTube Studio, opened a PR, merged it.

---

## Notable Moments (Good and Friction)

**What worked surprisingly well:**

- **The cost of a question is ~30 seconds; the cost of a wrong assumption is hours.** Claude consistently asked first. The "absolute minutes vs. relative percent" question alone could've been a wasted afternoon.
- **TDD cadence with real code per step.** No "// TODO implement" placeholders. Each plan step had the actual test code, the actual implementation, the exact command to run. The plan was executable.
- **Self-reviewing.** Claude self-reviewed the spec before showing it to me and caught its own bug (window-mapping inconsistency for "Last month"). It self-reviewed the plan and caught a primary-key collision risk.
- **Reading logs and acting on them.** Token-expired error → "your refresh token is dead, here's the script to regenerate it." `data.db` merge conflict → resolved with a `git checkout --ours`. Bug in production → SQL query to confirm, then fix.

**Friction points worth naming:**

- **Auth state is opaque to the AI.** Claude can't see my shell environment or my password manager. When the OAuth token expired, it had to wait for me to run the browser-based refresh flow. The handoff worked but it was the slowest part of the session.
- **Binary file conflicts.** `data.db` is a SQLite file committed to git, and it conflicted twice (because automated fetches were running on `main` while we worked on a branch). Claude resolved cleanly each time, but binary conflicts always require human judgment about which version is canonical.
- **Streamlit blank page.** Local Streamlit ran but the page was empty — turned out to be a missing `secrets.toml`. Claude fixed it in seconds once I shared the screenshot, but the error message itself was buried inside the password gate logic.
- **The view-count bug.** I caught it by eyeballing the public dashboard against YouTube Studio. Claude wrote unit tests for the *math* but didn't write a sanity-check test for the *magnitude*. A "does this number match reality?" test wasn't in the spec.

---

## Reflection for the Podcast

If I had to summarize the experience in three points for a listener:

1. **The collaboration shape changed.** I wasn't writing code; I was setting direction, reviewing diffs, eyeballing real data, and catching the one thing the AI couldn't catch (the magnitude bug). My role was much closer to *technical product manager + QA* than *engineer*.

2. **The expensive parts were authentication and operational state.** Code generation, test writing, even debugging — fast. The slow parts were OAuth refreshes, GitHub Actions secret rotation, and merge conflicts on a binary file. None of those are AI-hard problems; they're "the world has state and the AI can't see all of it" problems.

3. **The audit trail is the deliverable.** What we have now isn't just the feature — it's a spec, a plan, 19 tests, two PRs, and this reflection. If a teammate joins next week, they can read the spec, run the tests, and understand both *what* exists and *why* it was built that way. That's the part that makes this style of collaboration reproducible and reviewable, instead of magic.

The dashboard is live, the numbers match YouTube Studio, and the work that remains (Phase 2 trend charts) is documented in the same plan, ready to be picked up next session.

---

## Artifacts

- Spec: `docs/superpowers/specs/2026-05-02-audience-retention-buckets-design.md`
- Plan: `docs/superpowers/plans/2026-05-02-audience-retention-buckets.md`
- PR #1 (feature): https://github.com/cjmurphy4810/human-workforce-analytics/pull/1
- PR #2 (bug fix): https://github.com/cjmurphy4810/human-workforce-analytics/pull/2
- Live dashboard: https://human-workforce-analytics-t7y4scc8tntc5ufhoghaw7.streamlit.app
