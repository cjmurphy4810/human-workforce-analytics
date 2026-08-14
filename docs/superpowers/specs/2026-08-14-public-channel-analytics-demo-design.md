# Public Channel Analytics Demo Design

**Date:** 2026-08-14  
**Status:** Approved direction; awaiting written-spec review

## Purpose

Create a public, password-free demonstration of the existing YouTube analytics product for consulting prospects. The demo must reproduce the current dashboard experience using a fictional channel named **AI Engineering Genius**, while preventing disclosure of The Human Workforce's identity, content library, private analytics, credentials, or reconstructable source data.

The demo is a sales experience, not a live analytics account. It should let a prospect explore the same reports and controls they could receive as a consulting client.

## Product Boundary

The demo will be a separate Streamlit application and deployment derived from the existing `human-workforce-analytics` codebase. It will not be a runtime flag in the production application.

The public deployment will:

- require no password;
- contain no YouTube, Google Ads, OAuth, AI-provider, or production database credentials;
- make no calls to live analytics services or scheduled data-refresh jobs;
- use only a bundled synthetic database;
- identify all displayed information as simulated demo data; and
- preserve the current dashboard's useful interactions and reports.

Production data and demo data must never share a database file, deployment secret, fetch process, or writable storage path.

## Demo Channel Experience

The active channel is **AI Engineering Genius**. It behaves as the fully configured example account across every report page.

The channel area also shows three fictional prospects or portfolio channels:

- Automation Architects
- Future Systems Lab
- Practical AI Studio

These additional channel controls are visibly disabled and cannot change application state. Supporting text explains that additional channels can be configured through the consulting service. The disabled controls must use native disabled semantics where Streamlit supports them, so they are inaccessible to keyboard and pointer activation rather than merely appearing muted.

All branding, page titles, metadata, and visible copy must avoid “The Human Workforce,” its abbreviations, and the names of existing real channels.

## Synthetic Data Strategy

The demo database will be generated offline from schema-aware synthetic fixtures. It may use the production dataset only as an aggregate statistical reference during local generation; no production row, stable identifier, title, description, thumbnail URL, playlist name, recommendation text, or exact metric series will be copied into the deliverable.

The generator will create at least six months of coherent history for AI Engineering Genius. Synthetic values will preserve realistic relationships rather than exact source values, including:

- cumulative views and subscribers increasing consistently with daily activity;
- video and channel totals reconciling within documented reporting-window differences;
- watch time corresponding plausibly to views and average view duration;
- subscriber gains, engagement, retention, traffic sources, and geography varying by video and over time;
- organic and promoted performance supporting meaningful momentum and promotion classifications;
- qualifying watch hours excluding synthetic advertising traffic;
- playlist membership and totals matching synthetic videos;
- content-intelligence scores, recommendations, and draft assets referencing only fictional content; and
- publishing-queue recommendations using fictional episodes and fictional news-style prompts.

Dates will be generated relative to a fixed demo snapshot date so the public experience is deterministic and tests do not decay over time. The UI will label that snapshot as simulated. Generated values should be intentionally transformed and independently sampled, not produced by applying one reversible multiplier to private values.

## Fictional Content System

The dataset will include a coherent catalog of fictional AI-engineering content. Titles and playlists should sound credible while remaining invented. Example topic families include agent reliability, evaluation systems, retrieval architecture, AI observability, secure automation, model routing, and production deployment.

Every synthetic video receives a generated demo ID. Thumbnail fields will use local generic demo artwork or intentionally omit artwork where the current component already handles missing thumbnails. No live YouTube URL or real video ID will be included.

Recommendations and explanations must be newly written from synthetic attributes. They must not paraphrase distinctive private episode titles or proprietary publishing recommendations closely enough to identify their source.

## Page and Interaction Coverage

The demo will retain the current product's report set:

1. Overview
2. Daily Analytics
3. Qualifying Watch Hours
4. Organic Momentum
5. Promotion Intelligence
6. Content Intelligence
7. Video Render Comparisons

Existing controls—including date ranges, tabs, filters, video selection, projections, tables, chart toolbars, and ROI inputs—remain interactive wherever their underlying production counterpart is interactive. The AI Engineering Genius selection persists across multipage navigation.

Pages may receive small demo-specific explanatory copy, but calculations and report behavior should continue to use the existing analytics modules wherever possible. This keeps the demonstration faithful to the product being offered and avoids maintaining a separate mock dashboard.

## Sales Presentation Layer

A compact banner near the top of every page will state that AI Engineering Genius is a simulated channel with synthetic data. It should reassure visitors that the dashboard demonstrates the deliverable without implying that the results are typical or guaranteed.

The sidebar will include a concise consulting callout explaining that clients can receive a configured analytics workspace for their own YouTube channel. The first version will use informational copy only; no contact form, lead capture, or external transmission is introduced by this project.

The visual design should remain recognizably connected to the existing tool while improving public-demo clarity. It must not introduce production-only controls such as authentication, credential setup, refresh status, or instructions to run fetch scripts.

## Architecture

The implementation will isolate demo concerns into three layers:

### Demo application entry points

A demo app directory or deployment package will contain the public Streamlit entry point and demo page wrappers. It will import shared calculation and rendering code from the existing project where doing so does not create a production-data dependency.

### Demo configuration

A single configuration module will own the fictional channel registry, active channel, demo labels, fixed snapshot date, database location, and disabled channel names. Pages must not scatter hard-coded production or demo channel names.

### Synthetic fixture generation

An offline generator will create the demo SQLite database from a deterministic random seed. Generation is a development/build step, never a public runtime operation. The checked-in or deployment-bundled database is the only analytics source for the public app.

If existing pages are too tightly coupled to production configuration, the shared boundary should be improved narrowly by dependency injection or configuration parameters. Unrelated refactoring is out of scope.

## Data Flow

1. The public Streamlit entry point loads demo configuration.
2. The active channel resolves to the fixed `ai_engineering_genius` key.
3. Each page queries the bundled demo database with that channel key.
4. Existing analytics modules calculate report models from synthetic rows.
5. Streamlit renders charts, tables, filters, and explanations.
6. Disabled fictional channels remain presentation-only and never affect query state.

There is no inbound production-data path and no outbound analytics, form-submission, or account-connection path.

## Failure Handling

- A missing or unreadable demo database produces a clear public-friendly maintenance message without instructions that expose internal filenames or production commands.
- Empty datasets produce intentional demo empty states rather than production refresh instructions.
- Synthetic generation validates referential integrity and fails before replacing a previously valid fixture.
- Pages must not silently fall back to the production database or default production channel.
- Any detected production channel name, real video ID, remote thumbnail URL, or credential-like configuration causes the privacy validation test to fail.

## Testing and Privacy Verification

Automated tests will cover:

- deterministic fixture generation from a fixed seed;
- at least six months of populated history;
- schema and foreign-key consistency across synthetic tables;
- reconciliation of key cumulative and daily metrics;
- functioning calculations for every report page;
- the active AI Engineering Genius channel on all pages;
- disabled semantics for the three additional channel controls;
- absence of authentication in the demo entry point;
- absence of live-service calls and production secrets;
- absence of production channel names, known real IDs, real titles, and remote thumbnail URLs in demo source and data; and
- Streamlit smoke tests for all seven report routes.

Visual verification will run the app locally, inspect every page at a standard desktop viewport, exercise representative filters and tabs, and confirm that charts contain useful non-empty data. The final pass will also inspect rendered copy for real-brand leakage and verify that the demo notice remains visible.

## Deployment

The demo will have its own Streamlit Community Cloud app configuration and public URL. Deployment configuration will point only to the demo entry point and synthetic fixture. The production app and its password remain unchanged.

Deployment itself is a separate externally visible action and will occur only when explicitly authorized after local implementation and verification.

## Success Criteria

The design is successful when a prospect can open the public demo without credentials, explore the same analytics capabilities as the current application, understand that the data is simulated, see AI Engineering Genius as the configured channel, recognize unavailable fictional channels as examples of expandability, and encounter no identifiable Human Workforce content or connection to live accounts.

## Out of Scope

- Live onboarding or OAuth connection for prospects
- Editable channel creation
- Functional switching to the three additional fictional channels
- Lead-capture forms, CRM integrations, billing, or subscriptions
- Changes to the production dashboard's password or data pipeline
- Claims or projections about guaranteed client performance
