# Berlin Insider MVP

## 1. Problem

Berlin has many events every weekend, but information is fragmented across websites and channels. Existing newsletters are mostly email-based and not ideal for quick, chat-native updates.

## 2. MVP Goal

Deliver one weekly message on Friday morning with a curated summary of weekend events in Berlin, sent through one instant messaging channel.

Success for MVP:

- A user receives a useful Friday digest every week.
- The digest includes a balanced mix (events, food, exhibitions, nightlife).
- Content is sourced from the sites listed in [sources.md](./sources.md).

## 3. Scope

In scope for MVP:

- One channel only (start with Telegram or WhatsApp; choose easiest to implement first).
- One schedule: every Friday morning (Berlin local time).
- One curated digest format (short highlights + links).
- Source ingestion from selected items in `sources.md`.

Out of scope for MVP:

- Real-time alerts.
- Personalized recommendations per user.
- Mobile app or web frontend.
- Multi-language support.
- Full automation across all sources from day one.

## 4. User Story

As a Berlin resident, I want a single chat message every Friday morning that tells me what is worth doing this weekend, so I can plan quickly without checking many websites.

## 5. MVP User Flow

1. System pulls content from selected Berlin sources.
2. System extracts candidate events/ideas for this weekend.
3. System ranks and selects top items across categories.
4. System generates one concise digest message.
5. System sends message on Friday morning via chosen channel.

## 6. Content Strategy (MVP)

Input sources:

- Use entries from `sources.md`.

Selection rules:

- Prefer events explicitly dated for upcoming weekend.
- Keep category diversity (for example: 2 events, 2 food, 2 nightlife, 1 exhibition).
- Exclude duplicate items appearing in multiple sources.
- Prefer sources with clear links and event details.

Fallback:

- If a source fails, continue with available sources and still send digest.
- If not enough high-quality items, send shorter digest instead of skipping.

## 7. Delivery Channel Decision

Choose one for MVP:

- Telegram bot (typically easiest to start, good developer ergonomics).
- WhatsApp (possible but often more setup and policy overhead).

Recommendation:

- Start with Telegram for MVP speed.
- Add WhatsApp after proving weekly quality and reliability.

## 8. System Design (MVP)

Minimal components:

- Fetcher: pulls raw content from selected sources.
- Parser: extracts title, date, category, link, short description.
- Curator: deduplicates, filters by date/location, picks top items.
- Formatter: builds final digest text.
- Scheduler: runs every Friday morning (Europe/Berlin).
- Messenger: sends digest to channel.

Data storage:

- Lightweight store (JSON/SQLite) for:
  - last run timestamp,
  - already-sent links (to reduce repeats),
  - source fetch status.

## 9. Quality Bar

Each weekly digest should be:

- Timely: focused on current weekend only.
- Actionable: every item has a link and clear context.
- Short: scannable in under 2 minutes.
- Reliable: sent every Friday even if some sources fail.

## 10. Execution Plan

Phase 1: Skeleton (Day 1)

- Set up project structure and config.
- Implement scheduler + manual trigger command.
- Implement messenger for one channel.

Phase 2: Sources + parsing (Days 2-3)

- Integrate first 3-5 high-value sources from `sources.md`.
- Normalize extracted items into one schema.
- Add date filtering for current weekend.

Phase 3: Curation + formatting (Day 4)

- Add deduplication and category balancing.
- Generate clean digest template.
- Add basic logging and error handling.

Phase 4: Dry runs + launch (Day 5)

- Run locally with sample output.
- Validate message quality for one weekend.
- Turn on scheduled Friday delivery.

## 11. MVP Definition of Done

- One command to run pipeline end-to-end.
- Automated Friday schedule works in Europe/Berlin timezone.
- Digest is sent successfully to chosen channel.
- At least 5 meaningful items per digest (or fewer with explicit fallback note).
- Basic logs exist for fetch, parse, curate, and send steps.

## 12. Open Decisions

- Final channel for MVP: Telegram or WhatsApp.
- Exact Friday send time (for example: 08:00 or 09:00 Berlin time).
- Initial source subset (recommended: start with 3-5, not all at once).

