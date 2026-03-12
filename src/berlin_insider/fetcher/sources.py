from __future__ import annotations

from berlin_insider.fetcher.adapters.html import HtmlAdapter
from berlin_insider.fetcher.adapters.playwright_html import PlaywrightHtmlAdapter
from berlin_insider.fetcher.adapters.rss import RssAdapter
from berlin_insider.fetcher.adapters.tip_berlin_playwright import TipBerlinPlaywrightAdapter
from berlin_insider.fetcher.base import SourceAdapter, SourceDefinition
from berlin_insider.fetcher.models import SourceId
from berlin_insider.fetcher.parsers.content import (
    parse_berlin_food_stories,
    parse_gratis_in_berlin,
    parse_rausgegangen,
    parse_rausgegangen_daily,
    parse_telegram,
)
from berlin_insider.fetcher.parsers.daily import (
    parse_berlin_de_tickets_heute,
    parse_ra_berlin,
    parse_visit_berlin_daily,
)
from berlin_insider.fetcher.parsers.eventbrite import parse_eventbrite_jsonld


def _rss(source_id: SourceId, source_url: str, feed_url: str) -> RssAdapter:
    definition = SourceDefinition(source_id=source_id, source_url=source_url)
    return RssAdapter(definition=definition, feed_url=feed_url)


def _html(source_id: SourceId, source_url: str, parser) -> HtmlAdapter:
    definition = SourceDefinition(source_id=source_id, source_url=source_url)
    return HtmlAdapter(definition=definition, parser=parser)


def _tip(source_id: SourceId, source_url: str) -> TipBerlinPlaywrightAdapter:
    definition = SourceDefinition(source_id=source_id, source_url=source_url)
    return TipBerlinPlaywrightAdapter(definition=definition)


def _playwright_html(source_id: SourceId, source_url: str, parser) -> PlaywrightHtmlAdapter:
    definition = SourceDefinition(source_id=source_id, source_url=source_url)
    return PlaywrightHtmlAdapter(definition=definition, parser=parser)


SOURCES: dict[SourceId, SourceAdapter] = {
    SourceId.BERLIN_DE_WOCHENEND_TIPPS: _rss(
        SourceId.BERLIN_DE_WOCHENEND_TIPPS,
        "https://www.berlin.de/wochenend-tipps/",
        "https://www.berlin.de/wochenend-tipps/index.rss",
    ),
    SourceId.BERLIN_DE_TICKETS_HEUTE: _html(
        SourceId.BERLIN_DE_TICKETS_HEUTE,
        "https://www.berlin.de/tickets/heute/",
        parse_berlin_de_tickets_heute,
    ),
    SourceId.BLOG_IN_BERLIN: _rss(
        SourceId.BLOG_IN_BERLIN,
        "https://blog.inberlin.de/",
        "https://blog.inberlin.de/feed/",
    ),
    SourceId.IHEART_BERLIN: _rss(
        SourceId.IHEART_BERLIN,
        "https://www.iheartberlin.de/de/",
        "https://www.iheartberlin.de/de/feed/",
    ),
    SourceId.MITVERGNUEGEN: _rss(
        SourceId.MITVERGNUEGEN,
        "https://mitvergnuegen.com/",
        "https://mitvergnuegen.com/feed/",
    ),
    SourceId.RA_BERLIN: _playwright_html(
        SourceId.RA_BERLIN,
        "https://de.ra.co/events/de/berlin",
        parse_ra_berlin,
    ),
    SourceId.TIP_BERLIN_DAILY: _tip(SourceId.TIP_BERLIN_DAILY, "https://www.tip-berlin.de/event/"),
    SourceId.TIP_BERLIN_HOME: _tip(SourceId.TIP_BERLIN_HOME, "https://www.tip-berlin.de/"),
    SourceId.TIP_BERLIN_WEEKEND: _tip(
        SourceId.TIP_BERLIN_WEEKEND,
        "https://www.tip-berlin.de/tageshighlights/veranstaltungstipps-wochenende/",
    ),
    SourceId.VISIT_BERLIN_DAILY: _html(
        SourceId.VISIT_BERLIN_DAILY,
        "https://www.visitberlin.de/de/tagestipps-veranstaltungen-berlin-karte",
        parse_visit_berlin_daily,
    ),
    SourceId.VISIT_BERLIN_BLOG: _rss(
        SourceId.VISIT_BERLIN_BLOG,
        "https://www.visitberlin.de/de/blog",
        "https://www.visitberlin.de/de/blog/feed",
    ),
    SourceId.BERLIN_FOOD_STORIES_EDITORIALS: _html(
        SourceId.BERLIN_FOOD_STORIES_EDITORIALS,
        "https://www.berlinfoodstories.com/editorials",
        parse_berlin_food_stories,
    ),
    SourceId.BERLIN_FOOD_STORIES_NEWS: _html(
        SourceId.BERLIN_FOOD_STORIES_NEWS,
        "https://www.berlinfoodstories.com/news",
        parse_berlin_food_stories,
    ),
    SourceId.RAUSGEGANGEN_WEEKEND: _html(
        SourceId.RAUSGEGANGEN_WEEKEND,
        "https://rausgegangen.de/berlin/tipps-fuers-wochenende/",
        parse_rausgegangen,
    ),
    SourceId.RAUSGEGANGEN_DAILY: _html(
        SourceId.RAUSGEGANGEN_DAILY,
        "https://rausgegangen.de/berlin/",
        parse_rausgegangen_daily,
    ),
    SourceId.GRATIS_IN_BERLIN: _html(
        SourceId.GRATIS_IN_BERLIN,
        "https://www.gratis-in-berlin.de/",
        parse_gratis_in_berlin,
    ),
    SourceId.TELEGRAM_NIGHTDRIVE: _html(
        SourceId.TELEGRAM_NIGHTDRIVE,
        "https://t.me/s/G3Nightdrive",
        parse_telegram,
    ),
    SourceId.EVENTBRITE_BERLIN_WEEKEND: _html(
        SourceId.EVENTBRITE_BERLIN_WEEKEND,
        "https://www.eventbrite.de/d/germany--berlin/events--this-weekend/",
        parse_eventbrite_jsonld,
    ),
}
