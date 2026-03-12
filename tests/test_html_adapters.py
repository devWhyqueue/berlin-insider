from datetime import UTC, datetime

from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import FetchContext, SourceId
from berlin_insider.fetcher.parsers.content import (
    parse_berlin_food_stories,
    parse_gratis_in_berlin,
    parse_rausgegangen_daily,
    parse_rausgegangen,
    parse_telegram,
)
from berlin_insider.fetcher.parsers.daily import (
    parse_berlin_de_tickets_heute,
    parse_ra_berlin,
    parse_visit_berlin_daily,
)
from berlin_insider.fetcher.parsers.eventbrite import parse_eventbrite_jsonld


def _context() -> FetchContext:
    return FetchContext(
        user_agent="test-agent",
        timeout_seconds=5.0,
        max_items_per_source=10,
        collected_at=datetime.now(UTC),
    )


def test_parse_berlin_food_stories() -> None:
    html = """
    <h3 class="article-teaser__headline">
      <a href="https://www.berlinfoodstories.com/news/foo">Foo Story</a>
    </h3>
    """
    items = parse_berlin_food_stories(
        html,
        SourceDefinition(
            SourceId.BERLIN_FOOD_STORIES_NEWS, "https://www.berlinfoodstories.com/news"
        ),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Foo Story"


def test_parse_rausgegangen() -> None:
    html = """
    <a class="event-tile" href="/events/my-party/">
      <span class="text-sm">Heute, 27. Feb | 19:00 Uhr</span>
      <h4>My Party</h4>
      <span class="text-sm pr-1 opacity-70">Club X</span>
    </a>
    """
    items = parse_rausgegangen(
        html,
        SourceDefinition(
            SourceId.RAUSGEGANGEN_WEEKEND, "https://rausgegangen.de/berlin/tipps-fuers-wochenende/"
        ),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "My Party"
    assert items[0].location_hint == "Club X"


def test_parse_gratis_in_berlin() -> None:
    html = """
    <div class="tipp_wrapper">
      <h2 class="overviewcontentheading">
        <a href="/component/flexicontent/22-musik/2077071-abc">Concert</a>
      </h2>
      <div class="dateTipp">Heute</div>
    </div>
    """
    items = parse_gratis_in_berlin(
        html,
        SourceDefinition(SourceId.GRATIS_IN_BERLIN, "https://www.gratis-in-berlin.de/"),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Concert"
    assert items[0].raw_date_text == "Heute"


def test_parse_visit_berlin_daily() -> None:
    html = """
    <main>
      <article>
        <a href="/de/category/art">Art</a>
        <a href="/de/event/characters-languages-urban-space">
          <div>
            <p><time>24.10.2025</time> <time>31.05.2026</time></p>
          </div>
          <div>
            <h2>Characters. Languages. Urban space.</h2>
            <div>Urban space is full of signs.</div>
          </div>
        </a>
      </article>
    </main>
    """
    items = parse_visit_berlin_daily(
        html,
        SourceDefinition(
            SourceId.VISIT_BERLIN_DAILY,
            "https://www.visitberlin.de/de/tagestipps-veranstaltungen-berlin-karte",
        ),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Characters. Languages. Urban space."
    assert items[0].item_url == "https://www.visitberlin.de/de/event/characters-languages-urban-space"
    assert items[0].raw_date_text == "24.10.2025 31.05.2026"


def test_parse_rausgegangen_daily() -> None:
    html = """
    <a href="/events/naked-grapes-w-victordiscos-0/">
      <div>Heute, 12. Mar | 20:00 Uhr</div>
      <h4>Naked Grapes w/ VictorDiscos</h4>
      <div>Orangerie Neukolln</div>
    </a>
    """
    items = parse_rausgegangen_daily(
        html,
        SourceDefinition(SourceId.RAUSGEGANGEN_DAILY, "https://rausgegangen.de/berlin/"),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Naked Grapes w/ VictorDiscos"
    assert items[0].location_hint == "Orangerie Neukolln"
    assert items[0].item_url == "https://rausgegangen.de/events/naked-grapes-w-victordiscos-0/"


def test_parse_berlin_de_tickets_heute() -> None:
    html = """
    <main>
      <article>
        <h3><a href="https://www.berlin.de/tickets/show/blinded-by-delight/">Blinded by Delight</a></h3>
        <p>In der Hauptstadt werden Traume wahr. <a href="https://www.berlin.de/tickets/show/blinded-by-delight/">mehr</a></p>
      </article>
    </main>
    """
    items = parse_berlin_de_tickets_heute(
        html,
        SourceDefinition(SourceId.BERLIN_DE_TICKETS_HEUTE, "https://www.berlin.de/tickets/heute/"),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Blinded by Delight"
    assert items[0].snippet == "In der Hauptstadt werden Traume wahr."


def test_parse_ra_berlin() -> None:
    html = """
    <main>
      <div>
        <h3>Do., 12 Mar</h3>
        <div>
          <h3><a href="https://de.ra.co/events/2359569">Renate Klubnacht with Dazed State &amp; The Unknown</a></h3>
          <div>
            <a href="/clubs/8556">Renate</a>
          </div>
        </div>
      </div>
    </main>
    """
    items = parse_ra_berlin(
        html,
        SourceDefinition(SourceId.RA_BERLIN, "https://de.ra.co/events/de/berlin"),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Renate Klubnacht with Dazed State & The Unknown"
    assert items[0].location_hint == "Renate"
    assert items[0].raw_date_text == "Donnerstag"
    assert items[0].item_url == "https://de.ra.co/events/2359569"


def test_parse_telegram() -> None:
    html = """
    <div class="tgme_widget_message_wrap">
      <div class="tgme_widget_message" data-post="G3Nightdrive/11482"></div>
      <a class="tgme_widget_message_date" href="https://t.me/G3Nightdrive/11482"></a>
      <time datetime="2026-02-26T04:49:16+00:00">04:49</time>
      <div class="tgme_widget_message_text">Hello Berlin</div>
    </div>
    """
    items = parse_telegram(
        html,
        SourceDefinition(SourceId.TELEGRAM_NIGHTDRIVE, "https://t.me/s/G3Nightdrive"),
        _context(),
    )
    assert len(items) == 1
    assert items[0].item_url == "https://t.me/G3Nightdrive/11482"
    assert items[0].snippet == "Hello Berlin"


def test_parse_eventbrite_jsonld() -> None:
    html = """
    <script type="application/ld+json">
      {
        "@type": "ItemList",
        "itemListElement": [
          {
            "item": {
              "@type": "Event",
              "name": "Berlin Classic Festival",
              "url": "https://www.eventbrite.de/e/123",
              "startDate": "2026-02-28",
              "location": {"name": "Spreelounge"},
              "description": "Live classical music."
            }
          }
        ]
      }
    </script>
    """
    items = parse_eventbrite_jsonld(
        html,
        SourceDefinition(
            SourceId.EVENTBRITE_BERLIN_WEEKEND,
            "https://www.eventbrite.de/d/germany--berlin/events--this-weekend/",
        ),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Berlin Classic Festival"
    assert items[0].location_hint == "Spreelounge"


def test_parse_eventbrite_jsonld_nested_item_list() -> None:
    html = """
    <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@graph": [
          {
            "@type": "WebPage",
            "mainEntity": {
              "@type": "ItemList",
              "itemListElement": [
                {
                  "@type": "ListItem",
                  "position": 1,
                  "item": {
                    "@type": "Event",
                    "name": "Nested Event",
                    "url": "https://www.eventbrite.de/e/nested",
                    "startDate": "2026-03-08",
                    "location": {"name": "Nested Venue"},
                    "description": "Nested description."
                  }
                }
              ]
            }
          }
        ]
      }
    </script>
    """
    items = parse_eventbrite_jsonld(
        html,
        SourceDefinition(
            SourceId.EVENTBRITE_BERLIN_WEEKEND,
            "https://www.eventbrite.de/d/germany--berlin/events--this-weekend/",
        ),
        _context(),
    )
    assert len(items) == 1
    assert items[0].title == "Nested Event"
    assert items[0].location_hint == "Nested Venue"
