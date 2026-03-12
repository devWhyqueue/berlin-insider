from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from berlin_insider.web.models import (
    DeliveriesResponse,
    FeedbackResponse,
    ItemsResponse,
    OpsResponse,
    OverviewResponse,
)
from berlin_insider.web.render import BASE_PATH, STATIC_PATH, _render_dashboard_html
from berlin_insider.web.repository import _PublicSiteRepository

STATIC_DIR = Path(__file__).resolve().parent.parent / "public_static"


@dataclass(slots=True)
class PublicSiteDependencies:
    db_path: Path


def attach_public_site(app: FastAPI, *, deps: PublicSiteDependencies) -> None:
    """Mount the public Berlin Insider dashboard and sanitized JSON routes."""
    repo = _PublicSiteRepository(deps.db_path)
    router = _build_router(repo)
    app.mount(STATIC_PATH, StaticFiles(directory=STATIC_DIR), name="berlin-insider-static")
    app.include_router(router, prefix=BASE_PATH)


def _build_router(repo: _PublicSiteRepository) -> APIRouter:
    router = APIRouter()
    router.add_api_route("/", _index_endpoint(repo), response_class=HTMLResponse, methods=["GET"])
    router.add_api_route("/api/overview", _overview_endpoint(repo), methods=["GET"])
    router.add_api_route("/api/items", _items_endpoint(repo), methods=["GET"])
    router.add_api_route("/api/deliveries", _deliveries_endpoint(repo), methods=["GET"])
    router.add_api_route("/api/feedback", _feedback_endpoint(repo), methods=["GET"])
    router.add_api_route("/api/ops", _ops_endpoint(repo), methods=["GET"])
    return router


def _deliveries_endpoint(repo: _PublicSiteRepository):
    async def _endpoint() -> DeliveriesResponse:
        return repo._deliveries()

    return _endpoint


def _feedback_endpoint(repo: _PublicSiteRepository):
    async def _endpoint() -> FeedbackResponse:
        return repo._feedback()

    return _endpoint


def _index_endpoint(repo: _PublicSiteRepository):
    async def _endpoint() -> HTMLResponse:
        return HTMLResponse(
            _render_dashboard_html(
                overview=repo._overview(),
                items=repo._items(
                    source=None,
                    category=None,
                    has_summary=None,
                    timing=None,
                    search_text=None,
                ),
                deliveries=repo._deliveries(),
                feedback=repo._feedback(),
                ops=repo._ops(),
            )
        )

    return _endpoint


def _items_endpoint(repo: _PublicSiteRepository):
    async def _endpoint(
        source: str | None = Query(default=None),
        category: str | None = Query(default=None),
        has_summary: bool | None = Query(default=None),
        timing: Literal["upcoming", "undated"] | None = Query(default=None),
        search: str | None = Query(default=None),
    ) -> ItemsResponse:
        return repo._items(
            source=source,
            category=category,
            has_summary=has_summary,
            timing=timing,
            search_text=search,
        )

    return _endpoint


def _ops_endpoint(repo: _PublicSiteRepository):
    async def _endpoint() -> OpsResponse:
        return repo._ops()

    return _endpoint


def _overview_endpoint(repo: _PublicSiteRepository):
    async def _endpoint() -> OverviewResponse:
        return repo._overview()

    return _endpoint
