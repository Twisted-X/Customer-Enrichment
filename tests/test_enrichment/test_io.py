"""
Tests for enrichment/_io.py — should_enrich() covers all TTL/error branches.

load_dataframe, save_output, sftp_session are not exercised here because
they require real files or SFTP credentials; those belong in integration tests.
"""
import datetime
from enrichment._io import should_enrich
from enrichment._config import NETSUITE_LAST_ENRICHED_COL, ENRICHMENT_TTL_DAYS


def _days_ago(n):
    """Return an ISO date string N days in the past."""
    d = datetime.date.today() - datetime.timedelta(days=n)
    return d.isoformat()


def _row(**kwargs):
    """Build a minimal row dict; keyword args override defaults."""
    base = {NETSUITE_LAST_ENRICHED_COL: "", "enrichment_source": ""}
    base.update(kwargs)
    return base


class TestShouldEnrich:
    # ── Always re-enrich ──────────────────────────────────────────────────────

    def test_blank_date_returns_true(self):
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: ""})) is True

    def test_missing_date_column_returns_true(self):
        assert should_enrich({"enrichment_source": ""}) is True

    def test_none_date_returns_true(self):
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: None})) is True

    def test_nan_string_date_returns_true(self):
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: "nan"})) is True

    def test_enrichment_error_source_always_re_enriches(self):
        row = _row(
            enrichment_source="enrichment_error",
            **{NETSUITE_LAST_ENRICHED_COL: _days_ago(1)}
        )
        assert should_enrich(row) is True

    def test_address_mismatch_source_always_re_enriches(self):
        row = _row(
            enrichment_source="address_mismatch",
            **{NETSUITE_LAST_ENRICHED_COL: _days_ago(1)}
        )
        assert should_enrich(row) is True

    def test_unparseable_date_returns_true(self):
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: "not-a-date"})) is True

    # ── Skip re-enrichment (within TTL) ──────────────────────────────────────

    def test_recent_date_within_ttl_returns_false(self):
        recent = _days_ago(ENRICHMENT_TTL_DAYS - 1)
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: recent})) is False

    def test_today_returns_false(self):
        today = datetime.date.today().isoformat()
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: today})) is False

    # ── Re-enrich (past TTL) ──────────────────────────────────────────────────

    def test_old_date_beyond_ttl_returns_true(self):
        old = _days_ago(ENRICHMENT_TTL_DAYS + 1)
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: old})) is True

    def test_exactly_at_ttl_boundary_returns_true(self):
        boundary = _days_ago(ENRICHMENT_TTL_DAYS)
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: boundary})) is True

    # ── Source does not override TTL for clean past runs ─────────────────────

    def test_hybrid_full_source_with_recent_date_returns_false(self):
        recent = _days_ago(ENRICHMENT_TTL_DAYS - 5)
        row = _row(
            enrichment_source="hybrid_full",
            **{NETSUITE_LAST_ENRICHED_COL: recent}
        )
        assert should_enrich(row) is False

    def test_url_only_source_with_old_date_returns_true(self):
        old = _days_ago(ENRICHMENT_TTL_DAYS + 10)
        row = _row(
            enrichment_source="url_only",
            **{NETSUITE_LAST_ENRICHED_COL: old}
        )
        assert should_enrich(row) is True

    # ── Date format tolerance ─────────────────────────────────────────────────

    def test_datetime_with_time_component(self):
        # Some exports include HH:MM:SS
        recent = (datetime.date.today() - datetime.timedelta(days=2)).isoformat() + " 14:30:00"
        assert should_enrich(_row(**{NETSUITE_LAST_ENRICHED_COL: recent})) is False
