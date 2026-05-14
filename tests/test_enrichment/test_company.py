"""
Tests for enrichment/_company.py — company key normalisation and branch matching.
"""
import math
import pandas as pd

from enrichment._company import (
    normalize_company_key,
    is_channel_row,
    pick_branch_candidate_for_row,
    pick_places_result_for_company,
    build_branch_norms,
)
from enrichment._config import COMPANY_COL


# ---------------------------------------------------------------------------
# normalize_company_key
# ---------------------------------------------------------------------------

class TestNormalizeCompanyKey:
    def _row(self, name):
        return {COMPANY_COL: name}

    def test_basic_name(self):
        assert normalize_company_key(self._row("Boot Barn"), COMPANY_COL) == "boot barn"

    def test_llc_suffix_stripped(self):
        key = normalize_company_key(self._row("Academy LLC"), COMPANY_COL)
        assert "llc" not in key
        assert key == "academy"

    def test_inc_suffix_stripped(self):
        key = normalize_company_key(self._row("Western Wear Inc"), COMPANY_COL)
        assert key == "western wear"

    def test_ltd_suffix_stripped(self):
        key = normalize_company_key(self._row("Ranch Supply Ltd"), COMPANY_COL)
        assert key == "ranch supply"

    def test_ampersand_normalised(self):
        key = normalize_company_key(self._row("Boots & Jeans"), COMPANY_COL)
        assert key == "boots and jeans"

    def test_branch_suffix_stripped(self):
        # "Boot Barn - Katy Store" → "Boot Barn"
        key = normalize_company_key(self._row("Boot Barn - Katy Store"), COMPANY_COL)
        assert key == "boot barn"

    def test_none_returns_empty(self):
        assert normalize_company_key(self._row(None), COMPANY_COL) == ""

    def test_nan_returns_empty(self):
        assert normalize_company_key(self._row(math.nan), COMPANY_COL) == ""

    def test_empty_string_returns_empty(self):
        assert normalize_company_key(self._row(""), COMPANY_COL) == ""

    def test_whitespace_collapsed(self):
        key = normalize_company_key(self._row("  Boot   Barn  "), COMPANY_COL)
        assert key == "boot barn"

    def test_case_lowercased(self):
        key = normalize_company_key(self._row("CAVENDERS"), COMPANY_COL)
        assert key == "cavenders"

    def test_missing_key_in_row(self):
        assert normalize_company_key({}, COMPANY_COL) == ""


# ---------------------------------------------------------------------------
# is_channel_row
# ---------------------------------------------------------------------------

class TestIsChannelRow:
    def test_ecommerce_suffix(self):
        assert is_channel_row("Boot Barn - ecommerce") is True

    def test_online_suffix(self):
        assert is_channel_row("Western Wear - Online") is True

    def test_web_suffix(self):
        assert is_channel_row("Cavenders - web") is True

    def test_ecom_suffix(self):
        assert is_channel_row("Ranch - ecom") is True

    def test_website_suffix(self):
        assert is_channel_row("Boot Co - website") is True

    def test_no_dash_is_false(self):
        assert is_channel_row("Boot Barn Online") is False

    def test_wrong_suffix_is_false(self):
        assert is_channel_row("Boot Barn - Texas") is False

    def test_empty_string_is_false(self):
        assert is_channel_row("") is False

    def test_regular_company_is_false(self):
        assert is_channel_row("Academy Sports") is False


# ---------------------------------------------------------------------------
# pick_branch_candidate_for_row
# ---------------------------------------------------------------------------

class TestPickBranchCandidateForRow:
    CANDIDATES = [
        {"formatted_address": "100 Main St, Austin, TX 78701, USA", "displayName": {"text": "Store A"}},
        {"formatted_address": "200 Elm St, Houston, TX 77002, USA", "displayName": {"text": "Store B"}},
        {"formatted_address": "300 Oak Ave, Denver, CO 80203, USA", "displayName": {"text": "Store C"}},
    ]

    def test_matches_exact_zip(self):
        cand, conf = pick_branch_candidate_for_row(self.CANDIDATES, "Austin", "TX", "78701")
        assert cand is not None
        assert conf == "high"
        assert cand["displayName"]["text"] == "Store A"

    def test_matches_city_wrong_zip(self):
        cand, conf = pick_branch_candidate_for_row(self.CANDIDATES, "Austin", "TX", "78999")
        assert cand is not None
        assert conf == "medium"

    def test_no_match_returns_none(self):
        cand, conf = pick_branch_candidate_for_row(self.CANDIDATES, "Dallas", "TX", "75201")
        assert cand is None
        assert conf == "none"

    def test_state_mismatch_returns_none(self):
        # Address is TX but we say CA → mismatch
        cand, conf = pick_branch_candidate_for_row(self.CANDIDATES, "Austin", "CA", "78701")
        assert cand is None
        assert conf == "none"

    def test_empty_candidates_returns_none(self):
        cand, conf = pick_branch_candidate_for_row([], "Austin", "TX", "78701")
        assert cand is None
        assert conf == "none"


# ---------------------------------------------------------------------------
# pick_places_result_for_company
# ---------------------------------------------------------------------------

class TestPickPlacesResultForCompany:
    CANDIDATES = [
        {"formatted_address": "100 Main St, Austin, TX 78701, USA"},
        {"formatted_address": "200 Elm St, Houston, TX 77002, USA"},
    ]
    # Pre-normalised branch tuples
    BRANCH_NORMS_AUSTIN = [("AUSTIN", "TX", "78701")]
    BRANCH_NORMS_HOUSTON = [("HOUSTON", "TX", "77002")]

    def test_matches_first_candidate(self):
        result = pick_places_result_for_company(self.CANDIDATES, self.BRANCH_NORMS_AUSTIN)
        assert result == self.CANDIDATES[0]

    def test_matches_second_candidate(self):
        result = pick_places_result_for_company(self.CANDIDATES, self.BRANCH_NORMS_HOUSTON)
        assert result == self.CANDIDATES[1]

    def test_empty_candidates_returns_none(self):
        assert pick_places_result_for_company([], self.BRANCH_NORMS_AUSTIN) is None

    def test_empty_branch_norms_returns_none(self):
        assert pick_places_result_for_company(self.CANDIDATES, []) is None

    def test_no_address_match_returns_none(self):
        norms = [("DALLAS", "TX", "75201")]
        assert pick_places_result_for_company(self.CANDIDATES, norms) is None

    def test_candidate_without_address_skipped(self):
        candidates = [{"formatted_address": ""}, {"formatted_address": "100 Main St, Austin, TX 78701"}]
        result = pick_places_result_for_company(candidates, self.BRANCH_NORMS_AUSTIN)
        assert result == candidates[1]


# ---------------------------------------------------------------------------
# build_branch_norms
# ---------------------------------------------------------------------------

class TestBuildBranchNorms:
    def _df(self, rows):
        return pd.DataFrame(rows)

    def test_builds_norms_for_known_company(self):
        df = self._df([
            {COMPANY_COL: "Boot Barn", "city": "Austin", "state": "TX", "zip code": "78701"},
            {COMPANY_COL: "Boot Barn", "city": "Houston", "state": "TX", "zip code": "77002"},
        ])
        norms = build_branch_norms(df)
        assert "boot barn" in norms
        assert len(norms["boot barn"]) == 2

    def test_deduplicates_identical_branches(self):
        df = self._df([
            {COMPANY_COL: "Cavenders", "city": "Austin", "state": "TX", "zip code": "78701"},
            {COMPANY_COL: "Cavenders", "city": "Austin", "state": "TX", "zip code": "78701"},
        ])
        norms = build_branch_norms(df)
        assert len(norms.get("cavenders", [])) == 1

    def test_skips_rows_with_no_address(self):
        df = self._df([
            {COMPANY_COL: "Ranch Supply", "city": "", "state": "", "zip code": ""},
        ])
        norms = build_branch_norms(df)
        assert "ranch supply" not in norms

    def test_skips_rows_with_no_company(self):
        df = self._df([
            {COMPANY_COL: None, "city": "Austin", "state": "TX", "zip code": "78701"},
        ])
        norms = build_branch_norms(df)
        assert "" not in norms

    def test_groups_by_normalised_key(self):
        # "Boot Barn LLC" and "Boot Barn" should group together
        df = self._df([
            {COMPANY_COL: "Boot Barn LLC", "city": "Austin", "state": "TX", "zip code": "78701"},
            {COMPANY_COL: "Boot Barn",     "city": "Dallas", "state": "TX", "zip code": "75201"},
        ])
        norms = build_branch_norms(df)
        assert "boot barn" in norms
        assert len(norms["boot barn"]) == 2

    def test_no_address_columns_returns_empty_for_company(self):
        df = self._df([{COMPANY_COL: "Boots Inc"}])
        norms = build_branch_norms(df)
        assert norms == {}
