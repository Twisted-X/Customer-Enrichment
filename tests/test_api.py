"""
FastAPI endpoint tests — uses TestClient (no real network/browser calls).

Playwright-dependent endpoints (/api/check, /api/scrape) are tested with
_check_url_sync and _scrape_url_sync mocked to avoid browser startup.
"""
import sys
import types
import unittest.mock
import pytest

# ---------------------------------------------------------------------------
# Ensure config module doesn't fail even without the SKU xlsx in CI.
# MIN_EXPECTED_STYLE_CODES=0 is set in tests/conftest.py before this import.
# ---------------------------------------------------------------------------
from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    """TestClient for the FastAPI app. Imported once per module."""
    from api_server import app
    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_contains_healthy_status(self, client):
        resp = client.get("/health")
        data = resp.json()
        assert data.get("status") == "healthy"

    def test_contains_timestamp(self, client):
        resp = client.get("/health")
        data = resp.json()
        assert "timestamp" in data


# ---------------------------------------------------------------------------
# GET /api/test
# ---------------------------------------------------------------------------

class TestApiTestEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/api/test")
        assert resp.status_code == 200

    def test_returns_message(self, client):
        resp = client.get("/api/test")
        data = resp.json()
        assert "message" in data


# ---------------------------------------------------------------------------
# GET /
# ---------------------------------------------------------------------------

class TestRootEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_lists_endpoints(self, client):
        resp = client.get("/")
        data = resp.json()
        assert "endpoints" in data


# ---------------------------------------------------------------------------
# POST /api/check
# ---------------------------------------------------------------------------

class TestCheckEndpoint:
    def _mock_check_result(self, url, sells=False, retailer="example"):
        return {
            "url": url,
            "retailer": retailer,
            "sells_twisted_x": sells,
            "sells_footwear": True,
            "confidence": "high" if sells else "low",
            "store_type": "online",
            "sells_online": True,
            "proof": ["MCA0070 in page text"] if sells else [],
            "sample_products": [],
            "page_url": url,
            "checked_at": "2026-01-01T00:00:00",
            "error": None,
            "blocked": False,
        }

    def test_missing_url_field_returns_422(self, client):
        resp = client.post("/api/check", json={})
        assert resp.status_code == 422

    def test_valid_url_returns_200_with_mocked_check(self, client):
        url = "https://bootbarn.com"
        mock_result = self._mock_check_result(url, sells=True, retailer="bootbarn")
        with unittest.mock.patch("api_server._check_url_sync", return_value=mock_result):
            resp = client.post("/api/check", json={"url": url})
        assert resp.status_code == 200

    def test_response_has_sells_twisted_x_field(self, client):
        url = "https://bootbarn.com"
        mock_result = self._mock_check_result(url, sells=True, retailer="bootbarn")
        with unittest.mock.patch("api_server._check_url_sync", return_value=mock_result):
            data = client.post("/api/check", json={"url": url}).json()
        assert "sells_twisted_x" in data

    def test_response_has_proof_list(self, client):
        url = "https://bootbarn.com"
        mock_result = self._mock_check_result(url, sells=True, retailer="bootbarn")
        with unittest.mock.patch("api_server._check_url_sync", return_value=mock_result):
            data = client.post("/api/check", json={"url": url}).json()
        assert isinstance(data.get("proof"), list)

    def test_no_sells_result(self, client):
        url = "https://unknownsite.com"
        mock_result = self._mock_check_result(url, sells=False, retailer="unknownsite")
        with unittest.mock.patch("api_server._check_url_sync", return_value=mock_result):
            data = client.post("/api/check", json={"url": url}).json()
        assert data["sells_twisted_x"] is False


# ---------------------------------------------------------------------------
# POST /api/verify
# ---------------------------------------------------------------------------

class TestVerifyEndpoint:
    """
    /api/verify is pure Python (no browser/network) — no mocking needed.
    """
    def test_missing_body_returns_422(self, client):
        resp = client.post("/api/verify", json={})
        assert resp.status_code == 422

    def test_valid_request_returns_200(self, client):
        body = {
            "url": "https://bootbarn.com",
            "extracted_products": [
                {
                    "name": "Twisted X Men's Western Boot",
                    "price": "$129.99",
                    "sku": "MCA0070",
                    "product_url": "https://bootbarn.com/products/mca0070",
                }
            ],
            "original_products": [
                {
                    "text": "Twisted X Men's Western Boot MCA0070 $129.99",
                    "html_snippet": "",
                    "links": [],
                    "images": [],
                }
            ],
        }
        resp = client.post("/api/verify", json=body)
        assert resp.status_code == 200

    def test_verified_products_in_response(self, client):
        body = {
            "url": "https://bootbarn.com",
            "extracted_products": [
                {"name": "Twisted X Men's Western Boot", "price": "$129", "sku": "MCA0070",
                 "product_url": ""}
            ],
            "original_products": [
                {"text": "Twisted X Men's Western Boot MCA0070 $129", "html_snippet": "",
                 "links": [], "images": []}
            ],
        }
        data = client.post("/api/verify", json=body).json()
        assert "verified_products" in data
        assert "flagged_products" in data
        assert "verification_stats" in data

    def test_empty_products_returns_empty_lists(self, client):
        body = {
            "url": "https://bootbarn.com",
            "extracted_products": [],
            "original_products": [],
        }
        data = client.post("/api/verify", json=body).json()
        assert data["verified_products"] == []
        assert data["flagged_products"] == []

    def test_hallucinated_product_is_flagged(self, client):
        body = {
            "url": "https://bootbarn.com",
            "extracted_products": [
                {"name": "Completely Made Up Product XYZ999", "price": "$9999",
                 "sku": "FAKECODE123", "product_url": ""}
            ],
            "original_products": [
                {"text": "Twisted X Western Boot $129", "html_snippet": "",
                 "links": [], "images": []}
            ],
        }
        data = client.post("/api/verify", json=body).json()
        # Product name doesn't match block text → should be flagged
        stats = data["verification_stats"]
        assert stats["total_input"] == 1


# ---------------------------------------------------------------------------
# POST /api/scrape
# ---------------------------------------------------------------------------

class TestScrapeEndpoint:
    def _mock_scrape_result(self, url):
        return {
            "url": url,
            "retailer": "bootbarn",
            "scraped_at": "2026-01-01T00:00:00",
            "method": "playwright",
            "store_type": "online",
            "sells_online": True,
            "online_confidence": "high",
            "online_indicators": [],
            "blockers": [],
            "total_products": 1,
            "products": [
                {
                    "text": "Twisted X Boot MCA0070 $129",
                    "html_snippet": "<div>Twisted X Boot</div>",
                    "links": [],
                    "images": [],
                    "position": 1,
                }
            ],
            "errors": [],
        }

    def test_missing_url_returns_422(self, client):
        resp = client.post("/api/scrape", json={})
        assert resp.status_code == 422

    def test_valid_url_returns_200_with_mocked_scrape(self, client):
        url = "https://bootbarn.com"
        mock_result = self._mock_scrape_result(url)
        with unittest.mock.patch("api_server._scrape_url_sync", return_value=mock_result):
            resp = client.post("/api/scrape", json={"url": url})
        assert resp.status_code == 200

    def test_response_has_products_list(self, client):
        url = "https://bootbarn.com"
        mock_result = self._mock_scrape_result(url)
        with unittest.mock.patch("api_server._scrape_url_sync", return_value=mock_result):
            data = client.post("/api/scrape", json={"url": url}).json()
        assert "products" in data
        assert isinstance(data["products"], list)
