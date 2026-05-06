# Manual Diagnostic Scripts

These are **manual diagnostic / smoke-test scripts**, not pytest tests. They
make live network calls (Google Places API, SFTP server, retailer websites)
and require a populated `.env` at the repo root.

Run from the repo root with the venv active:

```bash
source venv/bin/activate
python tests/manual/test_dillards.py        # Google Places lookup spot-check
python tests/manual/test_root_domain.py     # URL normalization spot-check
python tests/manual/test_sftp_connection.py # SFTP connectivity check
```

Real automated tests should live in a sibling `tests/unit/` or `tests/integration/`
folder using pytest, with no external network dependencies.
