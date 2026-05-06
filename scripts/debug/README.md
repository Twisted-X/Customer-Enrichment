# Debug Scripts

Ad-hoc one-off scripts used to reproduce issues or investigate behaviour against
specific URLs. **Not part of the pipeline.** They typically have hardcoded test
inputs and require the API server to be running.

Run from the repo root:

```bash
source venv/bin/activate
python scripts/debug/debug_batch_repro.py
```
