# Public Channel Analytics Demo

This password-free public app presents the AI Engineering Genius channel using
synthetic data only. Database generation is deterministic and runs entirely
offline.

From the repository root, generate the fixture, run the app, and execute the
demo test suite with:

```bash
.venv/bin/python -m demo.generate_data --output demo/data/demo.db --seed 8142026
.venv/bin/streamlit run demo/app.py
.venv/bin/pytest tests/demo -v
```

The generated `demo/data/demo.db` database is synthetic and contains no
production channel data or credentials. Creating or updating any deployment is
a separate operation and requires explicit authorization.
