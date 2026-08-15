# Public Channel Analytics Demo

This password-free public app presents the AI Engineering Genius channel using
synthetic data only. Database generation is deterministic and runs entirely
offline.

From the repository root, generate the fixture, build the audited deployment
artifact, and execute the demo test suite with:

```bash
.venv/bin/python -m demo.generate_data --output demo/data/demo.db --seed 8142026
.venv/bin/python -m demo.build_artifact --output /tmp/public-demo
.venv/bin/pytest tests/demo -v
```

The artifact is the deployment boundary. It contains a SHA-256 manifest, the
synthetic fixture, the seven demo routes, and only their allowlisted pure
analytics dependencies. It intentionally excludes the production database,
application pages, live services, credentials, generator, tests, and this
documentation. Launch it independently of the source checkout with:

```bash
cd /tmp/public-demo
/path/to/streamlit run app.py
```

The generated `demo/data/demo.db` database is synthetic and contains no
production channel data or credentials. Creating or updating any deployment is
a separate operation and requires explicit authorization.
