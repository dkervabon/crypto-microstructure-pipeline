"""WSGI entry point for the Dash dashboard (used by gunicorn on Render).

    gunicorn wsgi:server

`src` is added to the path so the `crypto_pipeline` package is importable
without an editable install.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from crypto_pipeline.dashboard.app import server  # noqa: E402,F401
