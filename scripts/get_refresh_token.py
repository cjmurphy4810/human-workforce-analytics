"""One-time OAuth helper to obtain a refresh token.

Usage:
    python scripts/get_refresh_token.py /path/to/client_secret.json

Opens a browser, prompts you to log in, and writes credentials to
.oauth_credentials.json (gitignored) for reliable extraction.
"""

import json
import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]

OUT = Path(__file__).resolve().parent.parent / ".oauth_credentials.json"


def main(client_secret_path: str) -> None:
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    creds = flow.run_local_server(
        port=0,
        access_type="offline",
        prompt="consent",
    )
    payload = {
        "client_id": flow.client_config["client_id"],
        "client_secret": flow.client_config["client_secret"],
        "refresh_token": creds.refresh_token,
    }
    OUT.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote credentials to {OUT}")
    print("Now run:")
    print(f"  python -c \"import json; print(json.load(open('{OUT}'))['refresh_token'])\" | gh secret set YT_REFRESH_TOKEN --repo cjmurphy4810/human-workforce-analytics")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/get_refresh_token.py /path/to/client_secret.json")
        sys.exit(1)
    main(sys.argv[1])
