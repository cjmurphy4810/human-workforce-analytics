"""One-time OAuth helper to obtain a refresh token.

Usage:
    python scripts/get_refresh_token.py /path/to/client_secret.json

Opens a browser, prompts you to log in to chrisjmurphyauthor@gmail.com,
and prints the refresh token. Save the printed values into your secrets.
"""

import sys

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]


def main(client_secret_path: str) -> None:
    flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
    creds = flow.run_local_server(
        port=0,
        access_type="offline",
        prompt="consent",
    )
    print("\n=== COPY THESE INTO YOUR SECRETS ===")
    print(f"YT_CLIENT_ID = {flow.client_config['client_id']}")
    print(f"YT_CLIENT_SECRET = {flow.client_config['client_secret']}")
    print(f"YT_REFRESH_TOKEN = {creds.refresh_token}")
    print("====================================\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python scripts/get_refresh_token.py /path/to/client_secret.json")
        sys.exit(1)
    main(sys.argv[1])
