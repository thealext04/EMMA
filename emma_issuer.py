#!/usr/bin/env python3
"""
emma_issuer.py - Returns the issuing body name for a given CUSIP from EMMA.

Uses real session cookies captured from the browser to authenticate.
Note: AWSALB cookies expire periodically - re-run this generator if it stops working.

Usage:
    python emma_issuer.py 04781GAB7

Requirements:
    pip install requests beautifulsoup4
"""

import sys
import requests
from bs4 import BeautifulSoup

EMMA_BASE = "https://emma.msrb.org"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://emma.msrb.org/",
}

# Real session cookies from browser - refresh if script stops returning results
COOKIES = {
    "Disclaimer6": "msrborg",
    "AWSALB": "YQuIZ/mkn46OwzkIOAXZKCIlPfbjcHfGwBF2DkmiTyUDx75X8AS0IuqWRc8FeCnGPpQJPt6EMCHxDBV43uVcMoV5/H3J59wK+QNSUS96J1ZpzwbFdk0iehuudRi3",
    "AWSALBCORS": "YQuIZ/mkn46OwzkIOAXZKCIlPfbjcHfGwBF2DkmiTyUDx75X8AS0IuqWRc8FeCnGPpQJPt6EMCHxDBV43uVcMoV5/H3J59wK+QNSUS96J1ZpzwbFdk0iehuudRi3",
    "MostRecentRequestTimeInTicks": "639086846831371443",
}


def get_issuer(cusip: str) -> dict:
    cusip = cusip.strip().upper()

    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(COOKIES)

    resp = session.get(
        f"{EMMA_BASE}/QuickSearch/Results",
        params={"quickSearchText": cusip},
        allow_redirects=True,
        timeout=15,
    )
    resp.raise_for_status()

    if "Security/Details" not in resp.url:
        raise ValueError(
            f"CUSIP '{cusip}' did not resolve to a Security Details page.\n"
            f"  Landed on: {resp.url}"
        )

    soup = BeautifulSoup(resp.text, "html.parser")
    issue_link = soup.find("a", href=lambda h: h and "/IssueView/Details/" in h)

    if not issue_link:
        raise ValueError(
            f"Could not find issuer.\n"
            f"  URL: {resp.url}\n"
            f"  HTML length: {len(resp.text)} chars (should be ~260KB if session is valid)"
        )

    h3 = issue_link.find("h3")
    h5 = issue_link.find("h5")

    return {
        "cusip": cusip,
        "issuer": h3.get_text(strip=True) if h3 else "Unknown",
        "abbreviated": h5.get_text(strip=True) if h5 else "",
        "emma_url": resp.url,
    }


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python emma_issuer.py <CUSIP>")
        sys.exit(1)

    try:
        r = get_issuer(sys.argv[1])
        print(f"CUSIP       : {r['cusip']}")
        print(f"Issuer      : {r['issuer']}")
        if r["abbreviated"]:
            print(f"Abbreviated : {r['abbreviated']}")
        print(f"EMMA URL    : {r['emma_url']}")
    except requests.exceptions.HTTPError as e:
        print(f"Error: HTTP {e.response.status_code}")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
