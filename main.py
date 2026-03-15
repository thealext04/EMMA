#!/usr/bin/env python3
"""
emma_issuer.py — Returns the issuing body name for a given CUSIP from EMMA.

Usage:
    python emma_issuer.py <CUSIP>
    python emma_issuer.py 917542KT2

Requirements:
    pip install requests
"""

import sys
import requests

EMMA_API = "https://emma.msrb.org/Security/SecurityDetails.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


def get_issuer_name(cusip: str) -> str:
    """Query EMMA for the issuer name associated with a CUSIP."""
    cusip = cusip.strip().upper()

    # EMMA's security detail endpoint — returns JSON with issuer metadata
    url = f"https://emma.msrb.org/Security/SecurityDetails"
    params = {"cusip": cusip}

    response = requests.get(url, params=params, headers=HEADERS, timeout=10)
    response.raise_for_status()

    data = response.json()

    # The issuer name lives at the top level of the response
    issuer = data.get("IssuerName") or data.get("issuerName") or data.get("Issuer")

    if not issuer:
        raise ValueError(f"No issuer found in response for CUSIP {cusip}. "
                         f"Response keys: {list(data.keys())}")

    return issuer


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python emma_issuer.py <CUSIP>")
        print("Example: python emma_issuer.py 917542KT2")
        sys.exit(1)

    cusip = sys.argv[1]

    try:
        issuer = get_issuer_name(cusip)
        print(f"CUSIP:  {cusip.upper()}")
        print(f"Issuer: {issuer}")

    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to EMMA. Check your internet connection.")
        sys.exit(1)

    except requests.exceptions.HTTPError as e:
        print(f"Error: EMMA returned HTTP {e.response.status_code} for CUSIP {cusip}.")
        print("The CUSIP may not exist on EMMA, or the endpoint may have changed.")
        sys.exit(1)

    except ValueError as e:
        print(f"Error: {e}")
        print("Run with --debug to see the raw response.")
        sys.exit(1)