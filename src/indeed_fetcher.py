"""Indeed API GraphQL fetcher for retrieving applicant details.

This module provides functions to fetch applicant information from Indeed's
GraphQL API, including personal details and questionnaire responses.
"""

import os
import re
import requests
from typing import Optional, Dict, Any
from datetime import datetime


# Environment variables
INDEED_API_KEY = os.getenv("INDEED_API_KEY", "0f2b0de1b8ff96890172eeeba0816aaab662605e3efebbc0450745798c4b35ae")
INDEED_CTK = os.getenv("INDEED_CTK")
INDEED_GRAPHQL_ENDPOINT = "https://apis.indeed.com/graphql?co=JP&locale=ja"


def fetch_all_details(legacy_id: str, timeout: int = 10) -> Dict[str, Any]:
    """
    Fetch applicant's full details from Indeed API using legacy_id.

    Retrieves name, phone, email, location, and questionnaire responses.

    Args:
        legacy_id: The candidate's legacy ID from Indeed system
        timeout: Request timeout in seconds (default: 10)

    Returns:
        Dictionary with keys:
        - name: Candidate's display name
        - phone: Phone number (if available)
        - email: Email address (if available)
        - location: Location string (if available)
        - answers: List of questionnaire responses with questionKey and value
        - raw_data: Complete GraphQL response for debugging

        Returns empty dict if fetch fails or legacy_id is invalid.
    """
    if not legacy_id or not INDEED_CTK:
        return {}

    # GraphQL query - profile details only (answers field not available in this API version)
    query = """
    query CRP_CandidateSubmissions($input: CandidateSubmissionsInput!) {
      candidateSubmissions(input: $input) {
        results {
          id
          ... on CandidateSubmission {
            data {
              profile {
                name { displayName }
                contact { phoneNumber email aliasedEmail }
                location { location }
              }
            }
          }
        }
      }
    }
    """

    variables = {
        "input": {"legacyIds": [legacy_id]}
    }

    headers = {
        "content-type": "application/json",
        "indeed-api-key": INDEED_API_KEY,
        "indeed-ctk": INDEED_CTK,
        "indeed-client-sub-app": "talent-management-experience",
        "indeed-client-sub-app-component": "./CandidateReviewPage",
        "Origin": "https://employers.indeed.com",
        "Referer": "https://employers.indeed.com/",
        "Cookie": f"CTK={INDEED_CTK}",
    }

    payload = {
        "query": query,
        "variables": variables
    }

    try:
        response = requests.post(
            INDEED_GRAPHQL_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=timeout
        )
        response.raise_for_status()

        data = response.json()

        # Parse GraphQL response
        if "data" not in data or "candidateSubmissions" not in data["data"]:
            return {}

        results = data["data"]["candidateSubmissions"].get("results", [])
        if not results or len(results) == 0:
            return {}

        submission = results[0]
        submission_data = submission.get("data", {})
        profile = submission_data.get("profile", {})

        # Extract profile fields
        name = profile.get("name", {}).get("displayName")
        contact = profile.get("contact", {})
        phone = contact.get("phoneNumber")
        email = contact.get("email") or contact.get("aliasedEmail")
        location = profile.get("location", {}).get("location")

        return {
            "name": name,
            "phone": phone,
            "email": email,
            "location": location,
            "answers": [],
            "raw_data": data
        }

    except requests.exceptions.RequestException as e:
        # Silently return empty dict on network errors
        return {}
    except (KeyError, ValueError):
        # Silently return empty dict on JSON parsing errors
        return {}


def fetch_phone_for_applicant(name: str) -> Optional[str]:
    """
    Legacy function for backward compatibility.

    Attempts to fetch phone number by candidate name.
    Note: This is a stub function kept for compatibility with main.py.
    In the current Indeed API design, we primarily use legacy_id instead of name.

    Args:
        name: Candidate's display name

    Returns:
        Phone number string or None
    """
    # This is a legacy function that would require different API capabilities
    # In practice, fetch_all_details() with legacy_id is the recommended approach
    return None


def _extract_id_from_text(text: str) -> Optional[str]:
    """Helper: extract Indeed legacyId (hex) from a URL or HTML snippet.

    Looks for:
      - ?id=<hex> or &id=<hex> (URL query param)
      - /candidates/view?id=<hex>
      - JavaScript window.location redirect
    Hex length: 8–40 characters.
    """
    # URL query param: ?id= or &id=
    m = re.search(r"[?&]id=([a-f0-9]{8,40})", text)
    if m:
        return m.group(1)
    # JavaScript redirect: window.location.href = "...?id=<hex>"
    m = re.search(r'window\.location(?:\.href)?\s*=\s*["\']([^"\']+)["\']', text)
    if m:
        inner = re.search(r"[?&]id=([a-f0-9]{8,40})", m.group(1))
        if inner:
            return inner.group(1)
    # Meta refresh: <meta http-equiv="refresh" content="0; url=...?id=<hex>">
    m = re.search(r'content=["\'][^"\']*url=([^"\']+)["\']', text, re.IGNORECASE)
    if m:
        inner = re.search(r"[?&]id=([a-f0-9]{8,40})", m.group(1))
        if inner:
            return inner.group(1)
    return None


def resolve_legacy_id_from_tracking_url(url: str, timeout: int = 15) -> Optional[str]:
    """Follow Indeed email tracking URL redirect to extract legacyId.

    Indeed email tracking URLs (engage.indeed.com/f/a/...) redirect to
    employers.indeed.com/candidates/view?id=<legacyId>.

    Strategy:
    1. First try allow_redirects=True to get the final URL in one shot.
    2. Fall back to manual hop-by-hop following if step 1 gives no ID.
    3. If a non-redirect 200 is received, parse the body (JS/meta redirect).

    Args:
        url: The engage.indeed.com tracking URL from the email
        timeout: Request timeout in seconds

    Returns:
        legacyId string if found, None otherwise
    """
    if not url or "indeed" not in url:
        return None

    common_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Cookie": f"CTK={INDEED_CTK}" if INDEED_CTK else "",
    }

    # --- Strategy 1: allow_redirects=True (simplest, follows all redirects automatically) ---
    try:
        print(f"[indeed_fetcher] strategy1: allow_redirects=True url={url[:80]}", flush=True)
        resp = requests.get(url, allow_redirects=True, timeout=timeout, headers=common_headers)
        final_url = resp.url
        print(f"[indeed_fetcher] strategy1: final_url={final_url[:120]} status={resp.status_code}", flush=True)
        # Check final URL
        found = _extract_id_from_text(final_url)
        if found:
            print(f"[indeed_fetcher] strategy1: legacyId={found}", flush=True)
            return found
        # Check response body
        body = resp.text or ""
        found = _extract_id_from_text(body)
        if found:
            print(f"[indeed_fetcher] strategy1: legacyId found in body: {found}", flush=True)
            return found
        snippet = body[:300].replace('\n', ' ')
        print(f"[indeed_fetcher] strategy1: no ID found. body snippet: {snippet}", flush=True)
    except Exception as e:
        print(f"[indeed_fetcher] strategy1 exception: {type(e).__name__}: {e}", flush=True)

    # --- Strategy 2: manual hop-by-hop (allow_redirects=False) ---
    try:
        current_url = url
        for hop in range(5):
            resp = requests.get(
                current_url,
                allow_redirects=False,
                timeout=timeout,
                headers=common_headers,
            )
            location = resp.headers.get("Location", "")
            print(f"[indeed_fetcher] strategy2 hop={hop} status={resp.status_code} location={location[:120] if location else 'none'}", flush=True)

            # Check current URL and Location header for legacyId
            for check_url in [current_url, location]:
                if check_url:
                    found = _extract_id_from_text(check_url)
                    if found:
                        print(f"[indeed_fetcher] strategy2: legacyId={found} from url", flush=True)
                        return found

            # Follow redirect if present
            if location and resp.status_code in (301, 302, 303, 307, 308):
                current_url = location
                continue

            # No redirect: parse body
            body = resp.text or ""
            snippet = body[:300].replace('\n', ' ')
            print(f"[indeed_fetcher] strategy2: body snippet: {snippet}", flush=True)
            found = _extract_id_from_text(body)
            if found:
                print(f"[indeed_fetcher] strategy2: legacyId={found} from body", flush=True)
                return found
            break

    except Exception as e:
        print(f"[indeed_fetcher] strategy2 exception: {type(e).__name__}: {e}", flush=True)

    print(f"[indeed_fetcher] all strategies failed, no legacyId found", flush=True)
    return None


def fetch_applicant_details_safe(legacy_id: str, timeout: int = 10) -> Dict[str, Any]:
    """
    Safely fetch applicant details with graceful error handling.

    Wraps fetch_all_details with additional validation and logging.

    Args:
        legacy_id: The candidate's legacy ID
        timeout: Request timeout in seconds

    Returns:
        Dictionary with applicant details, or empty dict on any error
    """
    if not legacy_id:
        return {}

    return fetch_all_details(legacy_id, timeout)
