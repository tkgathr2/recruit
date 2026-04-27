"""Indeed API GraphQL fetcher for retrieving applicant details. This module provides functions to fetch applicant information from Indeed's GraphQL API, including personal details and questionnaire responses. """
import os
import re
import requests
from typing import Optional, Dict, Any
from datetime import datetime

# Environment variables
INDEED_API_KEY = os.getenv("INDEED_API_KEY", "0f2b0de1b8ff96890172eeeba0816aaab662605e3efebbc0450745798c4b35ae")
INDEED_GRAPHQL_ENDPOINT = "https://apis.indeed.com/graphql?co=JP&locale=ja"

# CTK override file path (Webフォームからの更新を再デプロイなしで反映するため)
_CTK_FILE = os.path.join(os.getenv("LOG_DIR", "/tmp"), "indeed_ctk_override.txt")


def get_ctk() -> str:
    """CTKを取得する。Webフォームで更新されたファイルを優先し、なければ環境変数を使う。"""
    try:
        if os.path.exists(_CTK_FILE):
            with open(_CTK_FILE, "r") as f:
                val = f.read().strip()
                if val:
                    return val
    except Exception:
        pass
    return os.getenv("INDEED_CTK", "")


# --- CTK Expiry Detection (GLOBAL STATE) ---
_ctk_expired = False


def is_ctk_expired() -> bool:
    """CTKが期限切れかどうかを返す。"""
    return _ctk_expired


def reset_ctk_expired() -> None:
    """CTK期限切れフラグをリセットする（CTK更新後に呼び出す）。"""
    global _ctk_expired
    _ctk_expired = False


def fetch_all_details(legacy_id: str, timeout: int = 5) -> Dict[str, Any]:
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
    global _ctk_expired
    ctk = get_ctk()
    if not legacy_id or not ctk:
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
        "indeed-ctk": ctk,
        "indeed-client-sub-app": "talent-management-experience",
        "indeed-client-sub-app-component": "./CandidateReviewPage",
        "Origin": "https://employers.indeed.com",
        "Referer": "https://employers.indeed.com/",
        "Cookie": f"CTK={ctk}",
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
        # CTK期限切れ検知: HTTP 401/403 は認証エラーの明確なサイン
        if response.status_code in (401, 403):
            _ctk_expired = True
            print(f"[indeed_fetcher] CTK期限切れ検知 (HTTP {response.status_code})", flush=True)
            return {}
        response.raise_for_status()
        data = response.json()
        # CTK期限切れ検知: GraphQL errors に認証関連メッセージが含まれる場合
        errors = data.get("errors", [])
        if errors:
            for error in errors:
                msg = str(error.get("message", "")).lower()
                if any(kw in msg for kw in [
                    "unauthorized", "unauthenticated", "authentication",
                    "forbidden", "invalid token", "expired", "ctk"
                ]):
                    _ctk_expired = True
                    print(f"[indeed_fetcher] CTK期限切れ検知 (GraphQL error: {error.get('message')})", flush=True)
                    return {}
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
    except requests.exceptions.HTTPError as e:
        # HTTPErrorのステータスコードでもCTK期限切れを検知
        if hasattr(e, 'response') and e.response is not None and e.response.status_code in (401, 403):
            _ctk_expired = True
            print(f"[indeed_fetcher] CTK期限切れ検知 (HTTPError {e.response.status_code})", flush=True)
        return {}
    except requests.exceptions.RequestException:
        # Silently return empty dict on network errors
        return {}
    except (KeyError, ValueError):
        # Silently return empty dict on JSON parsing errors
        return {}


def fetch_recent_candidates(limit: int = 30, timeout: int = 5) -> list:
    """Indeed GraphQL APIから最近の応募者一覧を取得する（legacyId不要）。
    engage.indeed.comリダイレクトが使えない場合のフォールバック。
    legacyIdsを指定せずにAPIを叩き、最新の応募者リストを取得する。
    Returns:
        list of dicts: [{name, phone, email, location, legacy_id}, ...]
        空リストの場合はAPI呼び出し失敗または応募者なし
    """
    global _ctk_expired
    ctk = get_ctk()
    if not ctk:
        return []
    # legacyIds を指定しない（全件取得を試みる）
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
    # legacyIdsなし・limit指定で最近の応募者を取得試行
    variables = {
        "input": {}  # legacyIds指定なし → 全件取得を期待
    }
    headers = {
        "content-type": "application/json",
        "indeed-api-key": INDEED_API_KEY,
        "indeed-ctk": ctk,
        "indeed-client-sub-app": "talent-management-experience",
        "indeed-client-sub-app-component": "./CandidateReviewPage",
        "Origin": "https://employers.indeed.com",
        "Referer": "https://employers.indeed.com/",
        "Cookie": f"CTK={ctk}",
    }
    try:
        response = requests.post(
            INDEED_GRAPHQL_ENDPOINT,
            json={"query": query, "variables": variables},
            headers=headers,
            timeout=timeout
        )
        if response.status_code in (401, 403):
            _ctk_expired = True
            print(f"[indeed_fetcher] CTK期限切れ検知 fetch_recent_candidates (HTTP {response.status_code})", flush=True)
            return []
        response.raise_for_status()
        data = response.json()
        print(f"[indeed_fetcher] fetch_recent_candidates response: {str(data)[:300]}", flush=True)
        results = (data.get("data") or {}).get("candidateSubmissions", {}).get("results", [])
        candidates = []
        for submission in results:
            sub_data = submission.get("data", {})
            profile = sub_data.get("profile", {})
            name = profile.get("name", {}).get("displayName")
            contact = profile.get("contact", {})
            phone = contact.get("phoneNumber")
            email = contact.get("email") or contact.get("aliasedEmail")
            location = profile.get("location", {}).get("location")
            legacy_id = submission.get("id")
            if name:
                candidates.append({
                    "name": name,
                    "phone": phone,
                    "email": email,
                    "location": location,
                    "legacy_id": legacy_id,
                })
        print(f"[indeed_fetcher] fetch_recent_candidates: {len(candidates)} candidates found", flush=True)
        return candidates
    except Exception as e:
        print(f"[indeed_fetcher] fetch_recent_candidates exception: {type(e).__name__}: {e}", flush=True)
        return []


def fetch_by_name(name: str, timeout: int = 5) -> Dict[str, Any]:
    """応募者名でIndeed APIを検索して連絡先を取得する。
    engage URLリダイレクトが失敗した場合のフォールバック。
    fetch_recent_candidates()の結果から名前でマッチングする。

    Args:
        name: 応募者の表示名（完全一致または部分一致）
    Returns:
        Dictionary with phone, email, location fields, or empty dict
    """
    if not name:
        return {}
    candidates = fetch_recent_candidates()
    name_lower = name.strip().lower()
    # 完全一致を優先
    for c in candidates:
        if c.get("name", "").strip().lower() == name_lower:
            print(f"[indeed_fetcher] fetch_by_name: exact match for '{name}'", flush=True)
            return c
    # 部分一致
    for c in candidates:
        c_name = c.get("name", "").strip().lower()
        if name_lower in c_name or c_name in name_lower:
            print(f"[indeed_fetcher] fetch_by_name: partial match '{c.get('name')}' for '{name}'", flush=True)
            return c
    print(f"[indeed_fetcher] fetch_by_name: no match for '{name}' among {len(candidates)} candidates", flush=True)
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


def resolve_legacy_id_from_tracking_url(url: str, timeout: int = 5) -> Optional[str]:
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
        "Cookie": f"CTK={get_ctk()}" if get_ctk() else "",
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
        for hop in range(3):
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


def fetch_applicant_details_safe(legacy_id: str, timeout: int = 5) -> Dict[str, Any]:
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
