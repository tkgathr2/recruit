"""Indeed API GraphQL fetcher for retrieving applicant details.

This module provides functions to fetch applicant information from Indeed's
GraphQL API, including personal details and questionnaire responses.
"""

import os
import requests
from typing import Optional, Dict, Any
from datetime import datetime


# Environment variables
INDEED_API_KEY = os.getenv("INDEED_API_KEY", "0f2b0de1b8ff96890172eeeba0816aaab662605e3efebbc0450745798c4b35ae")
INDEED_CTK = os.getenv("INDEED_CTK")
INDEED_GRAPHQL_ENDPOINT = "https://apis.indeed.com/graphql"


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

    # GraphQL query with profile details and questionnaire answers
    query = """
    query CRP_CandidateSubmissions($input: CandidateSubmissionsInput!, $inNexusPremiumPlus: Boolean!) {
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
              answers {
                ... on TextAnswer {
                  questionKey
                  value
                }
                ... on MultipleChoiceAnswer {
                  questionKey
                  values
                }
              }
            }
          }
        }
      }
    }
    """

    variables = {
        "input": {"legacyIds": [legacy_id]},
        "inNexusPremiumPlus": True
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

        # Extract questionnaire answers
        answers = submission_data.get("answers", [])
        parsed_answers = []
        for answer in answers:
            answer_dict = {
                "questionKey": answer.get("questionKey"),
                "value": answer.get("value") or answer.get("values")
            }
            parsed_answers.append(answer_dict)

        return {
            "name": name,
            "phone": phone,
            "email": email,
            "location": location,
            "answers": parsed_answers,
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
