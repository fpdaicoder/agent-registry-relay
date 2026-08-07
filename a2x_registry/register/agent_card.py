"""Agent Card URL fetching."""

import logging
from urllib.request import urlopen, Request
import json

from .models import AgentCard

logger = logging.getLogger(__name__)


def fetch_agent_card(url: str, timeout: int = 10) -> AgentCard:
    """Fetch an A2A Agent Card from a URL and parse it into an AgentCard model.

    Args:
        url: The URL to fetch the Agent Card from.
        timeout: HTTP request timeout in seconds.

    Returns:
        Parsed AgentCard.

    Raises:
        Exception: If the fetch or parse fails.
    """
    req = Request(url, headers={"Accept": "application/json", "User-Agent": "A2XRegistry/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        # Try UTF-8 first, fall back to declared encoding or latin-1
        charset = resp.headers.get_content_charset() or "utf-8"
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode(charset, errors="replace")
        data = json.loads(text)
    return AgentCard(**data)
