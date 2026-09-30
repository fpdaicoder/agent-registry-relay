"""Error type shared by the group chat data plane."""

from __future__ import annotations


class GroupChatError(Exception):
    """A group chat operation that must surface as a structured HTTP error."""

    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
