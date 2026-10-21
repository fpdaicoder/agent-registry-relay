"""Error type shared by the personal task-continuation data plane."""

from __future__ import annotations


class TaskError(Exception):
    """A task operation that must surface as a structured HTTP error."""

    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message