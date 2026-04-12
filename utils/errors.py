"""
Custom exceptions for the SRMA engine.
This module imports nothing from the project.
All other modules import exceptions from here.
"""


class CostCapExceededError(Exception):
    """Raised when per-PDF cost exceeds config.COST_HARD_STOP_USD."""
    pass


class SchemaCycleError(Exception):
    """Raised by template_manager.py when a cycle is detected in the
    variable dependency graph. Caught and displayed by app.py."""
    pass


class PDFUnreadableError(Exception):
    """Reserved for internal testing/helper paths only.
    Normal batch handling for unreadable PDFs must log and skip without raising.
    Do not use in production control flow."""
    pass
