"""
Universal Data Engineering Pipeline - Web Dashboard Package.
"""

from dashboard.generator import DashboardGenerator
from dashboard.app import create_app

__all__ = ["DashboardGenerator", "create_app"]
