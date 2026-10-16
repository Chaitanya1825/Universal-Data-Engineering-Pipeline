"""
Universal Data Engineering Pipeline - Dashboard Server Entrypoint.

Starts the interactive web analytics dashboard with file upload,
live pipeline orchestration, dynamic charts, and Power BI semantic integration.
"""

import argparse
from pathlib import Path
import sys

from dashboard.app import create_app, load_dashboard_config
from pipeline.database import get_database_url, load_project_env, mask_database_url


def main(argv=None) -> int:
    """Run dashboard web server with command-line arguments."""
    root_dir = Path(__file__).resolve().parent
    load_project_env(root_dir / ".env")

    cfg = load_dashboard_config(root_dir / "config" / "dashboard.json")

    parser = argparse.ArgumentParser(description="Universal Pipeline Interactive Dashboard")
    parser.add_argument("--host", default=cfg.get("host", "127.0.0.1"), help="Host IP address to bind server")
    parser.add_argument("--port", type=int, default=cfg.get("port", 5000), help="Port number for web server")
    parser.add_argument("--debug", action="store_true", default=cfg.get("debug", False), help="Run in debug mode")

    args = parser.parse_args(argv)

    db_url = get_database_url(dotenv_path=root_dir / ".env")
    masked_db = mask_database_url(db_url)

    print("\n" + "=" * 60)
    print(" UNIVERSAL DATA ENGINEERING PIPELINE - WEB DASHBOARD")
    print("=" * 60)
    print(f" Web Interface:    http://{args.host}:{args.port}/")
    print(f" Database Target:  {masked_db}")
    print(f" Landing Zone:     {root_dir / 'data' / 'raw'}")
    print("=" * 60)
    print(" Press Ctrl+C to stop the dashboard server.\n")

    app = create_app(root_dir=root_dir)
    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0


if __name__ == "__main__":
    sys.exit(main())
