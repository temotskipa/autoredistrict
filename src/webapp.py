"""Run the local autoredistrict web application."""

from __future__ import annotations

import argparse

import uvicorn


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the local autoredistrict web app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args(argv)

    uvicorn.run(
        "src.api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
