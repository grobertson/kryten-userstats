"""Main entry point for userstats module."""

import asyncio

from .main import main as async_main


def main():
    """CLI entry point that properly handles async main."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
