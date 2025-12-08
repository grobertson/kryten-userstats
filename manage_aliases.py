"""Utility script to manage username aliases and kudos trigger phrases."""

import asyncio
import json
import sys
from pathlib import Path

from userstats.database import StatsDatabase
import logging


async def load_aliases_from_file(db: StatsDatabase, aliases_file: str):
    """Load aliases from JSON file into database."""
    with open(aliases_file) as f:
        data = json.load(f)
        
    aliases = data.get("aliases", {})
    phrases = data.get("kudos_phrases", [])
    
    # Load aliases
    for username, alias_list in aliases.items():
        for alias in alias_list:
            await db.add_user_alias(username, alias)
            print(f"Added alias: {username} <- {alias}")
            
    # Load kudos phrases
    for phrase in phrases:
        await db.add_trigger_phrase(phrase)
        print(f"Added kudos phrase: {phrase}")
        
    print(f"\nLoaded {sum(len(v) for v in aliases.values())} aliases for {len(aliases)} users")
    print(f"Loaded {len(phrases)} kudos trigger phrases")


async def list_aliases(db: StatsDatabase):
    """List all username aliases."""
    # Query all unique usernames with aliases
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT username FROM user_aliases ORDER BY username")
    usernames = [row[0] for row in cursor.fetchall()]
    
    print("\n=== Username Aliases ===\n")
    for username in usernames:
        aliases = await db.get_user_aliases(username)
        print(f"{username}: {', '.join(aliases)}")
        
    conn.close()


async def list_phrases(db: StatsDatabase):
    """List all kudos trigger phrases."""
    phrases = await db.get_trigger_phrases()
    
    print("\n=== Kudos Trigger Phrases ===\n")
    for phrase in sorted(phrases):
        print(f"  - {phrase}")
        
    print(f"\nTotal: {len(phrases)} phrases")


async def add_alias_interactive(db: StatsDatabase):
    """Interactively add a username alias."""
    username = input("Enter username: ").strip()
    alias = input("Enter alias: ").strip()
    
    if username and alias:
        await db.add_user_alias(username, alias)
        print(f"Added: {username} <- {alias}")
    else:
        print("Both username and alias are required")


async def add_phrase_interactive(db: StatsDatabase):
    """Interactively add a kudos trigger phrase."""
    phrase = input("Enter kudos trigger phrase: ").strip()
    
    if phrase:
        await db.add_trigger_phrase(phrase)
        print(f"Added kudos phrase: {phrase}")
    else:
        print("Phrase cannot be empty")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Manage user aliases and kudos phrases")
    parser.add_argument("--db", default="data/userstats.db", help="Database path")
    parser.add_argument("--load", help="Load aliases from JSON file")
    parser.add_argument("--list-aliases", action="store_true", help="List all aliases")
    parser.add_argument("--list-phrases", action="store_true", help="List kudos phrases")
    parser.add_argument("--add-alias", action="store_true", help="Add alias interactively")
    parser.add_argument("--add-phrase", action="store_true", help="Add phrase interactively")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger(__name__)
    
    # Initialize database
    db = StatsDatabase(args.db, logger)
    await db.initialize()
    
    # Execute command
    if args.load:
        await load_aliases_from_file(db, args.load)
    elif args.list_aliases:
        await list_aliases(db)
    elif args.list_phrases:
        await list_phrases(db)
    elif args.add_alias:
        await add_alias_interactive(db)
    elif args.add_phrase:
        await add_phrase_interactive(db)
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
