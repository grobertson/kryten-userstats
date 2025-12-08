#!/usr/bin/env python3
"""CLI tool for querying kryten-userstats via NATS."""

import asyncio
import json
import sys
import argparse
from nats.aio.client import Client as NATS


async def query_nats(subject: str, request: dict, timeout: float = 5.0) -> dict:
    """Send NATS request and return response."""
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    try:
        response = await nc.request(
            subject,
            json.dumps(request).encode(),
            timeout=timeout
        )
        result = json.loads(response.data.decode())
        return result
    finally:
        await nc.close()


async def cmd_user(args):
    """Query user statistics."""
    request = {"username": args.username}
    if args.channel:
        request["channel"] = args.channel
    
    subject = f"cytube.query.userstats.{args.domain}.user.stats"
    result = await query_nats(subject, request)
    print(json.dumps(result, indent=2))


async def cmd_leaderboard(args):
    """Query leaderboards."""
    request = {"limit": args.limit}
    
    if args.type == "messages":
        subject = f"cytube.query.userstats.{args.domain}.leaderboard.messages"
    elif args.type == "kudos":
        subject = f"cytube.query.userstats.{args.domain}.leaderboard.kudos"
    elif args.type == "emotes":
        subject = f"cytube.query.userstats.{args.domain}.leaderboard.emotes"
    else:
        print(f"Unknown leaderboard type: {args.type}")
        return
    
    result = await query_nats(subject, request)
    
    # Pretty print leaderboard
    print(f"\n{args.type.upper()} LEADERBOARD (Top {args.limit})")
    print("=" * 50)
    for i, entry in enumerate(result.get("leaderboard", []), 1):
        if args.type == "emotes":
            print(f"{i:2d}. {entry['emote']:20s} - {entry['count']:,} uses")
        else:
            print(f"{i:2d}. {entry['username']:20s} - {entry['count']:,}")


async def cmd_channel(args):
    """Query channel statistics."""
    if args.query == "top":
        request = {"channel": args.channel, "limit": args.limit}
        subject = f"cytube.query.userstats.{args.domain}.channel.top_users"
        result = await query_nats(subject, request)
        
        print(f"\nTOP USERS IN #{args.channel} (Top {args.limit})")
        print("=" * 50)
        for i, user in enumerate(result.get("top_users", []), 1):
            print(f"{i:2d}. {user['username']:20s} - {user['count']:,} messages")
            
    elif args.query == "population":
        request = {"channel": args.channel, "hours": args.hours}
        subject = f"cytube.query.userstats.{args.domain}.channel.population"
        result = await query_nats(subject, request)
        print(json.dumps(result, indent=2))
        
    elif args.query == "media":
        request = {"channel": args.channel, "limit": args.limit}
        subject = f"cytube.query.userstats.{args.domain}.channel.media_history"
        result = await query_nats(subject, request)
        
        print(f"\nRECENT MEDIA IN #{args.channel} (Last {args.limit})")
        print("=" * 50)
        for i, media in enumerate(result.get("media_history", []), 1):
            print(f"{i:2d}. [{media['type']:2s}] {media['title']}")
            print(f"    {media['timestamp']}")


async def cmd_system(args):
    """Query system statistics."""
    if args.query == "stats":
        subject = f"cytube.query.userstats.{args.domain}.system.stats"
        result = await query_nats(subject, {})
        
        print("\nSYSTEM STATISTICS")
        print("=" * 50)
        print(f"Total Users:        {result.get('total_users', 0):,}")
        print(f"Total Messages:     {result.get('total_messages', 0):,}")
        print(f"Total PMs:          {result.get('total_pms', 0):,}")
        print(f"Total Kudos:        {result.get('total_kudos', 0):,}")
        print(f"Total Emotes:       {result.get('total_emotes', 0):,}")
        print(f"Total Media:        {result.get('total_media_changes', 0):,}")
        print(f"Active Sessions:    {result.get('active_sessions', 0):,}")
        
    elif args.query == "health":
        subject = f"cytube.query.userstats.{args.domain}.system.health"
        result = await query_nats(subject, {})
        
        print("\nSYSTEM HEALTH")
        print("=" * 50)
        print(f"Service:            {result.get('service', 'unknown')}")
        print(f"Status:             {result.get('status', 'unknown')}")
        print(f"Database:           {'✓' if result.get('database_connected') else '✗'}")
        print(f"NATS:               {'✓' if result.get('nats_connected') else '✗'}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Query kryten-userstats via NATS")
    parser.add_argument("--domain", default="cytu.be", help="CyTube domain")
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # User command
    user_parser = subparsers.add_parser("user", help="Query user statistics")
    user_parser.add_argument("username", help="Username to query")
    user_parser.add_argument("--channel", help="Specific channel (optional)")
    user_parser.set_defaults(func=cmd_user)
    
    # Leaderboard command
    lb_parser = subparsers.add_parser("leaderboard", help="Query leaderboards")
    lb_parser.add_argument("type", choices=["messages", "kudos", "emotes"], help="Leaderboard type")
    lb_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    lb_parser.set_defaults(func=cmd_leaderboard)
    
    # Channel command
    ch_parser = subparsers.add_parser("channel", help="Query channel statistics")
    ch_parser.add_argument("query", choices=["top", "population", "media"], help="Query type")
    ch_parser.add_argument("channel", help="Channel name")
    ch_parser.add_argument("--limit", type=int, default=10, help="Number of results")
    ch_parser.add_argument("--hours", type=int, default=24, help="Hours of history (population)")
    ch_parser.set_defaults(func=cmd_channel)
    
    # System command
    sys_parser = subparsers.add_parser("system", help="Query system statistics")
    sys_parser.add_argument("query", choices=["stats", "health"], help="Query type")
    sys_parser.set_defaults(func=cmd_system)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        asyncio.run(args.func(args))
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        print("\nMake sure:", file=sys.stderr)
        print("  1. NATS server is running", file=sys.stderr)
        print("  2. kryten-userstats is running", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
