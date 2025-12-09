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
    request = {
        "service": "userstats",
        "command": "user.stats",
        "username": args.username
    }
    if args.channel:
        request["channel"] = args.channel
    
    subject = "kryten.userstats.command"
    result = await query_nats(subject, request)
    
    if result.get("success"):
        print(json.dumps(result["data"], indent=2))
    else:
        print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
        sys.exit(1)


async def cmd_leaderboard(args):
    """Query leaderboards."""
    request = {
        "service": "userstats",
        "command": f"leaderboard.{args.type}",
        "limit": args.limit
    }
    
    subject = "kryten.userstats.command"
    result = await query_nats(subject, request)
    
    if result.get("success"):
        print(json.dumps(result["data"], indent=2))
    else:
        print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
        sys.exit(1)
        return
    subject = "kryten.userstats.command"
    result = await query_nats(subject, request)
    
    if not result.get("success"):
        print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
        sys.exit(1)
    
    data = result["data"]
    
    # Pretty print leaderboard
    print(f"\n{args.type.upper()} LEADERBOARD (Top {args.limit})")
    print("=" * 50)
    for i, entry in enumerate(data, 1):
        if args.type == "emotes":
            print(f"{i:2d}. {entry['emote']:20s} - {entry['count']:,} uses")
        else:
            print(f"{i:2d}. {entry['username']:20s} - {entry['count']:,}")


async def cmd_channel(args):
    """Query channel statistics."""
    subject = "kryten.userstats.command"
    
    if args.query == "top":
        request = {
            "service": "userstats",
            "command": "channel.top_users",
            "channel": args.channel,
            "limit": args.limit
        }
        result = await query_nats(subject, request)
        
        if not result.get("success"):
            print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
            sys.exit(1)
        
        data = result["data"]
        print(f"\nTOP USERS IN #{args.channel} (Top {args.limit})")
        print("=" * 50)
        for i, user in enumerate(data, 1):
            print(f"{i:2d}. {user['username']:20s} - {user['count']:,} messages")
            
    elif args.query == "population":
        request = {
            "service": "userstats",
            "command": "channel.population",
            "channel": args.channel,
            "hours": args.hours
        }
        result = await query_nats(subject, request)
        
        if result.get("success"):
            print(json.dumps(result["data"], indent=2))
        else:
            print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
            sys.exit(1)
        
    elif args.query == "media":
        request = {
            "service": "userstats",
            "command": "channel.media_history",
            "channel": args.channel,
            "limit": args.limit
        }
        result = await query_nats(subject, request)
        
        if not result.get("success"):
            print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
            sys.exit(1)
        
        data = result["data"]
        print(f"\nRECENT MEDIA IN #{args.channel} (Last {args.limit})")
        print("=" * 50)
        for i, media in enumerate(data, 1):
            print(f"{i:2d}. [{media['type']:2s}] {media['title']}")
            print(f"    {media['timestamp']}")


async def cmd_system(args):
    """Query system statistics."""
    subject = "kryten.userstats.command"
    
    if args.query == "stats":
        request = {
            "service": "userstats",
            "command": "system.stats"
        }
        result = await query_nats(subject, request)
        
        if not result.get("success"):
            print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
            sys.exit(1)
        
        data = result["data"]
        print("\nSYSTEM STATISTICS")
        print("=" * 50)
        print(f"Total Users:        {data.get('total_users', 0):,}")
        print(f"Total Messages:     {data.get('total_messages', 0):,}")
        print(f"Total PMs:          {data.get('total_pms', 0):,}")
        print(f"Total Kudos:        {data.get('total_kudos', 0):,}")
        print(f"Total Emotes:       {data.get('total_emotes', 0):,}")
        print(f"Total Media:        {data.get('total_media_changes', 0):,}")
        print(f"Active Sessions:    {data.get('active_sessions', 0):,}")
        
    elif args.query == "health":
        request = {
            "service": "userstats",
            "command": "system.health"
        }
        result = await query_nats(subject, request)
        
        if not result.get("success"):
            print(f"Error: {result.get('error', 'Unknown error')}", file=sys.stderr)
            sys.exit(1)
        
        data = result["data"]
        print("\nSYSTEM HEALTH")
        print("=" * 50)
        print(f"Service:            {data.get('service', 'unknown')}")
        print(f"Status:             {data.get('status', 'unknown')}")
        print(f"Database:           {'✓' if data.get('database_connected') else '✗'}")
        print(f"NATS:               {'✓' if data.get('nats_connected') else '✗'}")


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
