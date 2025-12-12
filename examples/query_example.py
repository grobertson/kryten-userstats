#!/usr/bin/env python3
"""Example script demonstrating NATS query endpoint usage with unified command pattern."""

import asyncio
import json

from nats.aio.client import Client as NatsClient


async def query_example():
    """Demonstrate various query endpoints using kryten.userstats.command."""

    nc = NatsClient()
    await nc.connect("nats://localhost:4222")

    print("=" * 60)
    print("Kryten User Statistics - Query Examples")
    print("=" * 60)

    subject = "kryten.userstats.command"

    # System stats
    print("\n1. System Statistics:")
    request = {"service": "userstats", "command": "system.stats"}
    response = await nc.request(subject, json.dumps(request).encode(), timeout=5.0)
    result = json.loads(response.data.decode())
    print(json.dumps(result, indent=2))

    # System health
    print("\n2. System Health:")
    request = {"service": "userstats", "command": "system.health"}
    response = await nc.request(subject, json.dumps(request).encode(), timeout=5.0)
    result = json.loads(response.data.decode())
    print(json.dumps(result, indent=2))

    # Top message senders
    print("\n3. Top Message Senders (420grindhouse):")
    request = {"service": "userstats", "command": "channel.top_users", "channel": "420grindhouse", "limit": 5}
    response = await nc.request(subject, json.dumps(request).encode(), timeout=5.0)
    result = json.loads(response.data.decode())
    print(json.dumps(result, indent=2))

    # Global message leaderboard
    print("\n4. Global Message Leaderboard:")
    request = {"service": "userstats", "command": "leaderboard.messages", "limit": 5}
    response = await nc.request(subject, json.dumps(request).encode(), timeout=5.0)
    result = json.loads(response.data.decode())
    print(json.dumps(result, indent=2))

    # Most used emotes
    print("\n5. Most Used Emotes:")
    request = {"service": "userstats", "command": "leaderboard.emotes", "limit": 5}
    response = await nc.request(subject, json.dumps(request).encode(), timeout=5.0)
    result = json.loads(response.data.decode())
    print(json.dumps(result, indent=2))

    # Example user stats (replace 'foo' with an actual username)
    print("\n6. User Statistics (example - may not have data):")
    request = {"service": "userstats", "command": "user.stats", "username": "foo", "channel": "420grindhouse"}
    response = await nc.request(subject, json.dumps(request).encode(), timeout=5.0)
    result = json.loads(response.data.decode())
    print(json.dumps(result, indent=2))

    await nc.close()

    print("\n" + "=" * 60)
    print("Queries completed!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(query_example())
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure:")
        print("  1. NATS server is running")
        print("  2. kryten-userstats is running")
        print("  3. Some data has been collected")
