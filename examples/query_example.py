#!/usr/bin/env python3
"""Example script demonstrating NATS query endpoint usage."""

import asyncio
import json
from nats.aio.client import Client as NATS


async def query_example():
    """Demonstrate various query endpoints."""
    
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    print("=" * 60)
    print("Kryten User Statistics - Query Examples")
    print("=" * 60)
    
    # System stats
    print("\n1. System Statistics:")
    response = await nc.request(
        "cytube.query.userstats.cytu.be.system.stats",
        json.dumps({}).encode(),
        timeout=5.0
    )
    stats = json.loads(response.data.decode())
    print(json.dumps(stats, indent=2))
    
    # System health
    print("\n2. System Health:")
    response = await nc.request(
        "cytube.query.userstats.cytu.be.system.health",
        json.dumps({}).encode(),
        timeout=5.0
    )
    health = json.loads(response.data.decode())
    print(json.dumps(health, indent=2))
    
    # Top message senders
    print("\n3. Top Message Senders (420grindhouse):")
    response = await nc.request(
        "cytube.query.userstats.cytu.be.channel.top_users",
        json.dumps({"channel": "420grindhouse", "limit": 5}).encode(),
        timeout=5.0
    )
    top_users = json.loads(response.data.decode())
    print(json.dumps(top_users, indent=2))
    
    # Global message leaderboard
    print("\n4. Global Message Leaderboard:")
    response = await nc.request(
        "cytube.query.userstats.cytu.be.leaderboard.messages",
        json.dumps({"limit": 5}).encode(),
        timeout=5.0
    )
    leaderboard = json.loads(response.data.decode())
    print(json.dumps(leaderboard, indent=2))
    
    # Most used emotes
    print("\n5. Most Used Emotes:")
    response = await nc.request(
        "cytube.query.userstats.cytu.be.leaderboard.emotes",
        json.dumps({"limit": 5}).encode(),
        timeout=5.0
    )
    emotes = json.loads(response.data.decode())
    print(json.dumps(emotes, indent=2))
    
    # Example user stats (replace 'foo' with an actual username)
    print("\n6. User Statistics (example - may not have data):")
    response = await nc.request(
        "cytube.query.userstats.cytu.be.user.stats",
        json.dumps({"username": "foo", "channel": "420grindhouse"}).encode(),
        timeout=5.0
    )
    user_stats = json.loads(response.data.decode())
    print(json.dumps(user_stats, indent=2))
    
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
