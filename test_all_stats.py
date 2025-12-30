#!/usr/bin/env python3
"""Test the channel.all_stats command."""

import asyncio
import json

from nats.aio.client import Client as NatsClient


async def test():
    nc = NatsClient()
    await nc.connect("nats://localhost:4222")

    req = {
        "service": "userstats",
        "command": "channel.all_stats",
        # Don't specify channel/domain - let service use defaults
        "limits": {
            "top_users": 3,
            "media_history": 3,
            "leaderboards": 3
        }
    }

    try:
        resp = await nc.request(
            "kryten.userstats.command",
            json.dumps(req).encode(),
            timeout=5.0
        )
        result = json.loads(resp.data.decode())
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(test())
