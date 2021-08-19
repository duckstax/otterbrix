#!/usr/bin/env python

import asyncio
import websockets
import msgpack
import uuid



async def hello():
    uri = "ws://localhost:9999"
    async with websockets.connect(uri) as websocket:
        data = msgpack.packb([str(uuid.uuid4()),2,["1qaz",["key"]]], use_bin_type=True)


        await websocket.send(data)
        print(f"> {data}")

        greeting = await websocket.recv()
        print(f"< {greeting}")


asyncio.get_event_loop().run_until_complete(hello())
