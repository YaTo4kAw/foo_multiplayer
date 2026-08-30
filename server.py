from datetime import datetime
import websockets
import asyncio
import tomllib
import tomli_w
import json
import os


class FooMultiplayerServer():
    def __init__(self, host: str, port: int):
        self.connections = set()
        self.cached_session = None
        self.cached_index = None
        self.cached_state = None
        self.cached_volume = None
        self.host = host
        self.port = port

    async def handler(self, websocket):
        self.connections.add(websocket)
        print(f"{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: new connection. total: {len(self.connections)}")

        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    data = {"raw": message}

                self.cached_session = data["session"]
                self.cached_index = data["index"]
                self.cached_state = data["state"]
                self.cached_volume = data["volume"]
                
                print(f"{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: session -> {self.cached_session} | index -> {self.cached_index} | state -> {self.cached_state} | volume -> {self.cached_volume}")

        except websockets.exceptions.ConnectionClosed:
            print(f"{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: connection closed")
        finally:
            self.connections.remove(websocket)
            print(f"{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}: client disconnected. total now: {len(self.connections)}")


    async def broadcast(self, message: dict):
        if self.connections:
            msg = json.dumps(message)
            await asyncio.gather(
                *[ws.send(msg) for ws in self.connections],
                return_exceptions=True
            )


    async def main(self):
        async with websockets.serve(self.handler, self.host, self.port):
            print(f"websocket launched at ws://{self.host}:{self.port}")
            while True:  # rewrite this section
                if self.cached_session and self.cached_index and self.cached_state and self.cached_volume and len(self.connections) > 1:
                    await self.broadcast({
                        "session": self.cached_session,
                        "index": self.cached_index,
                        "state": self.cached_state,
                        "volume": self.cached_volume
                    })
                await asyncio.sleep(0.25)


if __name__ == "__main__":
    if os.path.exists("./server_config.toml"):
        with open("./server_config.toml", "rb") as f:
            config = tomllib.load(f)
            host = config["host"]
            port = config["port"]
    else:
        with open("./server_config.toml", "wb") as f:
            host = input("input server host:\n> ")
            port = int(input("input server port:\n> "))
            tomli_w.dump({"host": host, "port": port}, f)

    fms = FooMultiplayerServer(host, port)
    asyncio.run(fms.main())
