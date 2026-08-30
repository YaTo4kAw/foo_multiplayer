from httpx_sse import aconnect_sse
import websockets
import asyncio
import tomli_w
import tomllib
import random
import httpx
import json
import os


class FooMultiplayerClient():
    def __init__(self, sync_id: str, domain: str, port: int):
        self.sync_id = sync_id
        self.domain = domain
        self.port = port
        self.session_id = random.randint(10000, 99999)
        self._value_changed = asyncio.Event()
        self.wait_for_recieved = False
        self.cached_volume = None
        self.cached_state = None
        self.cached_id = None

    async def foobar_connection(self):
        async with httpx.AsyncClient(timeout=None) as client:
            async with aconnect_sse(client, "GET", "http://localhost:8880/api/query/updates?player=true") as event:
                event.response.raise_for_status()
                if self.cached_id == None: print("foobar connected")

                async for sse in event.aiter_sse():
                    if len(sse.data) > 2:
                        current_id = int(json.loads(sse.data)["player"]["activeItem"]["index"])
                        current_state = json.loads(sse.data)["player"]["playbackState"]
                        current_volume = float(json.loads(sse.data)["player"]["volume"]["value"])
                        if not self.wait_for_recieved:
                            flag = False

                            if self.cached_id != current_id and current_id >= 0:
                                self.cached_id = current_id
                                flag = True

                            if current_state != self.cached_state:
                                self.cached_state = current_state
                                flag = True

                            if current_volume != self.cached_volume:
                                self.cached_volume = current_volume
                                flag = True

                            if flag:
                                self._value_changed.set()
                                self._value_changed.clear()
                        else:
                            if current_id == self.cached_id and current_state == self.cached_state and self.cached_volume == current_volume:
                                self.wait_for_recieved = False


    async def server_connection(self):
        async with websockets.connect(f"ws://{self.domain}:{self.port}/ws") as ws:
            print("server connected")
            print(f"session id: {self.session_id}")

            async def reciever():
                async for message in ws:
                    data = json.loads(message)
                    if data["session"] != self.session_id:
                        r_index = data["index"]
                        r_state = data["state"]
                        r_volume = data["volume"]


                        if r_index != self.cached_id:
                            self.cached_id = r_index
                            self.wait_for_recieved = True
                            await httpx.AsyncClient().post(url=f"http://localhost:8880/api/player/play/p{self.sync_id}/{r_index}")

                        if r_state != self.cached_state:
                            self.cached_state = r_state
                            self.wait_for_recieved = True
                            if r_state == "stopped":
                                await httpx.AsyncClient().post(url=f"http://localhost:8880/api/player/stop")
                            elif r_state == "paused":
                                await httpx.AsyncClient().post(url=f"http://localhost:8880/api/player/pause")
                            elif r_state == "playing":
                                await httpx.AsyncClient().post(url=f"http://localhost:8880/api/player/play")

                        if r_volume != self.cached_volume:
                            self.cached_volume = r_volume
                            self.wait_for_recieved = True
                            await httpx.AsyncClient().post(url=f"http://localhost:8880/api/player", json={"volume": r_volume})

            async def sender():
                while True:
                    await self._value_changed.wait()
                    await ws.send(json.dumps({"session": self.session_id, "index": self.cached_id, "state": self.cached_state, "volume": self.cached_volume}))

            await asyncio.gather(
                reciever(),
                sender()
            )


    async def main(self):
        await asyncio.gather(
            self.foobar_connection(),
            self.server_connection()
        )


if __name__ == "__main__":
    if os.path.exists("./client_config.toml"):
        with open("./client_config.toml", "rb") as f:
            config = tomllib.load(f)
            domain = config["domain"]
            port = config["port"]
    else:
        with open("./client_config.toml", "wb") as f:
            domain = input("input server domain:\n> ")
            port = int(input("input server port:\n> "))
            tomli_w.dump({"domain": domain, "port": port}, f)

    sync_id = input("type playlist's number to sync:\n> ")

    fs = FooMultiplayerClient(sync_id, domain, port)
    asyncio.run(fs.main())
