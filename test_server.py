#!/usr/bin/env python3
import yaml
import logging
import asyncio
import sys
from server import handle_client, Readings


class MockWriter:
    def __init__(self) -> None:
        self.msg: list[bytes] = []

    async def drain(self) -> None:
        pass

    def write(self, msg: bytes) -> None:
        self.msg.append(msg)

    def get_extra_info(self, attr) -> str:
        return "mock client"

    def close(self) -> None:
        pass


if __name__ == "__main__":
    fn = sys.argv[1]
    print(f"Reading packet capture from {fn}.yaml and writing {fn}, {fn}.log")
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        handlers=[
            logging.FileHandler(f"{fn}.log"),
        ],
    )
    with open(fn + ".yaml") as fin:
        cap = yaml.safe_load(fin)

    c_stream = asyncio.StreamReader()
    s_stream = MockWriter()
    s_exp = []
    for p in cap["packets"]:
        if p["peer"] == 0:
            c_stream.feed_data(p["data"])
        else:
            s_exp.append(p["data"])
    c_stream.feed_eof()

    async def commit(msg: str, data: Readings) -> None:
        pass

    try:
        asyncio.run(handle_client(commit, c_stream, s_stream))
    except asyncio.IncompleteReadError:
        pass

    with open(fn, "w") as fout:
        for a, b in zip(s_stream.msg, s_exp):
            fout.write(a.hex() + "\n")
            fout.write(b.hex() + "\n")
            fout.write("\n")
