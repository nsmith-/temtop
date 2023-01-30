#!/usr/bin/env python3
import argparse
import asyncio
import logging
import struct
import traceback
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import partial
from typing import Awaitable, Callable, Optional, Union

import pytz
from influxdb import InfluxDBClient  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class ClientTime:
    tz: pytz.tzinfo.BaseTzInfo

    def to_datetime(self, data: bytes) -> datetime:
        if len(data) != 6:
            raise IOError(f"Invalid date: {data.hex()}")
        ymdhms = list(struct.unpack("!BBBBBB", data))
        ymdhms[0] += 2000
        out = datetime(*ymdhms)
        return self.tz.localize(out)

    def from_datetime(self, value: datetime) -> bytes:
        clientval = self.tz.normalize(value)
        return struct.pack(
            "!BBBBBB",
            clientval.year - 2000,
            clientval.month,
            clientval.day,
            clientval.hour,
            clientval.minute,
            clientval.second,
        )

    def now(self) -> datetime:
        return datetime.now(self.tz)


# TODO: learn timezone from client
CLIENT_TIME = ClientTime(pytz.UTC)


def _cksum(data: bytes) -> bytes:
    s1, s2 = 0, 222
    for byte in data:
        s1 = (s1 + byte) & 0xFF
        s2 = (s2 + byte + byte + 1) & 0xFF
    return struct.pack("!BBB", s1, 0x16, s2)


@dataclass
class Readings:
    date: datetime
    values: dict[str, int]
    _reading = {
        0: "Date",
        1: "PM2.5",
        2: "PM10",
        3: "Temperature",
        4: "Humidity",
        5: "AQI",
        6: "CO2",
    }
    _dtype_size = {0: 1, 3: 2, 4: 4, 9: 6}

    @classmethod
    def parse(cls, data: bytes) -> "Readings":
        values = {}
        date = None
        while len(data):
            keyid, flag, dtype = struct.unpack("!BBB", data[:3])
            if dtype not in cls._dtype_size:
                raise IOError(f"Bad Reading value dtype {dtype} in {data.hex()}")
            length = cls._dtype_size[dtype]
            key = cls._reading.get(keyid, str(keyid))
            if flag == 0x10:
                if key != "Date":
                    raise IOError(f"Unexpected datetime value for {key}")
                date = CLIENT_TIME.to_datetime(data[3 : 3 + length])
            else:
                values[key] = int(data[3 : 3 + length].hex(), base=16)
            data = data[3 + length :]
        if date is None:
            raise IOError("Did not recieve date for reading")
        return cls(date, values)


class MsgType(Enum):
    # sent by client on start after a long delay
    # server replies with current time
    # client acknowledges
    Time = 0x0002
    # sent by client after time sequence
    # server replies with a time and something
    Time2 = 0x8089
    # sent by client after reply to time2
    # server replies ok
    Time3 = 0x8081
    # sent by client every refresh
    CurrentValue = 0x8090
    # sent by client every logging interval
    LoggedValue = 0x8084

    def __index__(self) -> int:
        return self.value


_AppMessage_const = 0x86090000


@dataclass
class AppMessage:
    type: MsgType
    flag: int
    payload: Union[Readings, tuple[bytes, datetime], datetime, bytes]

    @classmethod
    def deserialize(cls, data: bytes) -> "AppMessage":
        if len(data) < 12:
            raise IOError(f"AppMessage too short for known header: {data.hex()}")
        if not (data[0] == 0x68 and data[7] == 0x68):
            raise IOError(f"AppMessage header missing magic bytes: {data.hex()}")
        const, mtype, flag, length = struct.unpack("!xIHxHH", data[:12])
        if const != _AppMessage_const:
            raise IOError(f"AppMessage header has unexpected content: {data.hex()}")
        msgtype = MsgType(mtype)
        if not msgtype:
            raise IOError(f"AppMessage header has unexpected type id: {mtype}")
        if length + 12 != len(data):
            raise IOError(
                f"AppMessage header has unexpected payload length. {data.hex()}"
            )
        payload = data[12:]
        if msgtype in (MsgType.LoggedValue, MsgType.CurrentValue) and len(payload) > 1:
            return AppMessage(
                msgtype,
                flag,
                Readings.parse(payload),
            )
        elif msgtype == MsgType.Time and len(payload) == 6:
            return AppMessage(msgtype, flag, CLIENT_TIME.to_datetime(payload))
        elif msgtype == MsgType.Time2 and len(payload) == 12:
            stuff = payload[:6]
            date = CLIENT_TIME.to_datetime(payload[6:])
            return AppMessage(msgtype, flag, (stuff, date))
        else:
            return AppMessage(msgtype, flag, payload)

    def serialize(self) -> bytes:
        if isinstance(self.payload, bytes):
            payload = self.payload
        elif isinstance(self.payload, tuple):
            payload = self.payload[0] + CLIENT_TIME.from_datetime(self.payload[1])
        elif isinstance(self.payload, datetime):
            payload = CLIENT_TIME.from_datetime(self.payload)
        data = struct.pack(
            "!BIHBHH", 0x68, _AppMessage_const, self.type, 0x68, self.flag, len(payload)
        )
        data += payload
        return data

    @property
    def readings(self) -> Optional[Readings]:
        if self.type in (MsgType.CurrentValue, MsgType.LoggedValue):
            assert isinstance(self.payload, Readings)
            return self.payload
        return None


class ServerState:
    def __init__(self) -> None:
        self.in_time_seq = False

    def respond(self, msg: AppMessage) -> Optional[AppMessage]:
        # TODO msg: Message
        if msg.type == MsgType.Time and not self.in_time_seq:
            self.in_time_seq = True
            return AppMessage(MsgType.Time, 0, CLIENT_TIME.now())
        elif msg.type == MsgType.Time and self.in_time_seq:
            self.in_time_seq = False
        elif msg.type == MsgType.Time2:
            return AppMessage(
                MsgType.Time,
                0,
                (bytes.fromhex("0002003c03ac"), CLIENT_TIME.now()),
            )
        elif msg.type == MsgType.Time3:
            return AppMessage(MsgType.Time3, 1, b"\x00")
        elif msg.type == MsgType.CurrentValue:
            return AppMessage(MsgType.CurrentValue, 1, b"\x00")
        elif msg.type == MsgType.LoggedValue:
            return AppMessage(MsgType.LoggedValue, 0, b"\x00")
        return None


@dataclass
class Message:
    device_id: bytes
    payload: AppMessage

    @classmethod
    async def read(cls, reader: asyncio.StreamReader) -> "Message":
        data = await reader.readexactly(14)
        if not (data[0] == 0x55 and data[11] == 0x55):
            raise IOError(f"Message missing magic bytes: {data.hex()}")
        device_id = data[1:11]
        (payload_length,) = struct.unpack("!H", data[12:14])
        # payload_length counts its own bytes
        total = 12 + payload_length + 3 + 1
        data += await reader.readexactly(total - len(data))
        if not (data[12 + payload_length + 3] == 0x45):
            raise IOError(f"Message missing magic byte at end: {data.hex()}")
        payload = data[14 : 12 + payload_length]
        cksum = data[12 + payload_length : 12 + payload_length + 3]
        if cksum != _cksum(payload):
            raise IOError(
                f"Invalid checksum: {cksum.hex()} for payload {payload.hex()}"
            )
        return cls(device_id, AppMessage.deserialize(payload))

    async def write(self, writer: asyncio.StreamWriter) -> None:
        data = b"\x55" + self.device_id + b"\x55"
        payload = self.payload.serialize()
        data += struct.pack("!H", len(payload) + 2)
        data += payload
        data += _cksum(payload)
        data += b"\x45"
        writer.write(data)
        await writer.drain()


async def handle_client(
    commit_fn: Callable[[str, Readings], Awaitable[None]],
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    client_addr = writer.get_extra_info("peername")
    logger.info(f"New connection from {client_addr!r}")
    state = ServerState()
    try:
        while True:
            message = await Message.read(reader)
            logger.debug(f"New message from {client_addr!r}: {message!r}")
            if data := message.payload.readings:
                logger.debug(f"Committing data point: {data!r}")
                await commit_fn(message.payload.type.name, data)
            if reply_payload := state.respond(message.payload):
                reply = Message(message.device_id, reply_payload)
                logger.debug(f"Responding with: {reply!r}")
                await reply.write(writer)
    except asyncio.IncompleteReadError as ex:
        logger.warning(ex)
    except IOError as ex:
        logger.error(ex)
        traceback.print_tb(ex.__traceback__)
    finally:
        logger.info(f"Closing connection to {client_addr}")
        writer.close()


async def main(idbclient: InfluxDBClient) -> None:
    async def commit(msgtype: str, data: Readings) -> None:
        point = {
            "measurement": msgtype,
            "time": data.date.astimezone(pytz.utc),
            "fields": data.values,
        }
        # InfluxDBClient 1.x has no async methods
        await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: idbclient.write_points([point]),  # type: ignore
        )

    server = await asyncio.start_server(
        partial(handle_client, commit), "54.80.232.65", 5580
    )
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"Serving on {addrs}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Temtop data logging server with InfluxDB forwarding"
    )
    parser.add_argument("--tz", help="Timezone", type=str)
    parser.add_argument("-l", help="Log file", type=str, default="server.log")
    parser.add_argument("--host", help="InfluxDB hostname", type=str)
    parser.add_argument("--username", help="InfluxDB username", type=str)
    parser.add_argument("--password", help="InfluxDB password", type=str)
    args = parser.parse_args()

    CLIENT_TIME.tz = pytz.timezone(args.tz)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%m/%d/%Y %H:%M:%S",
        handlers=[
            logging.FileHandler("server.log"),
            logging.StreamHandler(),  # stderr
        ],
    )

    idbclient = InfluxDBClient(
        host=args.host,
        port=8086,
        username=args.username,
        password=args.password,
        database="temtop",
    )
    asyncio.run(main(idbclient))
