
import asyncio
from asyncio import StreamReader, StreamWriter
from struct import pack, unpack
from typing import Dict, Optional
from protocol import Protocol
from itertools import count

uid_counter = count(start=1) 
clients: Dict[int, StreamWriter] = {}
service_b: Optional[StreamWriter] = None


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    global service_b
    uid: int = next(uid_counter)
    clients[uid] = writer
    print(f"New client connected: {uid}")

    if service_b:
        # Уведомить сервис Б о новом клиенте
        message = Protocol.build_message(uid, 0x02, b"")
        service_b.write(message)
        await service_b.drain()

    try:
        while True:
            data: bytes = await reader.read(1024)
            if not data:
                break
            if service_b:
                # Отправить данные сервису Б
                message = Protocol.build_message(uid, 0x01, data)
                service_b.write(message)
                await service_b.drain()
    finally:
        print(f"Client disconnected: {uid}")
        del clients[uid]
        if service_b:
            message = Protocol.build_message(uid, 0x03, b"")
            service_b.write(message)
            await service_b.drain()


async def service_b_connection(reader: StreamReader, writer: StreamWriter) -> None:
    global service_b
    service_b = writer
    print("Service B connected")
    try:
        while True:
            # Читаем сообщение от сервиса Б
            result = await Protocol.read_message(reader)
            if result is None:
                break

            uid, msg_type, payload = result
            if msg_type == 0x01:  # DATA
                if uid in clients:
                    clients[uid].write(payload)
                    await clients[uid].drain()
            elif msg_type == 0x03:  # DISCONNECT
                if uid in clients:
                    clients[uid].close()
                    await clients[uid].wait_closed()
                    del clients[uid]
                    print(f"Client {uid} disconnected")
    finally:
        print("Service B disconnected")
        service_b = None


async def main() -> None:
    server_a = await asyncio.start_server(handle_client, "0.0.0.0", 25566)
    print("Service A listening on port 25566")

    server_b = await asyncio.start_server(service_b_connection, "0.0.0.0", 12345)
    print("Service B listener on port 12345")

    async with server_a, server_b:
        await asyncio.gather(server_a.serve_forever(), server_b.serve_forever())

asyncio.run(main())