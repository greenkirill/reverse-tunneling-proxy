import asyncio
from asyncio import StreamReader, StreamWriter
from datetime import datetime
from struct import pack, unpack
from typing import Dict, Optional
from protocol import MessageType, Protocol
from itertools import count
import logging
import os

# Настройка логирования
os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f"logs/logs_public_server_{timestamp}.txt"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

uid_counter = count(start=1)
clients: Dict[int, StreamWriter] = {}
service_b: Optional[StreamWriter] = None


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    global service_b
    uid: int = next(uid_counter)
    clients[uid] = writer
    logging.info(f"New client connected: {uid}")

    if service_b:
        try:
            # Уведомить сервис Б о новом клиенте
            message = Protocol.build_message(uid, MessageType.NEW_CLIENT, b"")
            service_b.write(message)
            await service_b.drain()
            logging.debug(f"Notified Service B about new client {uid}")
        except Exception as e:
            logging.error(f"Error notifying Service B about new client {uid}: {e}")

    try:
        while True:
            data: bytes = await reader.read(1024)
            if not data:
                break
            if service_b:
                try:
                    # Отправить данные сервису Б
                    message = Protocol.build_message(uid, MessageType.DATA, data)
                    service_b.write(message)
                    await service_b.drain()
                    logging.debug(f"Forwarded data from client {uid} to Service B")
                except Exception as e:
                    logging.error(f"Error forwarding data from client {uid} to Service B: {e}")
    finally:
        logging.info(f"Client disconnected: {uid}")
        del clients[uid]
        if service_b:
            try:
                message = Protocol.build_message(uid, MessageType.DISCONNECT, b"")
                service_b.write(message)
                await service_b.drain()
                logging.debug(f"Notified Service B about client {uid} disconnection")
            except Exception as e:
                logging.error(f"Error notifying Service B about client {uid} disconnection: {e}")


async def service_b_connection(reader: StreamReader, writer: StreamWriter) -> None:
    global service_b
    service_b = writer
    logging.info("Service B connected")
    try:
        while True:
            # Читаем сообщение от сервиса Б
            result = await Protocol.read_message(reader)
            if result is None:
                break

            uid, msg_type, payload = result
            if msg_type == 0x01:  # DATA
                if uid in clients:
                    try:
                        clients[uid].write(payload)
                        await clients[uid].drain()
                        logging.debug(f"Forwarded data from Service B to client {uid}")
                    except Exception as e:
                        logging.error(f"Error forwarding data to client {uid}: {e}")
            elif msg_type == 0x03:  # DISCONNECT
                if uid in clients:
                    try:
                        clients[uid].close()
                        await clients[uid].wait_closed()
                        del clients[uid]
                        logging.info(f"Client {uid} disconnected by Service B")
                    except Exception as e:
                        logging.error(f"Error disconnecting client {uid}: {e}")
    finally:
        logging.warning("Service B disconnected")
        service_b = None


async def main() -> None:
    try:
        server_a = await asyncio.start_server(handle_client, "0.0.0.0", 25565)
        logging.info("Service A listening on port 25565")

        server_b = await asyncio.start_server(service_b_connection, "0.0.0.0", 12345)
        logging.info("Service B listener on port 12345")

        async with server_a, server_b:
            await asyncio.gather(server_a.serve_forever(), server_b.serve_forever())
    except Exception as e:
        logging.critical(f"Critical error in main: {e}")


asyncio.run(main())
