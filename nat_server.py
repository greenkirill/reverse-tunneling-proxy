import asyncio
from asyncio import StreamReader, StreamWriter
from datetime import datetime
from typing import Dict, Optional
from struct import pack, unpack
from protocol import Protocol, MessageType
import logging
import os

# Настройка логирования
os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f"logs/logs_nat_server_{timestamp}.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

class ClientHandler:
    def __init__(
        self,
        uid: int,
        service_a_writer: StreamWriter,
        minecraft_address: str,
        minecraft_port: int,
    ) -> None:
        self.uid: int = uid
        self.service_a_writer: StreamWriter = service_a_writer
        self.minecraft_address: str = minecraft_address
        self.minecraft_port: int = minecraft_port
        self.minecraft_reader: Optional[StreamReader] = None
        self.minecraft_writer: Optional[StreamWriter] = None

    async def connect_to_minecraft(self) -> None:
        """Подключается к локальному Minecraft-серверу."""
        try:
            self.minecraft_reader, self.minecraft_writer = await asyncio.open_connection(
                self.minecraft_address, self.minecraft_port
            )
            logging.info(f"[{self.uid}] Connected to Minecraft server")
        except Exception as e:
            logging.error(f"[{self.uid}] Failed to connect to Minecraft server: {e}")

    async def handle_minecraft_to_service_a(self) -> None:
        """Читает данные от Minecraft-сервера и пересылает их в сервис А."""
        try:
            while True:
                if not self.minecraft_reader:
                    raise RuntimeError("Minecraft reader is not initialized")

                data = await self.minecraft_reader.read(1024)
                if not data:
                    break
                # Отправляем данные сервису А через Protocol
                message = Protocol.build_message(self.uid, 0x01, data)
                self.service_a_writer.write(message)
                await self.service_a_writer.drain()
                logging.debug(f"[{self.uid}] Data sent to Service A: {len(data)}")
        except Exception as e:
            logging.error(f"[{self.uid}] Error while reading from Minecraft: {e}")
        finally:
            await self.close()

    async def send_to_minecraft(self, data: bytes) -> None:
        """Отправляет данные в локальный Minecraft-сервер."""
        try:
            if self.minecraft_writer:
                self.minecraft_writer.write(data)
                await self.minecraft_writer.drain()
                logging.debug(f"[{self.uid}] Data sent to Minecraft: {len(data)}")
        except Exception as e:
            logging.error(f"[{self.uid}] Error while sending to Minecraft: {e}")

    async def close(self) -> None:
        """Закрывает соединения."""
        if self.minecraft_writer:
            try:
                self.minecraft_writer.close()
                await self.minecraft_writer.wait_closed()
                logging.info(f"[{self.uid}] Closed connection to Minecraft")
            except Exception as e:
                logging.error(f"[{self.uid}] Error while closing Minecraft connection: {e}")


class ServiceB:
    def __init__(
        self,
        service_a_address: str,
        service_a_port: int,
        minecraft_address: str,
        minecraft_port: int,
    ) -> None:
        self.service_a_address: str = service_a_address
        self.service_a_port: int = service_a_port
        self.minecraft_address: str = minecraft_address
        self.minecraft_port: int = minecraft_port
        self.service_a_reader: Optional[StreamReader] = None
        self.service_a_writer: Optional[StreamWriter] = None
        self.clients: Dict[int, ClientHandler] = {}

    async def connect_to_service_a(self) -> None:
        """Подключается к сервису А."""
        try:
            self.service_a_reader, self.service_a_writer = await asyncio.open_connection(
                self.service_a_address, self.service_a_port
            )
            logging.info("Connected to Service A")
        except Exception as e:
            logging.error(f"Failed to connect to Service A: {e}")

    async def handle_service_a(self) -> None:
        """Обрабатывает сообщения от сервиса А."""
        try:
            while True:
                if not self.service_a_reader:
                    raise RuntimeError("Service A reader is not initialized")

                # Читаем сообщение через Protocol
                result = await Protocol.read_message(self.service_a_reader)
                if result is None:
                    break

                uid, msg_type, payload = result
                logging.debug(f"Received message from Service A: uid={uid}, msg_type={msg_type}")
                if msg_type == 0x01:  # DATA
                    await self.handle_client_data(uid, payload)
                elif msg_type == 0x02:  # NEW_CLIENT
                    await self.handle_new_client(uid)
                elif msg_type == 0x03:  # DISCONNECT
                    await self.handle_disconnect(uid)
        except Exception as e:
            logging.error(f"Error while reading from Service A: {e}")
        finally:
            logging.warning("Disconnected from Service A")

    async def handle_new_client(self, uid: int) -> None:
        """Обрабатывает нового клиента, подключаясь к локальному Minecraft."""
        if uid not in self.clients and self.service_a_writer:
            try:
                client_handler = ClientHandler(
                    uid, self.service_a_writer, self.minecraft_address, self.minecraft_port
                )
                await client_handler.connect_to_minecraft()
                self.clients[uid] = client_handler
                asyncio.create_task(client_handler.handle_minecraft_to_service_a())
                logging.info(f"[{uid}] Client handler created")
            except Exception as e:
                logging.error(f"[{uid}] Error while handling new client: {e}")

    async def handle_client_data(self, uid: int, data: bytes) -> None:
        """Передает данные от клиента в Minecraft."""
        if uid in self.clients:
            await self.clients[uid].send_to_minecraft(data)
            logging.debug(f"[{uid}] Data forwarded to Minecraft")
        else:
            logging.warning(f"[{uid}] No client handler found for data")

    async def handle_disconnect(self, uid: int) -> None:
        """Закрывает соединение с Minecraft-сервером для клиента."""
        if uid in self.clients:
            await self.clients[uid].close()
            del self.clients[uid]
            logging.info(f"[{uid}] Client handler removed")

    async def run(self) -> None:
        """Запускает сервис Б."""
        await self.connect_to_service_a()
        await self.handle_service_a()


# Запуск сервиса Б
service_b = ServiceB(
    service_a_address="51.15.120.200",
    service_a_port=12345,
    minecraft_address="127.0.0.1",
    minecraft_port=25565,
)

asyncio.run(service_b.run())
