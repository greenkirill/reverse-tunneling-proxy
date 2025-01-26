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
        service_b: 'ServiceB',
        minecraft_address: str,
        minecraft_port: int,
    ) -> None:
        self.uid: int = uid
        self.service_b = service_b
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
                if self.service_b.service_a_writer:
                    self.service_b.service_a_writer.write(message)
                    await self.service_b.service_a_writer.drain()
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
        self.new_service_a_reader: Optional[StreamReader] = None
        self.new_service_a_writer: Optional[StreamWriter] = None
        self.clients: Dict[int, ClientHandler] = {}
        self.ping_task: Optional[asyncio.Task] = None
        self.reconnect_task: Optional[asyncio.Task] = None
        self.last_ping_time: datetime = datetime.now()
        self.last_pong_time: datetime = datetime.now()
        self.connection_lock: asyncio.Lock = asyncio.Lock() 
        self.is_reconnecting = False

    async def connect_to_service_a(self) -> None:
        """Подключается к сервису А."""
        try:
            self.service_a_reader, self.service_a_writer = await asyncio.open_connection(
                self.service_a_address, self.service_a_port
            )
            logging.info("Connected to Service A")
        except Exception as e:
            logging.error(f"Failed to connect to Service A: {e}")

    async def send_ping(self) -> None:
        """Отправляет PING-сообщение каждые 5 секунд."""
        while True:
            try:
                if self.service_a_writer:
                    self.service_a_writer.write(Protocol.build_message(0, 0x04, b"PING"))
                    await self.service_a_writer.drain()
                    self.last_ping_time = datetime.now()
                    logging.debug("Sent PING to Service A")
            except Exception as e:
                logging.error(f"Error sending PING to Service A: {e}")

            # Проверяем разницу между последним PING и PONG
            if self.last_ping_time and self.last_pong_time:
                elapsed = (datetime.now() - self.last_pong_time).total_seconds()
                if elapsed > 30:
                    logging.warning(f"No PONG received for {elapsed} seconds. Reconnecting...")
                    await self.reconnect_to_service_a()
                    return

            await asyncio.sleep(5)

    async def reconnect_to_service_a(self) -> None:
        """Переподключается к сервису А."""
        if self.is_reconnecting:  # Если реконнект уже выполняется, выходим
            logging.info("Reconnect already in progress. Skipping duplicate attempt.")
            return
        self.is_reconnecting = True  # Устанавливаем флаг
        logging.info("Reconnecting to Service A...")
        async with self.connection_lock:
            try:
                if self.service_a_writer:
                    self.service_a_writer.close()
                    await self.service_a_writer.wait_closed()
                await self.connect_to_service_a()
                logging.info("Reconnected to Service A")
            except Exception as e:
                logging.error(f"Error during reconnection: {e}")
            finally:
                self.is_reconnecting = False

    async def reconnect_to_service_a_loop(self) -> None:
        """Переподключается к сервису А раз в час."""
        sleep = 3600
        while True:
            await asyncio.sleep(sleep)  # Ждем 1 час перед реконнектом

            # Создаем новое соединение
            logging.info("Periodic reconnect: Attempting to establish a new connection to Service A...")
            try:
                new_reader, new_writer = await asyncio.open_connection(
                    self.service_a_address, self.service_a_port
                )
                logging.info("Periodic reconnect: New connection to Service A established")
                self.new_service_a_reader = new_reader
                self.new_service_a_writer = new_writer
            except Exception as e:
                logging.error(f"Periodic reconnect: Failed to establish a new connection to Service A: {e}")
                sleep = 300
                continue
            sleep = 3600

    async def handle_service_a(self) -> None:
        """Обрабатывает сообщения от сервиса А."""
        while True:
            # Если соединение не установлено, пытаемся подключиться
            if not self.service_a_reader or not self.service_a_writer:
                logging.info("Connection to Service A lost. Checking for ongoing reconnect...")
                if not self.is_reconnecting:  # Проверяем, идет ли реконнект
                    await self.reconnect_to_service_a()
                else:
                    logging.info("Reconnect already in progress. Waiting...")
                    await asyncio.sleep(1)  # Ждем, пока реконнект завершится
                continue

            try:
                # Читаем сообщение через Protocol
                result = await Protocol.read_message(self.service_a_reader)
                if result is None:
                    logging.warning("Service A returned None. Reconnecting...")
                    await self.reconnect_to_service_a()
                    continue

                uid, msg_type, payload = result
                logging.debug(f"Received message from Service A: uid={uid}, msg_type={msg_type}")

                if msg_type == 0x01:  # DATA
                    await self.handle_client_data(uid, payload)
                elif msg_type == 0x02:  # NEW_CLIENT
                    await self.handle_new_client(uid)
                elif msg_type == 0x03:  # DISCONNECT
                    await self.handle_disconnect(uid)
                elif msg_type == 0x05:  # PONG
                    await self.handle_pong()
                elif msg_type == 0x07: # END_OF_CONNECTION
                    await self.handle_end_of_connection()
                
            except Exception as e:
                logging.error(f"Error while reading from Service A: {e}")
                await self.reconnect_to_service_a()
                await asyncio.sleep(1)

    async def handle_end_of_connection(self) -> None:
        async with self.connection_lock:
            old_writer = self.service_a_writer

            self.service_a_reader = self.new_service_a_reader
            self.service_a_writer = self.new_service_a_writer

            if old_writer:
                try:
                    logging.info("Closing old connection to Service A...")
                    old_writer.close()
                    await old_writer.wait_closed()
                    logging.info("Old connection to Service A closed successfully")
                except Exception as e:
                    logging.error(f"Error while closing old connection to Service A: {e}")

            self.new_service_a_reader = None
            self.new_service_a_writer = None
            logging.info("Switched to new connection to Service A")

    async def handle_pong(self) -> None:
        self.last_pong_time = datetime.now()
        logging.debug(f"Received PONG from Service A. Elapsed since last PING: {self.last_pong_time - self.last_ping_time}")


    async def handle_new_client(self, uid: int) -> None:
        """Обрабатывает нового клиента, подключаясь к локальному Minecraft."""
        if uid not in self.clients and self.service_a_writer:
            try:
                client_handler = ClientHandler(
                    uid, self, self.minecraft_address, self.minecraft_port
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

        self.ping_task = asyncio.create_task(self.send_ping())
        self.reconnect_task = asyncio.create_task(self.reconnect_to_service_a_loop())

        await self.handle_service_a()

        if self.ping_task:
            self.ping_task.cancel()
        if self.reconnect_task:
            self.reconnect_task.cancel()

# Запуск сервиса Б
service_b = ServiceB(
    service_a_address="51.15.120.200",
    service_a_port=12345,
    minecraft_address="127.0.0.1",
    minecraft_port=25565,
)

asyncio.run(service_b.run())
