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
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# uid_counter = count(start=1)
# clients: Dict[int, StreamWriter] = {}
# service_b: Optional[StreamWriter] = None

class ClientConnectionManager:
    def __init__(self) -> None:
        self.clients: Dict[int, StreamWriter] = {}
        self.uid_counter = count(start=1)

    def set_service_b_manager(self, service_b_manager: 'ServiceBConnectionManager') -> None:
        self.service_b_manager = service_b_manager

    async def handle_client(self, reader: StreamReader, writer: StreamWriter) -> None:
        """Обрабатывает подключение клиента."""
        uid: int = next(self.uid_counter)
        self.clients[uid] = writer
        logging.info(f"New client connected: {uid}")

        # Уведомляем ServiceB о новом клиенте
        service_b = self.service_b_manager.current_writer
        if service_b:
            try:
                message = Protocol.build_message(uid, 0x02, b"")
                service_b.write(message)
                await service_b.drain()
                logging.info(f"Notified Service B about new client {uid}")
            except Exception as e:
                logging.error(f"Error notifying Service B about new client {uid}: {e}")

        try:
            while True:
                data: bytes = await reader.read(1024)
                if not data:
                    break

                # Пересылаем данные в ServiceB
                service_b = self.service_b_manager.current_writer
                if service_b:
                    try:
                        message = Protocol.build_message(uid, 0x01, data)
                        service_b.write(message)
                        await service_b.drain()
                        logging.debug(f"Forwarded data from client {uid} to Service B")
                    except Exception as e:
                        logging.error(f"Error forwarding data from client {uid} to Service B: {e}")
        finally:
            logging.info(f"Client disconnected: {uid}")
            del self.clients[uid]

            # Уведомляем ServiceB о разрыве соединения клиента
            service_b = self.service_b_manager.current_writer
            if service_b:
                try:
                    message = Protocol.build_message(uid, 0x03, b"")
                    service_b.write(message)
                    await service_b.drain()
                    logging.info(f"Notified Service B about client {uid} disconnection")
                except Exception as e:
                    logging.error(f"Error notifying Service B about client {uid} disconnection: {e}")



class ServiceBConnectionManager:
    def __init__(self, client_connection_manager: ClientConnectionManager) -> None:
        self.current_reader: Optional[StreamReader] = None
        self.current_writer: Optional[StreamWriter] = None
        self.switch_connection_flag: bool = False
        self.new_reader: Optional[StreamReader] = None
        self.new_writer: Optional[StreamWriter] = None
        self.client_connection_manager = client_connection_manager

    async def switch_connection(self, reader: StreamReader, writer: StreamWriter) -> None:
        """Запускает процесс переключения соединения."""
        try:
            writer.write(Protocol.build_message(0, 0x06, b"NEW_CONNECTION_ESTABLISHED"))
            await writer.drain()
            self.new_reader = reader
            self.new_writer = writer
            self.switch_connection_flag = True
            logging.info("New connection initialized and flagged for switching.")
        except Exception as e:
            logging.error(f"Error during NEW_CONNECTION_ESTABLISHED: {e}")
            writer.close()
            await writer.wait_closed()

    async def handle_connection(self) -> None:
        """Основной цикл обработки соединения."""
        while True:
            if self.switch_connection_flag:
                logging.info("Switching to the new connection...")
                await self._apply_new_connection()
                logging.info("Switched to the new connection.")

            if not self.current_reader or not self.current_writer:
                logging.error("No active connection. Waiting for a new connection...")
                await asyncio.sleep(1)
                continue

            try:
                # Читаем сообщение через Protocol
                result = await Protocol.read_message(self.current_reader)
                if result is None:
                    logging.warning("Service B returned None. Closing connection...")
                    await self._close_current_connection()
                    continue

                uid, msg_type, payload = result
                logging.debug(f"Received message from Service B: uid={uid}, msg_type={msg_type}")

                # Обработка сообщений
                if msg_type == 0x01:  # DATA
                    await self._handle_data(uid, payload)
                elif msg_type == 0x03:  # DISCONNECT
                    await self._handle_disconnect(uid)
                elif msg_type == 0x04:  # PING
                    await self._handle_ping()
                elif msg_type == 0x07:  # END_OF_CONNECTION
                    await self._handle_end_of_connection()

            except Exception as e:
                logging.error(f"Error in connection loop: {e}")
                await self._close_current_connection()
                await asyncio.sleep(1)  # Избегаем быстрого цикла ошибок

    async def _apply_new_connection(self) -> None:
        """Применяет новое соединение, закрывая старое."""
        old_writer = self.current_writer
        self.current_reader = self.new_reader
        self.current_writer = self.new_writer
        if old_writer:
            try:
                old_writer.write(Protocol.build_message(0, 0x07, b"END_OF_CONNECTION"))
                await old_writer.drain()
                logging.info("Sent END_OF_CONNECTION to old connection.")
            except Exception as e:
                logging.error(f"Error sending END_OF_CONNECTION: {e}")
            finally:
                old_writer.close()
                await old_writer.wait_closed()
                logging.info("Old connection closed.")

        self.new_reader = None
        self.new_writer = None
        self.switch_connection_flag = False
        logging.info("Connection switched successfully.")

    async def _close_current_connection(self) -> None:
        """Закрывает текущее соединение."""
        if self.current_writer:
            self.current_writer.close()
            await self.current_writer.wait_closed()
            logging.info("Current connection closed.")
        self.current_reader = None
        self.current_writer = None

    def get_clients(self) -> Dict[int, StreamWriter]:
        return self.client_connection_manager.clients

    async def _handle_data(self, uid: int, payload: bytes) -> None:
        """Обрабатывает входящие данные."""
        if uid in self.client_connection_manager.clients:
            try:
                self.client_connection_manager.clients[uid].write(payload)
                await self.client_connection_manager.clients[uid].drain()
                logging.debug(f"Forwarded data to client {uid}")
            except Exception as e:
                logging.error(f"Error forwarding data to client {uid}: {e}")

    async def _handle_disconnect(self, uid: int) -> None:
        """Обрабатывает сообщение о разрыве соединения."""
        if uid in self.client_connection_manager.clients:
            try:
                self.client_connection_manager.clients[uid].close()
                await self.client_connection_manager.clients[uid].wait_closed()
                del self.client_connection_manager.clients[uid]
                logging.info(f"Client {uid} disconnected.")
            except Exception as e:
                logging.error(f"Error disconnecting client {uid}: {e}")

    async def _handle_ping(self) -> None:
        """Обрабатывает PING."""
        try:
            if self.current_writer:
                self.current_writer.write(Protocol.build_message(0, 0x05, b"PONG"))
                await self.current_writer.drain()
                logging.debug("Sent PONG to Service B")
        except Exception as e:
            logging.error(f"Error sending PONG to Service B: {e}")

    async def _handle_end_of_connection(self) -> None:
        """Обрабатывает сообщение END_OF_CONNECTION."""
        logging.info("Received END_OF_CONNECTION from Service B. Closing current connection.")
        await self._close_current_connection()

async def main() -> None:
    try:
        client_manager = ClientConnectionManager()
        service_b_manager = ServiceBConnectionManager(client_manager)
        client_manager.set_service_b_manager(service_b_manager)

        # Запускаем сервер для клиентов
        server_a = await asyncio.start_server(client_manager.handle_client, "0.0.0.0", 25565)
        logging.info("Service A listening on port 25565")

        # Запускаем сервер для Service B
        server_b = await asyncio.start_server(service_b_manager.switch_connection, "0.0.0.0", 12345)
        logging.info("Service B listener on port 12345")

        async with server_a, server_b:
            await asyncio.gather(
                service_b_manager.handle_connection(),
                server_a.serve_forever(),
                server_b.serve_forever()
            )
    except Exception as e:
        logging.critical(f"Critical error in main: {e}")

asyncio.run(main())
