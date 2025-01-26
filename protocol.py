from asyncio import StreamReader
from enum import Enum
from struct import pack, unpack


class MessageType(Enum):
    DATA = 0x01        # Сообщение с данными
    NEW_CLIENT = 0x02  # Новый клиент
    DISCONNECT = 0x03  # Клиент отключился
    PING = 0x04        # Keep-alive сообщение (пинг)

class Protocol:
    HEADER_SIZE = 9  # 4 байта длина + 4 байта UID + 1 байт тип сообщения

    @staticmethod
    def build_message(uid: int, msg_type: int, payload: bytes) -> bytes:
        """
        Формирует сообщение с заголовком и данными.

        :param uid: Уникальный идентификатор клиента
        :param msg_type: Тип сообщения (например, 0x01 для данных)
        :param payload: Данные сообщения
        :return: Полное сообщение в формате байтов
        """
        length = Protocol.HEADER_SIZE + len(payload)
        length_bytes = pack("!I", length)  # Длина в 4 байта
        uid_bytes = pack("!I", uid)  # UID в 4 байта
        msg_type_bytes = pack("!B", msg_type)  # Тип сообщения в 1 байт
        return length_bytes + uid_bytes + msg_type_bytes + payload

    @staticmethod
    async def read_message(reader: StreamReader) -> tuple[int, int, bytes]:
        """
        Читает сообщение из потока, разбирает заголовок и возвращает данные.

        :param reader: asyncio.StreamReader для чтения
        :return: Кортеж (uid, msg_type, payload)
        """
        # Читаем заголовок
        header = await reader.readexactly(Protocol.HEADER_SIZE)
        length, uid, msg_type = unpack("!IIB", header)

        # Читаем полезные данные
        payload = await reader.readexactly(length - Protocol.HEADER_SIZE)
        return uid, msg_type, payload