import asyncio
import logging
from datetime import datetime, timedelta
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User, Chat, Channel
from telethon.errors import FloodWaitError, ChatAdminRequiredError
from config import (
    API_ID,
    API_HASH,
    SESSION_NAME,
    STRING_SESSION,
    LOG_LEVEL,
    LOG_FILE,
)
from database import MessageDatabase

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Инициализация базы данных
db = MessageDatabase()

# Инициализация клиента Telegram
session_arg = StringSession(STRING_SESSION) if STRING_SESSION else SESSION_NAME
client = TelegramClient(session_arg, API_ID, API_HASH)

# Флаг для отслеживания активного парсинга
parsing_active = {}


def get_chat_info(chat):
    """Получение информации о чате"""
    if isinstance(chat, User):
        return {
            'chat_id': chat.id,
            'chat_title': f"{chat.first_name or ''} {chat.last_name or ''}".strip() or chat.username or f"User {chat.id}",
            'chat_type': 'private',
            'participants_count': 1
        }
    elif isinstance(chat, (Chat, Channel)):
        return {
            'chat_id': chat.id,
            'chat_title': getattr(chat, 'title', None) or f"Chat {chat.id}",
            'chat_type': 'channel' if isinstance(chat, Channel) else 'group',
            'participants_count': getattr(chat, 'participants_count', None)
        }
    return {
        'chat_id': chat.id if hasattr(chat, 'id') else 0,
        'chat_title': 'Unknown',
        'chat_type': 'unknown',
        'participants_count': None
    }


def get_user_info(sender):
    """Получение информации о пользователе"""
    if not sender:
        return {
            'user_id': None,
            'username': None,
            'first_name': None,
            'last_name': None
        }
    
    return {
        'user_id': sender.id,
        'username': getattr(sender, 'username', None),
        'first_name': getattr(sender, 'first_name', None),
        'last_name': getattr(sender, 'last_name', None)
    }


def get_media_info(message):
    """Получение информации о медиа в сообщении"""
    if not message.media:
        return {
            'has_media': False,
            'media_type': None
        }
    
    media_type = type(message.media).__name__
    return {
        'has_media': True,
        'media_type': media_type
    }


async def process_message(message, chat, sender=None):
    """Обработка и сохранение сообщения"""
    try:
        # Получение информации о чате
        chat_info = get_chat_info(chat)
        
        # Получение информации о пользователе
        if sender is None:
            try:
                sender = await message.get_sender()
            except Exception as e:
                logger.debug(f"Не удалось получить информацию об отправителе: {e}")
                sender = None
        user_info = get_user_info(sender)
        
        # Получение информации о медиа
        media_info = get_media_info(message)
        
        # Проверка, является ли сообщение ответом
        is_reply = message.reply_to is not None
        reply_to_message_id = None
        if is_reply and hasattr(message.reply_to, 'reply_to_msg_id'):
            reply_to_message_id = message.reply_to.reply_to_msg_id
        
        # Подготовка данных для сохранения
        message_data = {
            'message_id': message.id,
            'chat_id': chat_info['chat_id'],
            'chat_title': chat_info['chat_title'],
            'chat_type': chat_info['chat_type'],
            'user_id': user_info['user_id'],
            'username': user_info['username'],
            'first_name': user_info['first_name'],
            'last_name': user_info['last_name'],
            'message_text': message.text or message.raw_text or '',
            'date': message.date.isoformat() if message.date else datetime.now().isoformat(),
            'is_reply': 1 if is_reply else 0,
            'reply_to_message_id': reply_to_message_id,
            'has_media': 1 if media_info['has_media'] else 0,
            'media_type': media_info['media_type'],
            'raw_data': {
                'message_id': message.id,
                'date': message.date.isoformat() if message.date else None,
                'views': getattr(message, 'views', None),
                'forwards': getattr(message, 'forwards', None),
                'replies': getattr(message.replies, 'replies', None) if hasattr(message, 'replies') and message.replies else None,
            }
        }
        
        # Сохранение сообщения
        await db.save_message(message_data)
        
        # Сохранение информации о чате
        chat_data = {
            **chat_info,
            'metadata': {
                'access_hash': getattr(chat, 'access_hash', None),
                'username': getattr(chat, 'username', None)
            }
        }
        await db.save_chat(chat_data)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)
        return False


async def parse_chat_history(chat_entity, limit=None, offset_date=None):
    """
    Парсинг истории сообщений из чата
    
    Args:
        chat_entity: Объект чата (может быть username, ID или entity)
        limit: Максимальное количество сообщений для парсинга (None = все)
        offset_date: Дата, с которой начинать парсинг (None = с начала)
    """
    chat_id = None
    chat_title = "Unknown"
    
    try:
        # Получение информации о чате
        try:
            if isinstance(chat_entity, (int, str)):
                chat = await client.get_entity(chat_entity)
            else:
                chat = chat_entity
        except ValueError as e:
            logger.error(f"Группа не найдена: {chat_entity}. Ошибка: {e}")
            raise ValueError(f"Группа '{chat_entity}' не найдена. Проверьте username или ID, или убедитесь, что у вас есть доступ к группе.")
        except Exception as e:
            logger.error(f"Ошибка при получении информации о группе {chat_entity}: {e}")
            raise
        
        chat_info = get_chat_info(chat)
        chat_id = chat_info['chat_id']
        chat_title = chat_info['chat_title']
        
        # Проверка, не идет ли уже парсинг этого чата
        if chat_id in parsing_active and parsing_active[chat_id]:
            logger.warning(f"Парсинг чата {chat_title} уже выполняется")
            return False
        
        parsing_active[chat_id] = True
        logger.info(f"Начало парсинга истории чата: {chat_title} (ID: {chat_id})")
        
        total_parsed = 0
        errors_count = 0
        
        try:
            async for message in client.iter_messages(
                chat,
                limit=limit,
                offset_date=offset_date,
                reverse=False  # Сначала старые сообщения
            ):
                try:
                    # Пропускаем служебные сообщения
                    if message.action:
                        continue
                    
                    try:
                        sender = await message.get_sender()
                    except Exception as e:
                        logger.debug(f"Не удалось получить отправителя для сообщения {message.id}: {e}")
                        sender = None
                    
                    success = await process_message(message, chat, sender)
                    
                    if success:
                        total_parsed += 1
                        if total_parsed % 100 == 0:
                            logger.info(f"Обработано сообщений из {chat_title}: {total_parsed}")
                    else:
                        errors_count += 1
                    
                    # Небольшая задержка, чтобы не получить FloodWait
                    if total_parsed % 50 == 0:
                        await asyncio.sleep(1)
                        
                except FloodWaitError as e:
                    logger.warning(f"FloodWait: ожидание {e.seconds} секунд...")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    errors_count += 1
                    logger.error(f"Ошибка при обработке сообщения {message.id}: {e}")
                    continue
                    
        except ChatAdminRequiredError:
            logger.error(f"Нет доступа к истории чата {chat_title}. Убедитесь, что бот добавлен в группу и имеет права.")
            return False
        except Exception as e:
            logger.error(f"Ошибка при парсинге чата {chat_title}: {e}", exc_info=True)
            return False
        finally:
            parsing_active[chat_id] = False
        
        logger.info(f"Парсинг завершен: {chat_title}. Обработано: {total_parsed}, Ошибок: {errors_count}")
        return True
        
    except Exception as e:
        logger.error(f"Критическая ошибка при парсинге чата: {e}", exc_info=True)
        if chat_id:
            parsing_active[chat_id] = False
        return False


@client.on(events.NewMessage)
async def handler(event):
    """Обработчик новых сообщений"""
    try:
        message = event.message
        chat = await event.get_chat()
        sender = await event.get_sender()
        
        # Пропускаем служебные сообщения
        if message.action:
            return
        
        await process_message(message, chat, sender)
        
        chat_info = get_chat_info(chat)
        user_info = get_user_info(sender)
        logger.debug(f"Сохранено сообщение: {chat_info['chat_title']} - {user_info['username'] or user_info['first_name'] or 'Unknown'}")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}", exc_info=True)


@client.on(events.MessageEdited)
async def handler_edited(event):
    """Обработчик отредактированных сообщений"""
    try:
        message = event.message
        chat = await event.get_chat()
        sender = await event.get_sender()
        
        await process_message(message, chat, sender)
        logger.debug(f"Отредактировано сообщение в чате {event.chat_id}")
    except Exception as e:
        logger.error(f"Ошибка при обработке отредактированного сообщения: {e}", exc_info=True)


@client.on(events.NewMessage(pattern=r'^/parse\s+(.+)$', incoming=True, from_users=None))
async def parse_command_handler(event):
    """Обработчик команды /parse для парсинга истории чата"""
    try:
        # Команда работает только в личных сообщениях
        if not event.is_private:
            return
        
        # Получаем аргументы команды
        args = event.pattern_match.group(1).strip()
        
        # Парсим аргументы: /parse @username или /parse @username limit=1000
        parts = args.split()
        chat_identifier = parts[0]
        limit = None
        
        # Поиск параметра limit
        for part in parts[1:]:
            if part.startswith('limit='):
                try:
                    limit = int(part.split('=')[1])
                except ValueError:
                    pass
        
        await event.respond(f"🔄 Начинаю парсинг чата: {chat_identifier}\n⏳ Это может занять некоторое время...")
        
        # Запускаем парсинг в фоне
        try:
            success = await parse_chat_history(chat_identifier, limit=limit)
            
            if success:
                count = await db.get_messages_count()  # Получаем общее количество
                await event.respond(
                    f"✅ Парсинг завершен!\n"
                    f"📊 Всего сообщений в базе: {count}\n"
                    f"💾 Используйте /stats для детальной статистики"
                )
            else:
                await event.respond(
                    "❌ Ошибка при парсинге.\n"
                    "Возможные причины:\n"
                    "• Группа приватная и вы не участник\n"
                    "• Неправильный username или ID\n"
                    "• Нет доступа к истории сообщений\n\n"
                    "Проверьте логи для подробностей."
                )
        except ValueError as e:
            # Ошибка при получении entity (группа не найдена)
            await event.respond(
                f"❌ Группа не найдена: {chat_identifier}\n\n"
                "Проверьте:\n"
                "• Правильность username (например: @groupname)\n"
                "• Правильность ID группы\n"
                "• Доступ к группе (для приватных групп нужно быть участником)"
            )
        except Exception as e:
            error_msg = str(e)
            if "username" in error_msg.lower() or "not found" in error_msg.lower():
                await event.respond(
                    f"❌ Группа не найдена или нет доступа.\n"
                    f"Ошибка: {error_msg}\n\n"
                    "Для приватных групп нужно быть участником."
                )
            else:
                await event.respond(f"❌ Ошибка: {error_msg}\nПроверьте логи для подробностей.")
            
    except Exception as e:
        logger.error(f"Ошибка в команде /parse: {e}", exc_info=True)
        await event.respond(f"❌ Критическая ошибка: {str(e)}")


@client.on(events.NewMessage(pattern=r'^/stats$', incoming=True, from_users=None))
async def stats_command_handler(event):
    """Обработчик команды /stats для получения статистики"""
    try:
        if not event.is_private:
            return
        
        total_messages = await db.get_messages_count()
        chats = await db.get_chats()
        
        stats_text = f"📊 **Статистика парсера**\n\n"
        stats_text += f"Всего сообщений: {total_messages}\n"
        stats_text += f"Всего чатов: {len(chats)}\n\n"
        stats_text += "**Топ чатов:**\n"
        
        # Получаем статистику по чатам
        for chat in chats[:10]:
            chat_messages = await db.get_messages_count(chat['chat_id'])
            stats_text += f"• {chat['chat_title']}: {chat_messages} сообщений\n"
        
        await event.respond(stats_text)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /stats: {e}", exc_info=True)
        await event.respond(f"❌ Ошибка: {str(e)}")


@client.on(events.NewMessage(pattern=r'^/help$', incoming=True, from_users=None))
async def help_command_handler(event):
    """Обработчик команды /help"""
    try:
        if not event.is_private:
            return
        
        help_text = """
🤖 **Команды userbot:**

`/parse @username` - Начать парсинг истории чата
`/parse @username limit=1000` - Парсинг с ограничением количества
`/stats` - Показать статистику
`/help` - Показать эту справку

**Примеры:**
`/parse @mygroup`
`/parse @support_group limit=5000`

**Примечание:** Бот должен быть добавлен в группу для парсинга.
        """
        
        await event.respond(help_text)
        
    except Exception as e:
        logger.error(f"Ошибка в команде /help: {e}", exc_info=True)


async def main():
    """Основная функция запуска userbot"""
    logger.info("Запуск userbot...")
    
    # Подключение к базе данных
    await db.connect()
    logger.info("Подключено к базе данных")
    
    # Подключение к Telegram
    import os
    if STRING_SESSION:
        logger.info("Используется STRING_SESSION из переменных окружения")
        await client.start()
    else:
        # Проверяем наличие файла сессии
        session_file = f"{SESSION_NAME}.session"
        if not os.path.exists(session_file):
            logger.warning(f"Файл сессии {session_file} не найден!")
            logger.warning("Userbot требует авторизацию. Запустите локально один раз для создания сессии.")
            logger.warning("Или используйте переменные окружения PHONE и PHONE_CODE для авторизации.")
            
            # Попытка авторизации через переменные окружения
            phone = os.getenv('PHONE')
            phone_code = os.getenv('PHONE_CODE')
            
            if phone and phone_code:
                logger.info(f"Попытка авторизации через переменные окружения для {phone}")
                try:
                    await client.start(phone=phone, code_callback=lambda: phone_code)
                    logger.info("Авторизация успешна через переменные окружения!")
                except Exception as e:
                    logger.error(f"Ошибка авторизации через переменные окружения: {e}")
                    logger.error("Загрузите файл сессии или авторизуйтесь локально")
                    raise
            else:
                logger.error("Файл сессии не найден и переменные окружения PHONE/PHONE_CODE не указаны")
                logger.error("Запустите userbot локально один раз для создания сессии, затем загрузите файл на сервер или укажите STRING_SESSION")
                raise FileNotFoundError(f"Файл сессии {session_file} не найден. Загрузите его на сервер или авторизуйтесь локально.")
        else:
            await client.start()
    
    logger.info(\"Userbot запущен и готов к работе!\")
    
    # Получение информации о себе
    me = await client.get_me()
    logger.info(f"Вошли как: {me.first_name} {me.last_name or ''} (@{me.username or 'без username'})")
    logger.info(f"ID аккаунта: {me.id}")
    
    # Статистика
    messages_count = await db.get_messages_count()
    logger.info(f"Всего сообщений в базе: {messages_count}")
    
    # Информация о командах
    logger.info("Доступные команды (в личных сообщениях):")
    logger.info("  /parse @username - парсинг истории чата")
    logger.info("  /stats - статистика")
    logger.info("  /help - справка")
    
    # Запуск в режиме ожидания
    await client.run_until_disconnected()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановка userbot...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        asyncio.run(db.close())

