import aiosqlite
import json
from datetime import datetime
from typing import Optional, List, Dict
from config import DATABASE_PATH


class MessageDatabase:
    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self.connection: Optional[aiosqlite.Connection] = None

    async def connect(self):
        """Подключение к базе данных"""
        self.connection = await aiosqlite.connect(self.db_path)
        await self.create_tables()

    async def close(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            await self.connection.close()

    async def create_tables(self):
        """Создание таблиц в базе данных"""
        cursor = await self.connection.cursor()
        
        # Таблица для сообщений
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_title TEXT,
                chat_type TEXT,
                user_id INTEGER,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                message_text TEXT,
                date TIMESTAMP,
                is_reply INTEGER DEFAULT 0,
                reply_to_message_id INTEGER,
                has_media INTEGER DEFAULT 0,
                media_type TEXT,
                raw_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица для чатов
        await cursor.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER UNIQUE NOT NULL,
                chat_title TEXT,
                chat_type TEXT,
                participants_count INTEGER,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP,
                metadata TEXT
            )
        ''')
        
        # Индексы для быстрого поиска
        await cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_chat_id 
            ON messages(chat_id)
        ''')
        
        await cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_date 
            ON messages(date)
        ''')
        
        await cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_user_id 
            ON messages(user_id)
        ''')
        
        await self.connection.commit()

    async def save_message(self, message_data: Dict):
        """Сохранение сообщения в базу данных"""
        cursor = await self.connection.cursor()
        
        try:
            await cursor.execute('''
                INSERT INTO messages (
                    message_id, chat_id, chat_title, chat_type,
                    user_id, username, first_name, last_name,
                    message_text, date, is_reply, reply_to_message_id,
                    has_media, media_type, raw_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                message_data.get('message_id'),
                message_data.get('chat_id'),
                message_data.get('chat_title'),
                message_data.get('chat_type'),
                message_data.get('user_id'),
                message_data.get('username'),
                message_data.get('first_name'),
                message_data.get('last_name'),
                message_data.get('message_text'),
                message_data.get('date'),
                message_data.get('is_reply', 0),
                message_data.get('reply_to_message_id'),
                message_data.get('has_media', 0),
                message_data.get('media_type'),
                json.dumps(message_data.get('raw_data', {}))
            ))
            
            await self.connection.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"Ошибка при сохранении сообщения: {e}")
            await self.connection.rollback()
            return None

    async def save_chat(self, chat_data: Dict):
        """Сохранение информации о чате"""
        cursor = await self.connection.cursor()
        
        try:
            await cursor.execute('''
                INSERT OR REPLACE INTO chats (
                    chat_id, chat_title, chat_type, participants_count,
                    last_activity, metadata
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                chat_data.get('chat_id'),
                chat_data.get('chat_title'),
                chat_data.get('chat_type'),
                chat_data.get('participants_count'),
                datetime.now().isoformat(),
                json.dumps(chat_data.get('metadata', {}))
            ))
            
            await self.connection.commit()
        except Exception as e:
            print(f"Ошибка при сохранении чата: {e}")
            await self.connection.rollback()

    async def get_messages_count(self, chat_id: Optional[int] = None) -> int:
        """Получение количества сохраненных сообщений"""
        cursor = await self.connection.cursor()
        
        if chat_id:
            await cursor.execute('SELECT COUNT(*) FROM messages WHERE chat_id = ?', (chat_id,))
        else:
            await cursor.execute('SELECT COUNT(*) FROM messages')
        
        result = await cursor.fetchone()
        return result[0] if result else 0

    async def get_chats(self) -> List[Dict]:
        """Получение списка всех чатов"""
        cursor = await self.connection.cursor()
        await cursor.execute('SELECT * FROM chats ORDER BY last_activity DESC')
        
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        return [dict(zip(columns, row)) for row in rows]

