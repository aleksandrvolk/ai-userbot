"""
Скрипт для экспорта данных из базы для анализа
"""
import asyncio
import json
import csv
from datetime import datetime
from database import MessageDatabase
from config import DATABASE_PATH


async def export_to_json(db_path: str = DATABASE_PATH, output_file: str = 'messages_export.json'):
    """Экспорт всех сообщений в JSON"""
    db = MessageDatabase(db_path)
    await db.connect()
    
    try:
        cursor = await db.connection.cursor()
        await cursor.execute('''
            SELECT 
                message_id, chat_id, chat_title, chat_type,
                user_id, username, first_name, last_name,
                message_text, date, is_reply, reply_to_message_id,
                has_media, media_type, raw_data
            FROM messages
            ORDER BY date DESC
        ''')
        
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        messages = []
        for row in rows:
            message = dict(zip(columns, row))
            # Парсим JSON поля
            if message.get('raw_data'):
                try:
                    message['raw_data'] = json.loads(message['raw_data'])
                except:
                    pass
            messages.append(message)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Экспортировано {len(messages)} сообщений в {output_file}")
        return output_file
        
    finally:
        await db.close()


async def export_to_csv(db_path: str = DATABASE_PATH, output_file: str = 'messages_export.csv'):
    """Экспорт всех сообщений в CSV"""
    db = MessageDatabase(db_path)
    await db.connect()
    
    try:
        cursor = await db.connection.cursor()
        await cursor.execute('''
            SELECT 
                message_id, chat_id, chat_title, chat_type,
                user_id, username, first_name, last_name,
                message_text, date, is_reply, reply_to_message_id,
                has_media, media_type
            FROM messages
            ORDER BY date DESC
        ''')
        
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            
            for row in rows:
                # Заменяем None на пустую строку для CSV
                row = [str(cell) if cell is not None else '' for cell in row]
                writer.writerow(row)
        
        print(f"✅ Экспортировано {len(rows)} сообщений в {output_file}")
        return output_file
        
    finally:
        await db.close()


async def export_chat_messages(chat_id: int, output_file: str = None):
    """Экспорт сообщений из конкретного чата"""
    db = MessageDatabase()
    await db.connect()
    
    try:
        cursor = await db.connection.cursor()
        await cursor.execute('''
            SELECT chat_title FROM chats WHERE chat_id = ?
        ''', (chat_id,))
        chat_info = await cursor.fetchone()
        chat_title = chat_info[0] if chat_info else f"chat_{chat_id}"
        
        if not output_file:
            output_file = f"messages_{chat_id}_{datetime.now().strftime('%Y%m%d')}.json"
        
        await cursor.execute('''
            SELECT 
                message_id, chat_id, chat_title, chat_type,
                user_id, username, first_name, last_name,
                message_text, date, is_reply, reply_to_message_id,
                has_media, media_type, raw_data
            FROM messages
            WHERE chat_id = ?
            ORDER BY date ASC
        ''', (chat_id,))
        
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        
        messages = []
        for row in rows:
            message = dict(zip(columns, row))
            if message.get('raw_data'):
                try:
                    message['raw_data'] = json.loads(message['raw_data'])
                except:
                    pass
            messages.append(message)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'chat_id': chat_id,
                'chat_title': chat_title,
                'total_messages': len(messages),
                'export_date': datetime.now().isoformat(),
                'messages': messages
            }, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Экспортировано {len(messages)} сообщений из '{chat_title}' в {output_file}")
        return output_file
        
    finally:
        await db.close()


async def get_statistics():
    """Получение статистики по базе данных"""
    db = MessageDatabase()
    await db.connect()
    
    try:
        cursor = await db.connection.cursor()
        
        # Общая статистика
        await cursor.execute('SELECT COUNT(*) FROM messages')
        total_messages = (await cursor.fetchone())[0]
        
        await cursor.execute('SELECT COUNT(DISTINCT chat_id) FROM messages')
        total_chats = (await cursor.fetchone())[0]
        
        await cursor.execute('SELECT COUNT(DISTINCT user_id) FROM messages WHERE user_id IS NOT NULL')
        total_users = (await cursor.fetchone())[0]
        
        # Топ чатов
        await cursor.execute('''
            SELECT chat_id, chat_title, COUNT(*) as count
            FROM messages
            GROUP BY chat_id, chat_title
            ORDER BY count DESC
            LIMIT 10
        ''')
        top_chats = await cursor.fetchall()
        
        print("\n📊 Статистика базы данных:")
        print(f"Всего сообщений: {total_messages}")
        print(f"Всего чатов: {total_chats}")
        print(f"Всего пользователей: {total_users}")
        print("\nТоп-10 чатов по количеству сообщений:")
        for chat_id, chat_title, count in top_chats:
            print(f"  • {chat_title}: {count} сообщений")
        
    finally:
        await db.close()


async def main():
    """Главная функция"""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'json':
            output = sys.argv[2] if len(sys.argv) > 2 else 'messages_export.json'
            await export_to_json(output_file=output)
        elif command == 'csv':
            output = sys.argv[2] if len(sys.argv) > 2 else 'messages_export.csv'
            await export_to_csv(output_file=output)
        elif command == 'chat':
            if len(sys.argv) < 3:
                print("Использование: python export_data.py chat <chat_id> [output_file]")
                return
            chat_id = int(sys.argv[2])
            output = sys.argv[3] if len(sys.argv) > 3 else None
            await export_chat_messages(chat_id, output)
        elif command == 'stats':
            await get_statistics()
        else:
            print("Неизвестная команда")
            print("Использование:")
            print("  python export_data.py json [output_file]  - экспорт в JSON")
            print("  python export_data.py csv [output_file]   - экспорт в CSV")
            print("  python export_data.py chat <chat_id> [output] - экспорт чата")
            print("  python export_data.py stats               - статистика")
    else:
        # По умолчанию экспорт в JSON
        await export_to_json()


if __name__ == '__main__':
    asyncio.run(main())

