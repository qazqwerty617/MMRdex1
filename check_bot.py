import asyncio
import os
from aiogram import Bot

TELEGRAM_BOT_TOKEN = "7540264023:AAG..." # (из вашего env)
TELEGRAM_USER_ID = -1003582014728 # Ваш Chat ID из логов

# Давайте возьмем токен из env, так как я его не знаю полностью
from dotenv import load_dotenv
load_dotenv()

token = os.getenv("TELEGRAM_BOT_TOKEN")
chat_id = os.getenv("TELEGRAM_USER_ID")

async def test():
    print(f"Testing bot with token: {token[:10]}...")
    print(f"Target Chat ID: {chat_id}")
    
    bot = Bot(token=token)
    
    try:
        # 1. Try sending to General (no topic)
        print("Attempting to send to General...", end=" ")
        await bot.send_message(chat_id, "🔔 Test Message to General")
        print("SUCCESS! ✅")
    except Exception as e:
        print(f"FAILED: {e}")

    try:
        # 2. Try sending to Topic 11 (from config)
        print("Attempting to send to Topic 11...", end=" ")
        await bot.send_message(chat_id, "🔔 Test Message to Topic 11", message_thread_id=11)
        print("SUCCESS! ✅")
    except Exception as e:
        print(f"FAILED: {e}")
        
    await bot.session.close()

if __name__ == "__main__":
    asyncio.run(test())
