"""
MMRdex Telegram Bot
Handles commands and sends notifications
"""
import logging
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, BufferedInputFile
from aiogram.filters import Command
from aiogram.enums import ParseMode

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_USER_ID, TELEGRAM_TOPIC_ID
from database import get_statistics, get_active_signals

logger = logging.getLogger(__name__)

# Create router for handlers
router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start command"""
    text = """
🤖 <b>MMRdex Bot</b>

Бот отслеживает арбитражные возможности от 10% 
между DEX и MEXC Futures.

<b>Критерии фильтрации:</b>
🟢 Токен торгуется на фьючерсах MEXC
🟢 Ликвидность на DEX от $50,000
🟢 Объём торгов на DEX от $30,000/24ч

<b>Сигналы:</b>
🟢 LONG — на MEXC дешевле (ожидается рост)
🔴 SHORT — на MEXC дороже (ожидается падение)

<b>Команды:</b>
/stats — статистика сигналов
/active — активные сигналы

Сигналы приходят автоматически! 🚀
"""
    await message.answer(text.strip(), parse_mode=ParseMode.HTML)


@router.message(Command("stats"))
async def cmd_stats(message: Message):
    """Handle /stats command - show statistics"""
    stats = await get_statistics()
    
    total = stats["wins"] + stats["draws"] + stats["loses"]
    if total > 0:
        win_rate = (stats["wins"] / total) * 100
    else:
        win_rate = 0
    
    text = f"""
📊 <b>Статистика MMRdex</b>
━━━━━━━━━━━━━━━━━━━━
📈 Всего сигналов: <b>{stats['total_signals']}</b>
📊 Avg Spread: <b>{stats['avg_spread']:+.1f}%</b>
📊 Avg Change: <b>{stats['avg_change']:+.1f}%</b>

<b>Результаты:</b>
🟢 Win: {stats['wins']} ({win_rate:.0f}%)
🟠 Draw: {stats['draws']}
🔴 Lose: {stats['loses']}
━━━━━━━━━━━━━━━━━━━━
"""
    await message.answer(text.strip(), parse_mode=ParseMode.HTML)


@router.message(Command("active"))
async def cmd_active(message: Message):
    """Handle /active command - show active signals"""
    signals = await get_active_signals()
    
    if not signals:
        await message.answer("📭 Нет активных сигналов")
        return
    
    text = f"📡 <b>Активные сигналы ({len(signals)})</b>\n━━━━━━━━━━━━━━━━━━━━\n"
    
    for s in signals[:10]:  # Limit to 10
        direction_emoji = "🟢" if s["direction"] == "LONG" else "🔴"
        text += f"{direction_emoji} ${s['token']} ({s['chain']}) | {s['spread_percent']:+.1f}%\n"
    
    if len(signals) > 10:
        text += f"\n... и ещё {len(signals) - 10} сигналов"
    
    await message.answer(text.strip(), parse_mode=ParseMode.HTML)


class TelegramBot:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.dp = Dispatcher()
        self.dp.include_router(router)
    
    async def start(self):
        """Start polling for messages"""
        logger.info("Starting Telegram bot...")
        
        # Fun startup message
        try:
            boot_text = """
🚀 <b>MMRdex Bot Online!</b>
━━━━━━━━━━━━━━━━━━━━
✅ Database connected
✅ Scanner initialized
✅ Tracker active

<i>Pumping bags...</i> 💼
"""
            await self.bot.send_message(
                chat_id=TELEGRAM_USER_ID,
                text=boot_text.strip(),
                parse_mode=ParseMode.HTML,
                message_thread_id=TELEGRAM_TOPIC_ID
            )
        except Exception as e:
            logger.error(f"Failed to send boot message: {e}")

        await self.dp.start_polling(self.bot)
    
    async def stop(self):
        """Stop the bot"""
        await self.bot.session.close()
    
    async def send_signal(self, message: str, chart_image: bytes = None):
        """Send signal notification to user, optionally with chart image"""
        try:
            if chart_image:
                # Send photo with caption
                photo = BufferedInputFile(chart_image, filename="chart.png")
                await self.bot.send_photo(
                    chat_id=TELEGRAM_USER_ID,
                    photo=photo,
                    caption=message,
                    parse_mode=ParseMode.HTML,
                    message_thread_id=TELEGRAM_TOPIC_ID
                )
            else:
                # Send text only
                await self.bot.send_message(
                    chat_id=TELEGRAM_USER_ID,
                    text=message,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    message_thread_id=TELEGRAM_TOPIC_ID
                )
        except Exception as e:
            logger.error(f"Failed to send signal: {e}")
    
    async def send_closure(self, message: str):
        """Send spread closure notification"""
        try:
            await self.bot.send_message(
                chat_id=TELEGRAM_USER_ID,
                text=message,
                parse_mode=ParseMode.HTML,
                message_thread_id=TELEGRAM_TOPIC_ID
            )
        except Exception as e:
            logger.error(f"Failed to send closure: {e}")

