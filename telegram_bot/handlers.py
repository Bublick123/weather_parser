from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import logging
from datetime import datetime
import requests

from utils import check_system_status, send_dag_run, get_latest_weather

logger = logging.getLogger(__name__)

# Эмодзи для статусов
EMOJI_STATUS = {
    'airflow': {'up': '✅', 'down': '❌'},
    'postgres': {'up': '✅', 'down': '❌'},
    'redis': {'up': '✅', 'down': '❌'},
    'celery': {'up': '✅', 'down': '❌'}
}

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    welcome_text = """
🌤️ <b>Добро пожаловать в Weather Parser Bot!</b>

Я управляю системой парсинга погоды на базе Apache Airflow.

<b>Доступные команды:</b>
/status - Статус системы
/parse - Запустить парсинг погоды
/weather - Посмотреть текущую погоду
/subscribe - Подписаться на уведомления
/help - Справка по командам

<b>Быстрые действия:</b>
"""
    
    keyboard = [
        [
            InlineKeyboardButton("🔄 Статус системы", callback_data="status"),
            InlineKeyboardButton("⚡ Запустить парсинг", callback_data="parse")
        ],
        [
            InlineKeyboardButton("🌡️ Текущая погода", callback_data="weather"),
            InlineKeyboardButton("🔔 Подписаться", callback_data="subscribe")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.effective_message.reply_text(
        welcome_text,
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status"""
    status = check_system_status()
    
    # Формируем красивый ответ
    status_text = "<b>📊 Статус системы Weather Parser</b>\n\n"
    
    for service, info in status.items():
        emoji = EMOJI_STATUS.get(service, {}).get('up' if info['status'] == 'up' else 'down', '⚪')
        status_text += f"{emoji} <b>{service.upper()}</b>\n"
        status_text += f"   Статус: {info['status']}\n"
        if info.get('details'):
            status_text += f"   Детали: {info['details']}\n"
        status_text += "\n"
    
    # Добавляем время последнего парсинга
    try:
        latest = get_latest_weather(limit=1)
        if latest:
            last_time = latest[0]['created_at'].strftime("%H:%M:%S")
            status_text += f"⏰ Последний парсинг: {last_time}"
    except:
        status_text += "⏰ Информация о парсинге недоступна"
    
    keyboard = [[InlineKeyboardButton("🔄 Обновить", callback_data="status")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.effective_message.reply_text(
        status_text,
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

async def parse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /parse"""
    keyboard = [
        [
            InlineKeyboardButton("🔄 Синхронный парсинг", callback_data="parse_sync"),
            InlineKeyboardButton("⚡ Асинхронный парсинг", callback_data="parse_async")
        ],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.effective_message.reply_text(
        "🔧 <b>Выберите тип парсинга:</b>\n\n"
        "🔄 <i>Синхронный</i> - классический, через Celery workers\n"
        "⚡ <i>Асинхронный</i> - быстрый, через asyncio\n",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

async def weather_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /weather"""
    try:
        weather_data = get_latest_weather(limit=6)
        
        if not weather_data:
            await update.effective_message.reply_text("❌ Нет данных о погоде. Запустите парсинг сначала.")
            return
        
        response = "🌤️ <b>Текущая погода в городах:</b>\n\n"
        
        for i, data in enumerate(weather_data, 1):
            response += f"{i}. <b>{data['city']}</b>\n"
            response += f"   🌡️ {data['temperature']}°C\n"
            response += f"   💧 {data['humidity']}% влажность\n"
            response += f"   📝 {data['description']}\n"
            response += f"   ⏰ {data['created_at'].strftime('%H:%M:%S')}\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="weather")],
            [InlineKeyboardButton("⚡ Запустить парсинг", callback_data="parse")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.effective_message.reply_text(
            response,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Ошибка получения погоды: {e}")
        await update.effective_message.reply_text("❌ Ошибка при получении данных о погоде.")

async def subscribe_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /subscribe"""
    chat_id = update.effective_chat.id
    
    # Здесь должна быть логика подписки (пока заглушка)
    keyboard = [
        [
            InlineKeyboardButton("✅ Подписаться", callback_data="subscribe_on"),
            InlineKeyboardButton("❌ Отписаться", callback_data="subscribe_off")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.effective_message.reply_text(
        "🔔 <b>Управление подпиской</b>\n\n"
        "Получайте уведомления:\n"
        "• ✅ Успешный парсинг\n"
        "• ❌ Ошибки при парсинге\n"
        "• ⚡ Аномальная температура\n"
        "• 🔄 Завершение DAG\n",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = """
🆘 <b>Справка по командам</b>

<b>Основные команды:</b>
/start - Начать работу с ботом
/status - Проверить статус системы
/parse - Запустить парсинг погоды
/weather - Посмотреть текущую погоду
/subscribe - Подписаться на уведомления
/help - Эта справка

<b>Быстрые действия через кнопки:</b>
• Статус системы - проверяет Airflow, БД, Redis
• Запустить парсинг - запускает сбор погоды
• Текущая погода - показывает последние данные
• Подписаться - включает/выключает уведомления

<b>Что парсим:</b>
• Москва, Лондон, Берлин, Париж, Токио
• Температура, влажность, описание погоды
• Обновляется ежедневно в 12:00 (UTC)

<b>Технологии:</b>
• Apache Airflow (оркестрация)
• PostgreSQL (хранение данных)
• Redis (очередь задач)
• Celery (обработка задач)
• FastAPI (веб-интерфейс)
"""
    
    await update.effective_message.reply_text(help_text, parse_mode="HTML")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback от inline кнопок"""
    query = update.callback_query
    await query.answer()  # Ответим на callback, чтобы убрать "часики"
    
    data = query.data
    
    if data == "status":
        await status_command(update, context)
    elif data == "weather":
        await weather_command(update, context)
    elif data == "parse":
        await parse_command(update, context)
    elif data == "subscribe":
        await subscribe_command(update, context)
    elif data == "parse_sync":
        # Запуск синхронного DAG
        result = send_dag_run("weather_parser_dag")
        if result:
            await query.edit_message_text(
                "✅ <b>Синхронный парсинг запущен!</b>\n\n"
                f"DAG Run ID: {result['dag_run_id']}\n"
                "Отслеживайте выполнение в Airflow UI.\n\n"
                "Используйте /status для проверки статуса.",
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text("❌ Ошибка запуска DAG")
    elif data == "parse_async":
        # Запуск асинхронного DAG
        result = send_dag_run("async_weather_parser_dag")
        if result:
            await query.edit_message_text(
                "⚡ <b>Асинхронный парсинг запущен!</b>\n\n"
                f"DAG Run ID: {result['dag_run_id']}\n"
                "Этот метод быстрее, использует asyncio.\n\n"
                "Используйте /status для проверки статуса.",
                parse_mode="HTML"
            )
        else:
            await query.edit_message_text("❌ Ошибка запуска DAG")
    elif data == "cancel":
        await query.edit_message_text("❌ Действие отменено.")