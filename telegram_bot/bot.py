import os
import logging
from dotenv import load_dotenv
from telegram.ext import Application, CommandHandler, CallbackQueryHandler
from handlers import (
    start_command,
    help_command,
    status_command,
    weather_command,
    parse_command,
    subscribe_command,
    button_callback
)

# Загрузка переменных окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def main():
    """Запуск бота"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("❌ Переменная окружения TELEGRAM_BOT_TOKEN не найдена!")
        return

    logger.info("🚀 Запускаю бота...")
    
    # Создание приложения
    application = Application.builder().token(token).build()

    # Регистрация обработчиков команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("weather", weather_command))
    application.add_handler(CommandHandler("parse", parse_command))
    application.add_handler(CommandHandler("subscribe", subscribe_command))

    # Регистрация обработчика кнопок
    application.add_handler(CallbackQueryHandler(button_callback))

    # Запуск polling
    logger.info("✅ Бот успешно запущен и ждет сообщений")
    application.run_polling()

if __name__ == '__main__':
    main()
