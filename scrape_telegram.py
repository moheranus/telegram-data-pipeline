import os
import json
import logging
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from dotenv import load_dotenv
import asyncio

# Configure logging
logging.basicConfig(
    filename='scrape_telegram.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Telegram API credentials
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('TELEGRAM_PHONE')

# List of Telegram channels to scrape
channels = [
    'CheMed123',
    'lobelia4cosmetics',
    'tikvahpharma',
    'Thequorachannel',
    'tenamereja'
]

# Data Lake directory
DATA_LAKE_PATH = Path('data/raw/telegram_messages')

async def scrape_channel(client, channel_name):
    """Scrape messages and images from a Telegram channel."""
    try:
        # Get the channel entity
        entity = await client.get_entity(f't.me/{channel_name}')
        logger.info(f"Scraping channel: {channel_name}")

        # Create directory for today's data
        today = datetime.now().strftime('%Y-%m-%d')
        output_dir = DATA_LAKE_PATH / today / channel_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # Collect messages
        messages_data = []
        async for message in client.iter_messages(entity, limit=100):  # Adjust limit as needed
            msg_data = {
                'id': message.id,
                'date': message.date.isoformat(),
                'text': message.text,
                'has_image': bool(message.photo),
                'sender_id': message.sender_id
            }

            # Download image if present
            if message.photo:
                image_path = output_dir / f'message_{message.id}.jpg'
                await client.download_media(message.photo, file=image_path)
                msg_data['image_path'] = str(image_path)
                logger.info(f"Downloaded image for message {message.id} in {channel_name}")

            messages_data.append(msg_data)
            await asyncio.sleep(1)  # Avoid rate limits

        # Save messages to JSON
        json_path = output_dir / 'messages.json'
        with json_path.open('w', encoding='utf-8') as f:
            json.dump(messages_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(messages_data)} messages to {json_path}")

    except FloodWaitError as e:
        logger.error(f"FloodWaitError for {channel_name}: Must wait {e.seconds} seconds")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.error(f"Error scraping channel {channel_name}: {str(e)}")

async def main():
    """Main function to scrape all channels."""
    # Initialize Telegram client
    client = TelegramClient('session_name', api_id, api_hash)

    try:
        await client.start(phone=phone)
        logger.info("Telegram client started successfully")

        # Scrape each channel
        for channel in channels:
            try:
                await scrape_channel(client, channel)
            except FloodWaitError as e:
                logger.error(f"FloodWaitError for {channel}: Must wait {e.seconds} seconds")
                await asyncio.sleep(e.seconds)

    except SessionPasswordNeededError:
        logger.error("Two-factor authentication required. Please provide your password.")
    except FloodWaitError as e:
        logger.error(f"FloodWaitError in main: Must wait {e.seconds} seconds")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        await client.disconnect()
        logger.info("Telegram client disconnected")

if __name__ == '__main__':
    asyncio.run(main())