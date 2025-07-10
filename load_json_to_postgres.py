import os
import json
from pathlib import Path
import logging
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    filename='load_json_to_postgres.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# PostgreSQL credentials
db_params = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}

# Data Lake directory
DATA_LAKE_PATH = Path('data/raw/telegram_messages')

def create_raw_table(cursor):
    """Create the raw.telegram_messages table if it doesn't exist."""
    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        message_id BIGINT,
        channel_name VARCHAR(255),
        message_date TIMESTAMP,
        text TEXT,
        has_image BOOLEAN,
        sender_id BIGINT,
        image_path TEXT,
        scraped_date DATE,
        PRIMARY KEY (message_id, channel_name, scraped_date)
    );
    """
    cursor.execute(create_table_query)
    logger.info("Created raw.telegram_messages table")

def load_json_to_postgres():
    """Load JSON files from Data Lake into PostgreSQL."""
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Create raw table
        create_raw_table(cursor)

        # Iterate through JSON files in Data Lake
        for json_file in DATA_LAKE_PATH.rglob('*/messages.json'):
            scraped_date = json_file.parent.parent.name  # e.g., 2025-07-10
            channel_name = json_file.parent.name  # e.g., tenamereja

            logger.info(f"Processing {json_file}")
            with json_file.open('r', encoding='utf-8') as f:
                messages = json.load(f)

            # Insert each message into PostgreSQL
            insert_query = """
            INSERT INTO raw.telegram_messages (
                message_id, channel_name, message_date, text, has_image, sender_id, image_path, scraped_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (message_id, channel_name, scraped_date) DO NOTHING;
            """
            for msg in messages:
                try:
                    cursor.execute(insert_query, (
                        msg['id'],
                        channel_name,
                        msg['date'],
                        msg['text'] if msg['text'] else None,
                        msg['has_image'],
                        msg['sender_id'],
                        msg.get('image_path'),
                        scraped_date
                    ))
                    logger.info(f"Inserted message {msg['id']} from {channel_name}")
                except Exception as e:
                    logger.error(f"Error inserting message {msg['id']} from {channel_name}: {str(e)}")

            connection.commit()
            logger.info(f"Loaded {len(messages)} messages from {json_file}")

    except (Exception, Error) as error:
        logger.error(f"Error loading JSON to PostgreSQL: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            logger.info("PostgreSQL connection closed")

if __name__ == '__main__':
    load_json_to_postgres()