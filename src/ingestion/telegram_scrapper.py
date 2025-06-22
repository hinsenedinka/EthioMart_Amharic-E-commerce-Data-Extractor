from telethon import TelegramClient
import csv
import os
from dotenv import load_dotenv

# Load environment variables once
load_dotenv('.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Function to get the last message ID for each channel from the existing CSV
def get_last_message_ids(csv_filepath):
    last_ids = {}
    if os.path.exists(csv_filepath) and os.path.getsize(csv_filepath) > 0:
        with open(csv_filepath, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            try:
                header = next(reader) # Skip header row
                # Find column indices dynamically in case order changes
                username_idx = header.index('Channel Username')
                id_idx = header.index('ID')
            except (StopIteration, ValueError): # Handle empty file or missing header
                print(f"Warning: CSV '{csv_filepath}' is empty or header is missing/malformed. Starting fresh.")
                return {} # If header is bad or file empty, cannot resume intelligently

            for row in reader:
                try:
                    channel_username = row[username_idx]
                    message_id = int(row[id_idx])
                    # Update last_ids only if current message_id is higher
                    last_ids[channel_username] = max(last_ids.get(channel_username, 0), message_id)
                except (IndexError, ValueError):
                    # Handle rows that might be malformed or missing data, skip them
                    continue
    return last_ids

# Function to scrape data from a single channel
# Added min_id parameter
async def scrape_channel(client, channel_username, writer, min_id=0):
    entity = await client.get_entity(channel_username)
    channel_title = entity.title  # Extract the channel's title

    # Use min_id to only get messages strictly newer than the last scraped one
    # limit=None means fetch all messages until min_id is reached (or no more messages)
    async for message in client.iter_messages(entity, limit=1000, min_id=min_id):
        writer.writerow([channel_title, channel_username, message.id, message.message, message.date])

# Initialize the client once
client = TelegramClient('scraping_session', api_id, api_hash)

async def main():
    await client.start()

    csv_filepath = 'telegram_data.csv'
    # Get the last scraped message IDs before opening the file for appending
    last_scraped_ids = get_last_message_ids(csv_filepath)

    # Check if the file exists and has content to decide if header needs to be written
    file_exists_and_not_empty = os.path.exists(csv_filepath) and os.path.getsize(csv_filepath) > 0

    # Open the CSV file in append mode ('a')
    with open(csv_filepath, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Write header only if the file was just created or was empty
        if not file_exists_and_not_empty:
            writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date'])

        # List of channels to scrape
        channels = [
            '@Shageronlinestore','@gebeyaadama','@marakibrand','@aradabrand2','@marakisat2','@qnashcom'
        ]

        # Iterate over channels and scrape data into the single CSV file
        for channel in channels:
            # Get the last ID for the current channel; default to 0 if not found (new channel)
            current_min_id = last_scraped_ids.get(channel, 0)
            await scrape_channel(client, channel, writer, current_min_id) # Pass min_id
            print(f"Scraped data from {channel} starting from message ID > {current_min_id}")

with client:
    client.loop.run_until_complete(main())