# --- TikTok Data Filtering Script (Hugging Face Streaming) ---
# This script now connects directly to the TikTok-10M dataset on Hugging Face,
# streams the data in chunks, and filters for barber-related content.

import pandas as pd
import os
import time
from datasets import load_dataset, get_dataset_config_names
from huggingface_hub import login

# --- CRITICAL CONFIGURATION ---
# IMPORTANT: The token is now read from an environment variable (HF_TOKEN) 
# to keep it secure and out of the committed code.
HUGGING_FACE_TOKEN = os.environ.get("HF_TOKEN")

# The name of the dataset on Hugging Face
DATASET_NAME = "tiktok-10m/parquet_chunk_1"
OUTPUT_DIR = "TikTok_Results"
OUTPUT_FILENAME = "barber_videos_streamed.parquet"
OUTPUT_FILEPATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
# ------------------------------

def filter_tiktok_data():
    """
    Connects to the Hugging Face dataset, streams and filters chunks of data, 
    and saves the filtered results to a Parquet file.
    """
    
    # Check for the token immediately
    if not HUGGING_FACE_TOKEN:
        print("-" * 50)
        print("FATAL ERROR: Hugging Face Token (HF_TOKEN) not found.")
        print("Please set the 'HF_TOKEN' environment variable or Codespace Secret.")
        print("-" * 50)
        return

    start_time = time.time()
    total_videos_saved = 0
    keywords = ['barber', 'haircut', 'fade', 'clippers', 'shave', 'barbershop', 'wahl']

    print("--- Starting TikTok Data Stream & Filter ---")
    print(f"Keywords: {', '.join(keywords)}")

    # 1. Hugging Face Login
    try:
        # Use the token from the environment variable for login
        login(token=HUGGING_FACE_TOKEN)
        print("Hugging Face login successful.")
    except Exception as e:
        print(f"Error logging into Hugging Face. Check token validity. Error: {e}")
        return

    # 2. Get Dataset Stream Configuration
    try:
        # Load the dataset in streaming mode, using the environment token
        dataset = load_dataset(DATASET_NAME, split='train', streaming=True, use_auth_token=HUGGING_FACE_TOKEN)
    except Exception as e:
        print(f"FATAL ERROR: Could not load dataset '{DATASET_NAME}'. Check dataset name or token validity. Error: {e}")
        return

    # 3. Process the Data Stream
    print(f"Streaming and processing data from '{DATASET_NAME}'...")
    
    # We will process the data in batches (e.g., 50,000 rows at a time)
    batch_size = 50000
    current_batch = []
    
    # Prepare the output list for filtered data
    all_filtered_rows = []
    
    for i, row in enumerate(dataset):
        # Convert the single row from the stream to a list of records for easier DataFrame conversion
        current_batch.append(row)
        
        if (i + 1) % batch_size == 0:
            # When batch is full, process it
            df = pd.DataFrame(current_batch)
            current_batch = [] # Reset batch
            
            # Filtering Logic
            # The 'desc' field holds the video description
            filter_mask = df['desc'].str.contains('|'.join(keywords), case=False, na=False)
            df_filtered = df[filter_mask]
            
            all_filtered_rows.append(df_filtered)
            total_videos_saved += len(df_filtered)
            
            print(f"Processed {i + 1:,} rows. Found {total_videos_saved:,} matching videos so far.")
            
    # Process the last remaining batch (if any)
    if current_batch:
        df = pd.DataFrame(current_batch)
        filter_mask = df['desc'].str.contains('|'.join(keywords), case=False, na=False)
        df_filtered = df[filter_mask]
        all_filtered_rows.append(df_filtered)
        total_videos_saved += len(df_filtered)
        print(f"Processed final rows. Total matching videos: {total_videos_saved:,}")

    # 4. Final Save
    if total_videos_saved > 0:
        final_df = pd.concat(all_filtered_rows, ignore_index=True)
        
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        
        # Save the filtered DataFrame to the Parquet file.
        final_df.to_parquet(OUTPUT_FILEPATH, index=False)
        
        end_time = time.time()
        execution_time = round(end_time - start_time, 2)
        
        print("-" * 50)
        print("Success! Process Complete. The filtered dataset is saved.")
        print(f"File Location: {OUTPUT_FILEPATH}")
        print(f"Total REAL videos saved: {len(final_df):,}")
        print(f"Execution Time: {execution_time} seconds (Note: This will take significantly longer to stream the full dataset.)")
        print("-" * 50)
    else:
        print("-" * 50)
        print("Filter finished, but no videos matched the keywords in the streamed data.")
        print("-" * 50)


if __name__ == "__main__":
    filter_tiktok_data()
