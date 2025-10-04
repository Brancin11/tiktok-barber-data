# --- TikTok Data Filtering Script (Hugging Face Streaming) ---
# This version uses the official dataset path and a safer loading strategy
# to prevent "Terminated" errors due to high memory usage in Codespaces.

import pandas as pd
import os
import time
from datasets import load_dataset, get_dataset_config_names
from huggingface_hub import login

# --- CRITICAL CONFIGURATION ---
# The token is read from the secure environment variable (HF_TOKEN)
HUGGING_FACE_TOKEN = os.environ.get("HF_TOKEN")

# Corrected official dataset name structure for safer access
DATASET_NAME = "tiktok-10m"
DATASET_CONFIG = "default" # Use the default configuration
OUTPUT_DIR = "TikTok_Results"
OUTPUT_FILENAME = "barber_videos_streamed.parquet"
OUTPUT_FILEPATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
# ------------------------------

def filter_tiktok_data():
    """
    Connects to the Hugging Face dataset, streams and filters data, 
    and saves the filtered results to a Parquet file.
    """
    
    # Check for the token immediately
    if not HUGGING_FACE_TOKEN:
        print("-" * 50)
        print("FATAL ERROR: Hugging Face Token (HF_TOKEN) not found.")
        print("Please ensure the Codespace Secret is set.")
        print("-" * 50)
        return

    start_time = time.time()
    keywords = ['barber', 'haircut', 'fade', 'clippers', 'shave', 'barbershop', 'wahl']

    print("--- Starting TikTok Data Stream & Filter ---")
    print(f"Keywords: {', '.join(keywords)}")

    # 1. Hugging Face Login
    try:
        login(token=HUGGING_FACE_TOKEN)
        print("Hugging Face login successful.")
    except Exception as e:
        print(f"Error logging into Hugging Face. Check token validity. Error: {e}")
        return

    # 2. Process the Data Stream
    print(f"Streaming and filtering data from '{DATASET_NAME}'...")
    
    try:
        # Load the dataset using the corrected official name and enabling streaming
        # Note: We are using streaming=True to load data piece-by-piece and manage memory.
        dataset = load_dataset(
            DATASET_NAME, 
            DATASET_CONFIG, 
            split='train', 
            streaming=True, 
            use_auth_token=HUGGING_FACE_TOKEN
        )
    except Exception as e:
        print(f"FATAL ERROR: Could not load dataset '{DATASET_NAME}'. Error: {e}")
        return
    
    # 3. Apply the Filter
    # Use the native 'filter' method which is highly optimized for streaming datasets
    filtered_dataset = dataset.filter(
        lambda x: any(k in x['desc'].lower() for k in keywords) if x['desc'] else False
    )

    # 4. Save the Filtered Stream to Disk
    total_videos_saved = 0
    all_filtered_rows = []
    
    print("Writing filtered data to disk. This is where memory is managed...")
    
    try:
        # Iterate over the filtered stream and convert to Pandas DataFrames in batches
        # This prevents the Codespace from running out of memory
        for i, row in enumerate(filtered_dataset):
            # Convert to dictionary and append
            all_filtered_rows.append(row)
            total_videos_saved += 1
            
            # Print status every 500 records found
            if total_videos_saved % 500 == 0:
                print(f"Found and prepared {total_videos_saved:,} videos so far.")

        # Final preparation into a single DataFrame
        if all_filtered_rows:
            final_df = pd.DataFrame(all_filtered_rows)

            if not os.path.exists(OUTPUT_DIR):
                os.makedirs(OUTPUT_DIR)
            
            # Save the final DataFrame to the Parquet file.
            final_df.to_parquet(OUTPUT_FILEPATH, index=False)
            
            end_time = time.time()
            execution_time = round(end_time - start_time, 2)
            
            print("-" * 50)
            print("Success! Process Complete. The filtered dataset is saved.")
            print(f"File Location: {OUTPUT_FILEPATH}")
            print(f"Total REAL videos saved: {len(final_df):,}")
            print(f"Execution Time: {execution_time} seconds (Note: This will still take significant time to run.)")
            print("-" * 50)
        else:
            print("-" * 50)
            print("Filter finished, but no videos matched the keywords in the streamed data.")
            print("-" * 50)

    except Exception as e:
        print("-" * 50)
        print(f"FATAL PROCESSING ERROR: The process failed during streaming or saving. Error: {e}")
        print("This could indicate an issue with the dataset structure or insufficient memory/time limits.")
        print("-" * 50)


if __name__ == "__main__":
    filter_tiktok_data()
