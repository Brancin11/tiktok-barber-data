import pandas as pd
import os
import time

# --- Configuration ---
# The target output file name
OUTPUT_PARQUET_FILE = 'TikTok_Results/barber_videos_streamed.parquet'
# Keywords to search for in the video descriptions
SEARCH_KEYWORDS = [
    "barber", "haircut", "fades", "barberlife", "taper", 
    "lineup", "shave", "barbershop", "hairstylist", "clippers"
]
# ---------------------

def filter_tiktok_data():
    """
    Simulates loading and filtering a large dataset for barber-related content.
    The goal is to extract all columns for videos matching the search keywords.
    """
    print("--- Starting TikTok Data Filter ---")
    start_time = time.time()
    
    # 1. Create a mock dataset to simulate the 10 million videos.
    # In a real Codespace environment, this part would read the actual large file.
    # For demonstration, we create a small, representative DataFrame.
    print("Simulating loading of large TikTok dataset...")
    
    data = {
        'video_id': range(10),
        'user_id': [f'user_{i % 3}' for i in range(10)],
        'description': [
            "Best fade haircut I've ever seen! #barberlife",
            "Cute puppies playing in the park. So sweet.",
            "Amazing taper and lineup today. Client happy.",
            "New recipe for pasta primavera. Delicious!",
            "My morning routine at the barbershop.",
            "Vlog of my trip to Italy last summer.",
            "Fresh shave and edge up. #barber",
            "Tips for becoming a better graphic designer.",
            "Just finished a great skin fade. Love the job!",
            "Unboxing the new gaming console."
        ],
        'views': [15000, 500, 25000, 1200, 30000, 900, 18000, 700, 45000, 2000],
        'likes': [1500, 50, 2500, 120, 3000, 90, 1800, 70, 4500, 200],
        'video_date': pd.to_datetime(['2023-10-01', '2023-09-20', '2023-10-02', '2023-09-25', '2023-10-03', '2023-09-28', '2023-10-04', '2023-09-26', '2023-10-05', '2023-09-27'])
    }
    
    df = pd.DataFrame(data)
    print(f"Simulated DataFrame with {len(df)} rows and {len(df.columns)} columns loaded.")
    
    # 2. Filtering Logic
    # Combine all keywords into a single regex pattern for efficient searching (case-insensitive)
    pattern = '|'.join(SEARCH_KEYWORDS)
    
    # Create a boolean mask where the description column contains any of the keywords
    # We use .astype(str) to ensure the column is treated as strings for searching
    mask = df['description'].astype(str).str.contains(pattern, case=False, na=False)
    
    # Apply the mask to get the filtered DataFrame
    df_filtered = df[mask].copy()

    print(f"Filtering complete. Found {len(df_filtered)} matching videos.")
    
    if df_filtered.empty:
        print("No videos matched the filter criteria in the simulated dataset.")
        return

    # 3. Save the Output (Parquet format)
    
    # Ensure the output directory exists
    output_dir = os.path.dirname(OUTPUT_PARQUET_FILE)
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the filtered DataFrame to a Parquet file.
    # The Parquet format preserves column types and is efficient for large datasets.
    df_filtered.to_parquet(OUTPUT_PARQUET_FILE, index=False, engine='pyarrow', compression='snappy')
    
    end_time = time.time()
    
    print("-" * 50)
    print(f"Process Complete. The filtered dataset is saved and ready for download.")
    print(f"File Location: {OUTPUT_PARQUET_FILE}")
    print(f"Total rows (videos) saved: {len(df_filtered)}")
    print(f"Execution Time: {end_time - start_time:.2f} seconds (Note: Real execution will take longer)")
    print("-" * 50)

if __name__ == "__main__":
    filter_tiktok_data()
