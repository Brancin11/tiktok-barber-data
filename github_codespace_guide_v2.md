
Simple Guide: Running Your Data Filter on GitHub (All Video Data)
This guide shows you, step-by-step, how to use GitHub’s powerful cloud computer (called a Codespace) to run the data filter script and get your data.

Key Update: The final output file will now contain ALL the original video information (views, likes, user ID, video date, etc.) for every filtered video, not just the description.

Phase 1: Start the Cloud Computer
Go to your GitHub Project: Navigate to the page where your three files are stored (tiktok_barber_stream.py, requirements.txt, and this guide).

Find the Code Button: Look for the green "Code" button near the top right of the file list.

Start Codespace:

Click the "Code" button.

In the menu that appears, click the "Codespaces" tab.

Click the button that says "Create codespace on main".

It will take a minute or two to start up. When it's ready, a new page will open in your browser that looks like a simplified version of a program editor.

Phase 2: Get the Tools Ready and Run the Code
This editor has a main area, a list of files on the left, and a black box (the Terminal) at the bottom. We need to type two commands into that black box.

Step A: Install the Tools
Click inside the black box at the bottom of the screen.

Type this exact command and press Enter:

pip install -r requirements.txt

(This step downloads the necessary software like pandas and datasets.)

Step B: Run the Script
After the previous command finishes (it will show a new blinking line), type this exact command and press Enter:

python tiktok_barber_stream.py

(This starts the huge task of checking 10 million videos.)

IMPORTANT: The script will print progress updates (e.g., "Found X videos so far..."). This process is slow and will take anywhere from 10 to 30 minutes. Do not close the tab or turn off your computer until you see the message: "Process Complete. The filtered dataset is saved and ready for download."

Phase 3: Find and Download Your Filtered Data
When the script is complete, your filtered data is ready.

Check the File List: Look at the list of files on the left side of the screen (the File Explorer).

Find the Folder: A new folder called TikTok_Results will appear in the list.

Find the File: Click on the TikTok_Results folder. Inside, you will see your final data file: barber_videos_streamed.parquet.

Download to Your Computer:

Right-click on the barber_videos_streamed.parquet file.

Select "Download..." from the menu.

The file is now saved to your computer's Downloads folder!

Phase 4: What is the .parquet file?
The file is named barber_videos_streamed.parquet.

Parquet is just a special, efficient way to save analytical data.

To view it: You will need a program that can read Parquet files. Since this file now contains more complex metadata (like nested columns), the best way to view it is by using a simple Python script on your own machine (e.g., in a Jupyter Notebook) or a dedicated data viewing too
