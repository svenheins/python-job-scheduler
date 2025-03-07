import time
import random
import argparse
import os

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Run for a random duration plus input file name character count.")
parser.add_argument("--input-file", type=str, required=True, help="Path to the input file.")
args = parser.parse_args()

# Extract the file name and count its characters
file_name = os.path.basename(args.input_file)
char_count = len(file_name)

# Generate a random sleep duration between 1 and 5 seconds and add character count
sleep_duration = random.randint(1, 5) + char_count

print(f"Running for {sleep_duration} seconds...")
time.sleep(sleep_duration)

print("Done!")
