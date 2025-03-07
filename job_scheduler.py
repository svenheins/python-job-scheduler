import sqlite3
import os
import subprocess
import argparse
import json
import logging
from datetime import datetime
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("scheduler.log"), logging.StreamHandler()]
)

class JobScheduler:
    def __init__(self, db_path="jobs.db"):
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        """Create the database and tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create jobs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            input_file TEXT UNIQUE,
            status TEXT,
            start_time TEXT,
            end_time TEXT,
            exit_code INTEGER,
            attempts INTEGER DEFAULT 0
        )
        ''')
        
        conn.commit()
        conn.close()
        
    def add_job(self, input_file):
        """Add a new job to the queue if it doesn't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO jobs (input_file, status, attempts) VALUES (?, ?, ?)",
                (input_file, "pending", 0)
            )
            conn.commit()
            logging.info(f"Added job for input file: {input_file}")
        except sqlite3.Error as e:
            logging.error(f"Error adding job: {e}")
        finally:
            conn.close()
    
    def get_pending_jobs(self):
        """Get all jobs that haven't been completed successfully"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id, input_file FROM jobs WHERE status IN ('pending', 'failed')"
        )
        jobs = cursor.fetchall()
        conn.close()
        
        return jobs
    
    def run_job(self, job_id, input_file):
        with open('command.json') as f:
            command_config = json.load(f)
        """Execute the job and update its status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Update job status to running
        cursor.execute(
            "UPDATE jobs SET status = ?, start_time = ?, attempts = attempts + 1 WHERE id = ?",
            ("running", datetime.now().isoformat(), job_id)
        )
        conn.commit()
        conn.close()
        
        logging.info(f"Starting job {job_id} for input file: {input_file}")
        
        try:
            # Run the actual script
            cmd = [cmd_i for cmd_i in command_config.values()]
            cmd.extend(["--input-file", input_file])
            process = subprocess.run(cmd, check=True)
            exit_code = process.returncode
            status = "completed" if exit_code == 0 else "failed"
        except Exception as e:
            logging.error(f"Error executing job {job_id}: {e}")
            exit_code = 1
            status = "failed"
        
        # Update job status after execution
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE jobs SET status = ?, end_time = ?, exit_code = ? WHERE id = ?",
            (status, datetime.now().isoformat(), exit_code, job_id)
        )
        conn.commit()
        conn.close()
        
        logging.info(f"Job {job_id} completed with status: {status}")
        return status
    
    def run_pending_jobs(self, max_jobs=None):
        """Run all pending jobs, optionally limited to max_jobs"""
        waiting_timer = 1
        while True:
            pending_jobs = self.get_pending_jobs()
            
            if max_jobs:
                pending_jobs = pending_jobs[:max_jobs]
            
            logging.info(f"Running {len(pending_jobs)} jobs...")
            for job_id, input_file in pending_jobs:
                waiting_timer = 1
                self.run_job(job_id, input_file)
            logging.info("All jobs completed. Waiting for more jobs...")
            
            if not pending_jobs:
                # wait for some time before checking again
                waiting_timer *= 2
                waiting_timer = min(waiting_timer, 300)
                logging.info(f"No more pending jobs. Waiting for {waiting_timer} seconds before checking again...")
                time.sleep(waiting_timer)
                
                


def main():
    parser = argparse.ArgumentParser(description="Job scheduler for managing input file processing")
    subparsers = parser.add_subparsers(dest="command")
    
    # Add job command
    add_parser = subparsers.add_parser("add", help="Add job(s) to the queue")
    add_parser.add_argument("--input-file", help="Single input file to process")
    add_parser.add_argument("--input-dir", help="Directory with input files to process")
    add_parser.add_argument("--pattern", default="*", help="File pattern to match in input directory")
    
    # Run jobs command
    run_parser = subparsers.add_parser("run", help="Run pending jobs")
    run_parser.add_argument("--max-jobs", type=int, help="Maximum number of jobs to run")
    
    # List jobs command
    list_parser = subparsers.add_parser("list", help="List all jobs")
    list_parser.add_argument("--status", help="Filter by status")
    
    args = parser.parse_args()
    scheduler = JobScheduler()
    
    if args.command == "add":
        if args.input_file:
            scheduler.add_job(os.path.abspath(args.input_file))
        elif args.input_dir:
            import glob
            pattern = os.path.join(os.path.abspath(args.input_dir), args.pattern)
            for file_path in glob.glob(pattern):
                if os.path.isfile(file_path):
                    scheduler.add_job(file_path)
    
    elif args.command == "run":
        scheduler.run_pending_jobs(args.max_jobs)
    
    elif args.command == "list":
        conn = sqlite3.connect(scheduler.db_path)
        cursor = conn.cursor()
        
        query = "SELECT id, input_file, status, start_time, end_time, attempts FROM jobs"
        params = []
        
        if args.status:
            query += " WHERE status = ?"
            params.append(args.status)
        
        cursor.execute(query, params)
        jobs = cursor.fetchall()
        
        if not jobs:
            print("No jobs found.")
        else:
            print("ID | Input File | Status | Start Time | End Time | Attempts")
            print("-" * 80)
            for job in jobs:
                print(" | ".join(str(field) if field is not None else "-" for field in job))
        
        conn.close()

if __name__ == "__main__":
    main()