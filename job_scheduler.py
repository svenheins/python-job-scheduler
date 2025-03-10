import sqlite3
import os
import subprocess
import argparse
import json
import logging
from datetime import datetime
import time
import threading
import glob

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("scheduler.log"), logging.StreamHandler()],
)


class JobScheduler:
    def __init__(self, db_path="jobs.db"):
        self.db_path = db_path
        self.setup_database()
        self._stop_event = threading.Event()

    def setup_database(self):
        """Create the database and tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Create jobs table
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            input_file TEXT UNIQUE,
            status TEXT,
            start_time TEXT,
            end_time TEXT,
            exit_code INTEGER,
            attempts INTEGER DEFAULT 0
        )
        """
        )

        conn.commit()
        conn.close()

    def add_job(self, input_file):
        """Add a new job to the queue if it doesn't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute(
                "INSERT OR IGNORE INTO jobs (input_file, status, attempts) VALUES (?, ?, ?)",
                (input_file, "pending", 0),
            )
            conn.commit()
            logging.info(f"Added job for input file: {input_file}")
        except sqlite3.Error as e:
            logging.error(f"Error adding job: {e}")
        finally:
            conn.close()

    def remove_job(self, input_file):
        """Remove a job from the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # First, log the job status before deletion
            cursor.execute(
                "SELECT id, status FROM jobs WHERE input_file = ?", (input_file,)
            )
            job = cursor.fetchone()

            if job:
                job_id, status = job
                cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
                conn.commit()
                logging.info(
                    f"Removed job {job_id} for input file: {input_file} (previous status: {status})"
                )
            else:
                logging.warning(
                    f"Attempted to remove non-existent job for file: {input_file}"
                )
        except sqlite3.Error as e:
            logging.error(f"Error removing job: {e}")
        finally:
            conn.close()

    def get_all_job_files(self):
        """Get all files registered in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT input_file FROM jobs")
        job_files = [row[0] for row in cursor.fetchall()]
        conn.close()

        return job_files

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
        """Execute the job and update its status"""
        try:
            with open("command.json") as f:
                command_config = json.load(f)
        except Exception as e:
            logging.error(f"Error loading command.json: {e}")
            return "failed"

        # Check if file still exists before processing
        if not os.path.exists(input_file):
            logging.warning(
                f"Input file {input_file} no longer exists. Removing job {job_id}"
            )
            self.remove_job(input_file)
            return "removed"

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Update job status to running
        cursor.execute(
            "UPDATE jobs SET status = ?, start_time = ?, attempts = attempts + 1 WHERE id = ?",
            ("running", datetime.now().isoformat(), job_id),
        )
        conn.commit()
        conn.close()

        logging.info(f"Starting job {job_id} for input file: {input_file}")

        try:
            # Run the actual script
            cmd = [cmd_i for cmd_i in command_config.values()]
            cmd.extend(["--input_file", input_file])
            logging.info(f"Job = {str(cmd)}")
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
            (status, datetime.now().isoformat(), exit_code, job_id),
        )
        conn.commit()
        conn.close()

        logging.info(f"Job {job_id} completed with status: {status}")
        return status

    def run_pending_jobs(self, max_jobs=None):
        """Run all pending jobs, optionally limited to max_jobs"""
        waiting_timer = 1
        while not self._stop_event.is_set():
            pending_jobs = self.get_pending_jobs()

            if max_jobs:
                pending_jobs = pending_jobs[:max_jobs]

            if pending_jobs:
                logging.info(f"Running {len(pending_jobs)} jobs...")
                for job_id, input_file in pending_jobs:
                    waiting_timer = 1
                    self.run_job(job_id, input_file)
                logging.info("All jobs completed. Waiting for more jobs...")
            else:
                # wait for some time before checking again
                waiting_timer *= 2
                waiting_timer = min(waiting_timer, 300)
                logging.info(
                    f"No more pending jobs. Waiting for {waiting_timer} seconds before checking again..."
                )
                # Use stop_event.wait() instead of time.sleep() to allow graceful shutdown
                self._stop_event.wait(waiting_timer)

    def monitor_directory(self, monitor_dir, pattern="*"):
        """Monitor a directory for new files and removed files"""
        monitor_dir = os.path.abspath(monitor_dir)
        logging.info(f"Starting to monitor directory: {monitor_dir}")

        # Keep track of processed files
        processed_files = set()

        while not self._stop_event.is_set():
            current_files = set()
            try:
                # Use glob to find all files matching the pattern
                pattern_path = os.path.join(monitor_dir, pattern)
                for file_path in glob.glob(pattern_path):
                    if os.path.isfile(file_path):
                        current_files.add(file_path)

                # Find new files and add them
                new_files = current_files - processed_files
                for file_path in new_files:
                    self.add_job(file_path)

                # Find removed files and delete them from the database
                removed_files = processed_files - current_files
                for file_path in removed_files:
                    self.remove_job(file_path)

                # Additionally, check all files in the database and remove any that don't exist
                self.sync_database_with_filesystem(monitor_dir, pattern)

                # Update processed files
                processed_files = current_files

            except Exception as e:
                logging.error(f"Error monitoring directory: {e}")

            # Check every 5 seconds
            self._stop_event.wait(5)

    def sync_database_with_filesystem(self, monitor_dir=None, pattern=None):
        """Ensure database is in sync with the filesystem by removing entries for non-existent files"""
        try:
            all_job_files = self.get_all_job_files()
            for file_path in all_job_files:
                # Only check files that should be in the monitored directory
                if monitor_dir is None or (
                    file_path.startswith(monitor_dir)
                    and os.path.basename(file_path) == pattern
                ):
                    if not os.path.exists(file_path):
                        logging.info(
                            f"File {file_path} no longer exists. Removing from database"
                        )
                        self.remove_job(file_path)
        except Exception as e:
            logging.error(f"Error syncing database with filesystem: {e}")

    def stop(self):
        """Signal all threads to stop"""
        self._stop_event.set()


def main():
    parser = argparse.ArgumentParser(
        description="Job scheduler for managing input file processing"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Add job command
    add_parser = subparsers.add_parser("add", help="Add job(s) to the queue")
    add_parser.add_argument("--input-file", help="Single input file to process")
    add_parser.add_argument("--input-dir", help="Directory with input files to process")
    add_parser.add_argument(
        "--pattern", default="*", help="File pattern to match in input directory"
    )

    # Run jobs command
    run_parser = subparsers.add_parser("run", help="Run pending jobs")
    run_parser.add_argument(
        "--max-jobs", type=int, help="Maximum number of jobs to run"
    )

    # List jobs command
    list_parser = subparsers.add_parser("list", help="List all jobs")
    list_parser.add_argument("--status", help="Filter by status")

    # Monitor command
    monitor_parser = subparsers.add_parser(
        "monitor", help="Monitor a directory for new files"
    )
    monitor_parser.add_argument(
        "--monitor-dir", required=True, help="Directory to monitor"
    )
    monitor_parser.add_argument(
        "--pattern", default="*", help="File pattern to match in monitored directory"
    )

    # Combined monitor and run command
    monitor_run_parser = subparsers.add_parser(
        "monitor-and-run", help="Monitor a directory and run jobs"
    )
    monitor_run_parser.add_argument(
        "--monitor-dir", required=True, help="Directory to monitor"
    )
    monitor_run_parser.add_argument(
        "--pattern", default="*", help="File pattern to match in monitored directory"
    )
    monitor_run_parser.add_argument(
        "--max-jobs", type=int, help="Maximum number of jobs to run"
    )

    # Sync command
    sync_parser = subparsers.add_parser(
        "sync", help="Sync database with filesystem (remove entries for missing files)"
    )

    args = parser.parse_args()
    scheduler = JobScheduler()

    if args.command == "add":
        if args.input_file:
            scheduler.add_job(os.path.abspath(args.input_file))
        elif args.input_dir:
            pattern = os.path.join(os.path.abspath(args.input_dir), args.pattern)
            for file_path in glob.glob(pattern):
                if os.path.isfile(file_path):
                    scheduler.add_job(file_path)

    elif args.command == "run":
        try:
            scheduler.run_pending_jobs(args.max_jobs)
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, shutting down...")
            scheduler.stop()

    elif args.command == "list":
        conn = sqlite3.connect(scheduler.db_path)
        cursor = conn.cursor()

        query = (
            "SELECT id, input_file, status, start_time, end_time, attempts FROM jobs"
        )
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
                print(
                    " | ".join(
                        str(field) if field is not None else "-" for field in job
                    )
                )

        conn.close()

    elif args.command == "monitor":
        try:
            scheduler.monitor_directory(args.monitor_dir, args.pattern)
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, shutting down...")
            scheduler.stop()

    elif args.command == "monitor-and-run":
        try:
            # Start monitor thread
            monitor_thread = threading.Thread(
                target=scheduler.monitor_directory,
                args=(args.monitor_dir, args.pattern),
            )
            monitor_thread.daemon = True
            monitor_thread.start()

            # Run jobs in the main thread
            scheduler.run_pending_jobs(args.max_jobs)
        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, shutting down...")
            scheduler.stop()

    elif args.command == "sync":
        scheduler.sync_database_with_filesystem()
        print("Database synchronized with filesystem.")


if __name__ == "__main__":
    main()
