# python-job-scheduler

Running a long lasting python job based on files in a directory

## add an input file to the database

```
python job_scheduler.py add --input-file test.json
```

## run the queue

```
python job_scheduler.py run
```

## example usage: Alphafold3

The idea is to copy any protein signature (json) to a directory (/server/project/username/alphafold3/input/) in this case. And let the scheduler start the alphafold process, which takes roughly 50 minutes on a H100 (inference: 1 minute per seed).

1. Go to /app/alphafold
2. ensure that it has the right user write privileges (a sqlite database is created locally for the jobs), test this by running "touch test.db"
3. copy the job_scheduler and command.json to /app/alphafold

Adjust the command.json accordingly:

```
{
    "0": "python",
    "1": "run_alphafold.py",
    "2": "--model_dir=/server/project/username/alphafold3/model_parameters",
    "3": "--db_dir=/server/project/username/alphafold3/database",
    "4": "--output_dir=/server/project/username/alphafold3/output"
}
```

Finally, run the following command from /app/alphafold:

```
python job_scheduler.py monitor-and-run --monitor-dir /server/project/username/alphafold3/input/
```
