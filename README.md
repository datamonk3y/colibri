# Introduction
This pipeline uses docker to create both a pyspark notebook environment and a postgres database instance for answering the Colibri technical assessment.

# Assumptions
There are 5 Columns within the provided sample files
* timestamp - Timestamp representing the time individual readings were too. 1 record per hour, per turbine, 24 hours a day
* turbine_id - Integer value representing a unique identifier for individual turbines, 15 in total
* wind_speed - Float value representing wind speed
* wind_direction - Integer value between 0 and 359, each representing single degree from a complete rotation
* power_output - Float value representing power output in MW

# Setup
Setup environment using docker, enter root directory using your terminal of choice and type:
```
docker compose up notebook
```

To stop and remove containers, enter the root directory and type
```
docker compose down
```

# Running the pipeline
After running the `docker compose up notebook` command, your terminal will output a link similar to the below in the output (Note, TOKEN has been redacted)
```
http://127.0.0.1:8888/lab?token=<TOKEN VALUE>
```

Opening this link in a browser will open a Jupyter notebook instance running in a docker container.
Navigate to `notebooks/turbines.ipynb` to view the pipeline.
This will need to be run manually, as no orchestration is included in this pipeline.

# Postgres
It is also possible to connect to the postgres container directly from your local machine, for simplicity most credential information is the same
* Hostname=localhost
* Port=5432
* Database=postgres
* Username=postgres
* Password=postgres
