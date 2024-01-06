# etletletl
For design, see DESIGN.md. 

## Environment
I used https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook for the container image.
Follow the next few sections for testing the pipeline in this container.

## Run pipeline
To start the container (which exposes ports for a Jupyter notebook and the Spark UI)
```
docker run --name suspicious_etl -detach --rm -w /home/jovyan/work -v "${PWD}":/home/jovyan/work quay.io/jupyter/pyspark-notebook:2023-10-20
```

You can optionally add `-p <local-port>:8888 -p <another-local-port>:4040` to expose the ports for Jupyter notebooks and Spark UI.

Then run the ETL pipeline
```
docker exec -it suspicious_etl /usr/local/spark/bin/spark-submit --master local\[4\] ./sensors_etl_pipeline.py
```
which runs the pipeline with 4 threads. Depending on your shell, you may have to drop the espcaing for `[]`.
To run the script with a single worker thread, use `--master local`.


## Query the output
The output of the pipeline is two datasets: statistics and derived.

To query the output, first start a pyspark REPL in the container (or use the Jupyter notebook)
```
docker exec -it suspicious_etl /usr/local/spark/bin/pyspark
```

The REPL looks like
```
Python 3.11.6 | packaged by conda-forge | (main, Oct  3 2023, 10:40:35) [GCC 12.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/01/05 20:08:18 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.0
      /_/

Using Python version 3.11.6 (main, Oct  3 2023 10:40:35)
Spark context Web UI available at http://57c22ec20f5d:4040
Spark context available as 'sc' (master = local[*], app id = local-1704485299176).
SparkSession available as 'spark'.
```

Then one can run sql queries using the `datasink` module
```
>>> from sensorsetl import datasink
>>> statsSink = datasink.StatisticsSink(spark)
>>> statsSink.query('SELECT * FROM statistics').show()
```

```
>>> derivedSink = datasink.DerivedSink(spark)
>>> derivedSink.query('SELECT * FROM dervied WHERE run_uuid = 6176976534744076288 AND robot_id = 1 LIMIT 3').show()
```

## Run tests
Start bash shell inside the container, then
```
export PYTHONPATH=/usr/local/spark/python:/usr/local/spark-3.5.0-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip
python -m unittest tests
```
