# ETL Pipeline Details

The batched processing pipeline takes a data source with a defined schema (in the case of sample data, parquet), performs the transformation steps, and load the data to two output datasets:

- The wide-format feature data (table name `derived`, see `DerivedSink` in `datasink.py`)

- The Run statistics (table name `statistics`, see `StatisticsSink` in `datasink.py`)

The pipeline can be configured with `pipelneconf.py`.

Downstream users can interact with the output datasets using SQL queries.

### Input
The code reads `sample.parquet`, but it is extensible to any data source that comes with a schema.

I made the following assumptions about the input for pipeline runs:
- The pipeline is at some point run with all the data associated with a `run_uuid`, 
- The user (or future work) will deal with duplicate entries in the output datasets, in case this pipeline is run multiple times on a `run_uuid`

`input_schema` in the config file defines the assumed input schema with non-Nullable fields.
Input rows with `NULL` in non-Nullable fields will be dropped.

### Pipeline
The pipeline steps are as follows:

```
parquetSource -> cleanStringDropNa -> downsampleAndPivot -> addDerivedFeatures -> calcRuntimeStats
                                                                                |
                                                                                |
                                                                                |-> squashRobotId
```

I have not looked at what Spark actually did with the steps. In principle, `cleanStringDropNa` is parallelizable across all rows
and I intend to keep it that way (i.e. no drop duplicates). `downsampleAndPivot` does a groupBy time bin, robot_id, and run_uuid. This
shuffling step is likely the bottle neck because the data come in at very high time resolution. The rest of the pipelines use the same
groupings and are parallelable over all rows. Paritioning the data first by `run_uuid`, `robot_id` and then parallelizing over them
may be a good starting point since they are independent partitions for this pipeline.

`find-downsample-interval.py` was where I tried to figure out a good cadence for downsampling. There are a few (`run_uuid`, `robot_id`)
that do not have all the measurements, so I keep the missing columns as null. The max 90th percentile for cadences for all metrics is
0.012s. I picked 0.01s as the downsampling interval. 

### Output
The pipeline writes to two datasets

- The wide-format data `derived`, which should be indexed by `run_uuid` and provide fast range query on time and good compression. This is
currently done with parquet, partitioned over `run_uuid`. Further bucketing on date can be done.

- The `statistics` table (in its deduplicated version) is a good candidate for SQL database with primary key (`run_uuid`, `robot_id`) and may be joined with other tables with these identifiers. This is currently done with just parquet partitioned by `run_uuid`.

Without knowing the input patterns and usage pattern, So I have prioritized having writes succeed over the results being consistent and
unique. 

### TODOs
- The pipeline has not been packaged for running on Spark cluster mode. Some tweaks to directory struture may be necessary.
- I need to prevent writes to the datasinks by a user.