# IMDB DATA PROCESSING PIPELINE

### A. Project Introduction

##### 1. 

##### 2.

##### 3.

---

### B. Project Structure

```
./Hadoop HDFS Storage Structure
|
|______ ./user
        |______ ./hadoop
                |______ ./imdb_storage
                |       |______ fact_full_movie.parquet
                |______ ./partitioned_by_year        
                        |______ _SUCCESS
                        |______ ./startYear=1874
                        |______ ./startYear=1878
                        |______ ./startYear=1881
                        |______ ...

./DE1-imdb-data-processing-pipeline
|
|______ ./venv
|
|______ ./data_preparation
|       |______ step1-download_and_extract_data.py
|       |______ step2-normalize_and_validate_parquet_data.py
|       |______ step3-validate_parquet_data.py
|       |______ step4-create_new_data_for_batch_processing.py
|       |______ step5-validate_data_for_batch_processing.py
|       |______ step6-apply_sparkSQL_on_dataset.ipynb
|       |______ step7-exploratory_data_analysis.ipynb
|       |______ step8-partition_data_for_batch_processing_on_hdfs
|
|______ ./data_storage
|       |
|       |______ ./data_for_warehouse
|       |       |______ ./startYear=1874
|       |       |       |______ ./startYear=1874
|       |       |               |______ part-00004-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |       |______ ./startYear=1878
|       |               |______ ./startYear=1878
|       |                       |______ part-00003-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |                       |______ part-00012-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |                       |______ part-00022-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |                       |______ part-00026-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |                       |______ part-00036-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |
|       |______ ./data_for_batch
|       |       |______ fact_movie_full.parquet
|       |
|       |______ ./data_parquet
|       |       |______ name.basics.parquet
|       |       |______ title.akas.parquet
|       |       |______ title.basics.parquet
|       |       |______ title.crew.parquet
|       |       |______ title.episode.parquet
|       |       |______ title.principals.parquet
|       |       |______ title.ratings.parquet
|       |
|       |______ ./data_raw
|               |______ ./data_tsv
|               |       |______ name.basics.tsv
|               |       |______ title.akas.tsv
|               |       |______ title.basics.tsv
|               |       |______ title.crew.tsv
|               |       |______ title.episode.tsv
|               |       |______ title.principals.tsv
|               |       |______ title.ratings.tsv
|               |______ name.basics.tsv.gz
|               |______ title.akas.tsv.gz
|               |______ title.basics.tsv.gz
|               |______ title.crew.tsv.gz
|               |______ title.episode.tsv.gz
|               |______ title.principals.tsv.gz
|               |______ title.ratings.tsv.gz
|
|______ ./data_warehouse
|       |______
|
|______ .gitignore
|______ crontab_information.txt
|______ main.py
|______ README.md
|______ requirements.txt
```

---

### C. Project Workflow & Technologies

```
         +--------------------------+
         |  IMDB TSV Data           |   ----->  TSV data at local
         |--------------------------+
         |
        [1] Normalize data by Spark
         |
         V--------------------------+
         |  IMDB parquet data       |   ----->  Parquet data at local
         |--------------------------+
         |
        [2] Push .parquet data from local to HDFS
         |
         V--------------------------+
         |  IMDB parquet data       |   ----->  Parquet data at HDFS
         |--------------------------+
         |
        [3] Transform data with feature engineering concept by Spark
         |
         V--------------------------+
         |  Clean parquet data      |   ----->  Parquet data at HDFS
         |--------------------------+
         |
        [4] Do a lots of queries to understand all the tables data
         |
         V--------------------------+
         | Partitioned parquet Data |   ----->  Parquet data at HDFS
         |--------------------------+
         |
        [5] Partition data on HDFS for the best practice of querying data by HIVE
         |
         V-------------------------------------+
         | Partitioned parquet Data in partions|   ----->  Parquet data at HDFS
         |-------------------------------------+
         |
        [6] Design MySQL database
         |
         V-------------------------------------+
         | Partitioned parquet Data in partions|   ----->  Parquet data at HDFS
         |-------------------------------------+
         |
        [7] Push data into MySQL tables for relational database to query as data warehouse
         |
         V-------------------------------------------+
         | Structured database in MySQL              |
         +-------------------------------------------+
```

---

### D. Project Installation

---

### E. Project Commands Cheatsheet

```
=========================================================================================================================================
|   HADOOP                                  |   CODE                                                                                    |
|___________________________________________|___________________________________________________________________________________________|
|   1. Start Hadoop                         |   $ start-dfs.sh && start-yarn.sh                                                         |
|   2. Check Hadoop                         |   $ jps                                                                                   |
|-------------------------------------------|-------------------------------------------------------------------------------------------|
|   1. Hadoop Namenode UI                   |   http://localhost:9870                                                                   |
|   2. Hadoop Datanode UI                   |   http://localhost:9864                                                                   |
|   3. Hadoop YARN Resource Manager UI      |   http://localhost:8088                                                                   |
|===========================================|===========================================================================================|

=========================================================================================================================================
|   SPARK                                   |   CODE                                                                                    |
|___________________________________________|___________________________________________________________________________________________|
|   1. Check Spark version                  |   $ spark-shell --version                                                                 |
|   2. Start Scala shell                    |   $ spark-shell                                                                           |
|   3. Start PySpark shell                  |   $ pyspark                                                                               |
|   4. Start Spark standalone master server |   $ start-master.sh                                                                       |
|   5. Stop Spark standalone master server  |   $ stop-master.sh                                                                        |
|   6. Start Spark worker server            |   $ start-worker.sh spark://Swift-SF314-54:7077                                           |
|   7. Stop Spark worker server             |   $ stop-worker.sh spark://Swift-SF314-54:7077                                            |
|   8. Specify resource allocation for wrs  |   $ start-worker.sh -c 4 -m 2g spark://Swift-SF314-54:7077                                |
|   9. Stop Spark worker server             |   $ stop-worker.sh -c 4 -m 2g spark://Swift-SF314-54:7077                                 |
|-------------------------------------------|-------------------------------------------------------------------------------------------|
|   1. Hadoop Namenode UI                   |   http://localhost:9870                                                                   |
|   2. Hadoop Datanode UI                   |   http://localhost:9864                                                                   |
|   3. Hadoop YARN Resource Manager UI      |   http://localhost:8088                                                                   |
|===========================================|===========================================================================================|

=========================================================================================================================================
|   PIG                                     |   CODE                                                                                    |
|___________________________________________|___________________________________________________________________________________________|
|   1. Start Hadoop                         |   $ start-dfs.sh && start-yarn.sh                                                         |
|   2. Check Hadoop                         |   $ jps                                                                                   |
|-------------------------------------------|-------------------------------------------------------------------------------------------|
|   1. Hadoop Namenode UI                   |   http://localhost:9870                                                                   |
|   2. Hadoop Datanode UI                   |   http://localhost:9864                                                                   |
|   3. Hadoop YARN Resource Manager UI      |   http://localhost:8088                                                                   |
|===========================================|===========================================================================================|

=========================================================================================================================================
|   HIVE                                    |   CODE                                                                                    |
|___________________________________________|___________________________________________________________________________________________|
|   1. Start Hadoop                         |   $ start-dfs.sh && start-yarn.sh                                                         |
|   2. Check Hadoop                         |   $ jps                                                                                   |
|-------------------------------------------|-------------------------------------------------------------------------------------------|
|   1. Hadoop Namenode UI                   |   http://localhost:9870                                                                   |
|   2. Hadoop Datanode UI                   |   http://localhost:9864                                                                   |
|   3. Hadoop YARN Resource Manager UI      |   http://localhost:8088                                                                   |
|===========================================|===========================================================================================|
```

```aiignore

[title.basics]                         [title.ratings]
+-------------+                       +-------------------+
| tconst (PK) |◄─────────────────────┤ tconst (FK)        |
| titleType   |                       | averageRating     |
| primaryTitle|                       | numVotes          |
| ...         |                       +-------------------+
+-------------+

           ▲
           │
           │
           │           [title.crew]
           │         +---------------------+
           └─────────┤ tconst (FK)         |
                     | directors (nconst*) |
                     | writers (nconst*)   |
                     +---------------------+

           ▲
           │
           │           [title.episode]
           │         +----------------------+
           └─────────┤ tconst (FK)          |
                     | parentTconst (FK)    |
                     | seasonNumber         |
                     | episodeNumber        |
                     +----------------------+

           ▲
           │
           │           [title.principals]
           │         +----------------------------+
           ├────────► tconst (FK)                 |
           │         | ordering                   |
           │         | nconst (FK)                |
           │         | category, job, characters  |
           │         +----------------------------+
           │
           │
[title.akas]         [name.basics]
+-----------+        +-----------------------------+
| titleId   |───────► nconst (PK)                  |
| ordering  |        | primaryName, birthYear, ... |
| title     |        | knownForTitles (tconst*)    |
| region    |        +-----------------------------+
| language  |
+-----------+

```

---

### F. Notes

In case of you guys get confused:
- If you want to use this project, then feel free to use it, no need to contact me.

---

### G. About Author

- Name: Nguyễn Đức Tây
- University / major: HCMC University of Techology and Education / Data Engineering
- Degree: Bachelor of Science in Data Engineering
- Email: nguyenductay121999@gmail.com
- Last modified date: 05/07/2025

<p align="center">
    <img src="https://i.pinimg.com/originals/98/4e/81/984e81934046c3050464525dfcacb6bc.gif" width="800"/>
</p>