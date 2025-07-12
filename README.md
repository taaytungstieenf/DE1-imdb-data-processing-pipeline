# IMDB DATA PROCESSING PIPELINE

### A. Project Introduction

##### 1. What is this project?

This is a data processing system project that applies a **data warehouse architecture**.  
The main objectives are to:

- **Download** raw IMDb data
- **Process** and clean the data
- **Store** it in a partitioned format
- **Schedule** automated data loading into a data warehouse
- Enable **data visualization** for business analysis

##### 2. Data for This Project

According to the data provider:

> Subsets of IMDb data are available for access to customers for personal and non-commercial use.  
> You may maintain local copies of this data, subject to our terms and conditions.  
> Please refer to the Non-Commercial Licensing section and the copyright/license  
> to ensure compliance.

Source: https://developer.imdb.com/non-commercial-datasets/

##### 3. Project Technologies Used
+ Big data storage tool as data lake    : Hadoop HDFS
+ Big data processing tool              : Spark
+ Relational database as data warehouse : MySQL
+ Data visualizaton                     : Streamlit

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
|______ ./data_pipeline
|       |______ ./pipeline_automation
|       |       |______ process1-from_hdfs_to_local.sh
|       |       |______ process2-from_local_to_mysql.sh
|       |______ ./pipeline_validation
|       |       |______ process1-validate_lastest_partitioned_dataset.py        
|       |       |______ process2-validate_mysql_table_schema.py
|       |______ process1-from_hdfs_to_local.py
|       |______ process2-from_local_to_mysql.py
|
|______ ./data_preparation
|       |
|       |______ step1-download_and_extract_data.py
|       |______ step2-export_data_into_parquet_format.py
|       |______ step3-validate_newly_exported_data.py
|       |______ step4-create_dataset_for_hdfs_storage.py
|       |______ step5-validate_new_imdb_dataset.py
|       |______ step6-upload_dataset_to_hdfs.py
|       |______ step7-validate_full_dataset_on_hdfs.py
|       |______ step8-partition_dataset_by_year_on_hdfs.py
|       |______ step9-validate_partitioned_datasets_on_hdfs.py
|
|______ ./data_storage
|       |
|       |______ ./hub1-raw_data_for_manipulation
|       |       |______ ./parquet_data
|       |       |       |______ name.basics.parquet
|       |       |       |______ title.akas.parquet
|       |       |       |______ title.basics.parquet
|       |       |       |______ title.crew.parquet
|       |       |       |______ title.episode.parquet
|       |       |       |______ title.principals.parquet
|       |       |       |______ title.ratings.parquet
|       |       |______ ./tsv_data
|       |       |       |______ name.basics.tsv
|       |       |       |______ title.akas.tsv
|       |       |       |______ title.basics.tsv
|       |       |       |______ title.crew.tsv
|       |       |       |______ title.episode.tsv
|       |       |       |______ title.principals.tsv
|       |       |       |______ title.ratings.tsv
|       |       |______ name.basics.tsv.gz
|       |       |______ title.akas.tsv.gz
|       |       |______ title.basics.tsv.gz
|       |       |______ title.crew.tsv.gz
|       |       |______ title.episode.tsv.gz
|       |       |______ title.principals.tsv.gz
|       |       |______ title.ratings.tsv.gz
|       |
|       |______ ./hub2-new_dataset_for_HDFS
|       |       |______ ./fact_movie_full.parquet
|       |               |______ ._SUCCESS.crc
|       |               |______ .part-00000-72b4ac2d-428c-4a71-afad-acd466789850-c000.snappy.parquet.crc
|       |               |______ .part-00001-72b4ac2d-428c-4a71-afad-acd466789850-c000.snappy.parquet.crc
|       |               |______ .part-00002-72b4ac2d-428c-4a71-afad-acd466789850-c000.snappy.parquet.crc
|       |               |______ ...
|       |
|       |______ ./hub3-partitioned_datasets_for_warehouse
|       |       |______ ./startYear=1874
|       |       |       |______ part-00004-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |       |______ ./startYear=1878
|       |       |       |______ part-00003-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |       |       |______ part-00012-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |       |       |______ part-00022-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |       |       |______ part-00026-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |       |       |______ part-00036-fb03c9db-ae4e-4b08-a154-151f508fa381.c000.snappy.parquet
|       |       |______ ./startYear=YYY
|       |______ downloaded_partitions.json
|
|______ ./data_validation
|       |______ validation_process1.log
|       |______ validation_process2.log
|       |______ validation_step3.log
|       |______ validation_step5.log
|       |______ validation_step7.log
|       |______ validation_step9.log
|
|______ ./data_visualization
|       |______ ./pages
|       |       |______ Chart A - Histogram.py
|       |       |______ Chart B - Bar Chart.py
|       |       |______ Chart C - Box Plot.py
|       |       |______ Chart D - Pie Chart.py
|       |       |______ Chart E - Line Chart.py
|       |       |______ Chart F - Heatmap.py
|       |       |______ Chart G - Scatter Plot.py
|       |       |______ Chart H - Word Cloud.py
|       |______ HOME.py
|
|______ .gitignore
|______ README.md
|______ requirements.txt
```

---

### C. Data Modeling

```

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

### D. Project Workflow

```
         +-------------------------------------------+
         |  IMDB Data online                         |   ----->  IMDB data online
         |-------------------------------------------+
         |
        [1] Download, extract data in local
         |
         V-------------------------------------------+
         |  IMDB data in form of TSV                 |   ----->  IMDB TSV data at local
         |-------------------------------------------+
         |
        [2] Turn TSV data into parquet format in local
         |
         V-------------------------------------------+
         |  IMDB data in form of parquet             |   ----->  IMDB parquet data at local
         |-------------------------------------------+
         |
      VVV[3] Validate newly exported parquet data
         |
         V-------------------------------------------+
         |  IMDB data in form of parquet             |   ----->  IMDB parquet data at local
         |-------------------------------------------+
         |
        [4] Turn a RDBMS into 1 dataset for HDFS storage
         |
         V-------------------------------------------+
         | IMDB dataset                              |   ----->  New IMDB dataset at local
         |-------------------------------------------+
         |
     VVV[5] Validate new IMDB dataset
         |
         V-------------------------------------------+
         | IMDB dataset                              |   ----->  New IMDB dataset at local
         |-------------------------------------------+
         |
        [6] Upload dataset to Hadoop HDFS
         |
         V-------------------------------------------+
         | IMDB dataset                              |   ----->  New IMDB dataset at HDFS
         |-------------------------------------------+
         |
     VVV[7] Validate full dataset on HDFS
         |
         V-------------------------------------------+
         | IMDB dataset                              |   ----->  Full IMDB dataset at HDFS
         |-------------------------------------------+
         |
        [8] Partition full dataset on HDFS to partions for batch processing
         |
         V-------------------------------------------+
         | IMDB dataset in partitions                |   ----->  Partitoned IMDB dataset at HDFS
         |-------------------------------------------+
         |
     VVV[9] Validate partioned datasets on HDFS
         |
         V-------------------------------------------+                                                        
         | IMDB dataset in partitions                |   ----->  Partitoned IMDB dataset at HDFS                        
         |-------------------------------------------+                                                                      
         |                                                                                                              
        [X] Download batch partitioned dataset for each year from HDFS to local                               ___________
         |                                                                                                              |                    
         V-------------------------------------------+                                                                  |   
         | IMDB dataset in patitions by year         |   ----->  Partitoned IMDB dataset at local                       |
         |-------------------------------------------+                                                                  |
         |                                                                                                              |
     VVV[x] Validate partitions dataset at local                                                                        |
         |                                                                                                              |
         V-------------------------------------------+                                                                  |
         | IMDB dataset in patitions by year         |   ----->  Partitoned IMDB dataset at local                       |
         |-------------------------------------------+                                                                  |
         |                                                                                                              |--------=====\\ Data pipeline
        [Y] Push partioned datasets into MySQL as data warehouse                                                        |--------=====// Automation
         |                                                                                                              |
         V-------------------------------------------+                                                                  |
         | IMDB dataset in MySQL                     |   ----->  Data in Data Warehouse                                 |
         |-------------------------------------------+                                                                  |
         |                                                                                                              |
     VVV[X] Validate data in Data Warehouse                                                                             |
         |                                                                                                              |
         V-------------------------------------------+                                                                  |
         | IMDB data in MySQL ready for visualization|   ----->  Data in Data Warehouse                                 |
         +-------------------------------------------+                                                        __________|
```

---

### E. Project Installation

---

### F. Project Commands Cheatsheet

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
|                                                                CRONTAB                                                                |
=========================================================================================================================================
|   CONCEPTS                                |   CODE                                                                                    |
|___________________________________________|___________________________________________________________________________________________|
|   Step 01. Take abs python3 directory     |   $ which python3                                                                         |
|   Step 02. Take abs main.py directory     |   $ which main                                                                            |
|=======================================================================================================================================|
|   Step 03. Bash Scripting installation    |   $ nano run_bash.sh                                                                      |
|---------------------------------------------------------------------------------------------------------------------------------------|
|   #!/bin/bash                                                                                                                         |
|                                                                                                                                       |
|   PROJECT_DIR="/home/tae/Projects/Capstone 1 – DATA ENGINEERING/DE1-imdb-data-processing-pipeline"                                    |
|   PYTHON="$PROJECT_DIR/.venv/bin/python3"                                                                                             |
|   SCRIPT="$PROJECT_DIR/main.py"                                                                                                       |
|   LOGFILE="$PROJECT_DIR/cron.log"                                                                                                     |
|                                                                                                                                       |
|   $PYTHON $SCRIPT >> $LOGFILE 2>&1                                                                                                    |
|                                                                                                                                       |
|---------------------------------------------------------------------------------------------------------------------------------------|
|   Step 04. Give permission to execute     |   $ chmod +x run_main.sh                                                                  |
|=======================================================================================================================================|
|   Step 03. Run run_main.sh automatically  |   $ crontab -e                                                                            |
|---------------------------------------------------------------------------------------------------------------------------------------|
|   ~ 0 */3 * * * /home/tae/Projects/Capstone\ 1\ –\ DATA\ ENGINEERING/DE1-imdb-data-processing-pipeline/run_main.sh                    |
|---------------------------------------------------------------------------------------------------------------------------------------|
|   Step 04. Verify Crontab installation    |   $ crontab -l                                                                            |
|===========================================|===========================================================================================|
```

---

### G. Notes

In case of you guys get confused:
- If you want to use this project, then feel free to use it, no need to contact me.

---

### H. About Author

- Name: Nguyễn Đức Tây
- University / major: HCMC University of Techology and Education / Data Engineering
- Degree: Bachelor of Science in Data Engineering
- Email: nguyenductay121999@gmail.com
- Last modified date: 05/07/2025

<p align="center">
    <img src="https://i.pinimg.com/originals/98/4e/81/984e81934046c3050464525dfcacb6bc.gif" width="800"/>
</p>