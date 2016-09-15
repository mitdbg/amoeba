![Amoeba](https://github.com/anilshanbhag/amoeba-ui/blob/master/static/img/amoeba.png)

Amoeba is a relational storage manager built on top of HDFS for the Spark/Hadoop stack. 

Amoeba is geared towards accelerating ad-hoc query analysis. A user can start analyzing his dataset immediately on upload and the partitioning layout of the data improves over time based on user queries. It does this by splitting the dataset into block-sized partitions based on a partitioning tree. The partitioning tree is a multi-attribute binary tree index which is created with no workload knowledge and incrementally adapted based on user queries.

The user interacts via two components:
- Upfront Parititoner: Loads the data into Amoeba
- Adaptive Query Executor: Used to run queries against the tables in Amoeba

Linux / Mac OS X
--------------

* Install and run the following dependencies
  * `hadoop-2.6`
  * `spark-1.6.0`
  * `zookeeper-3.4.6`

* Install `gradle` 
  * Mac `brew install gradle`
  * Ubuntu `sudo apt-get install gradle`

* Build Amoeba JAR `cd amoeba; gradle shadowJar`

* Amoeba uses fabric for automation. Install it `sudo pip install fabric`

### Running a simple example

* Generate a simple dataset with 1000 tuples and 2 integer attributes `A` and `B`
  
  `cd data; python gen_simple.py`

* Modify the Amoeba configuration to reflect your current hadoop/spark/zookeeper installation
  
  `cd ../conf; vim main.properties` 

* Upload the dataset into Amoeba
  
  ```
  cd ../scripts
  fab setup:local_simple create_table_info bulk_sample_gen create_robust_tree write_partitions
  ```
  
  `setup:local_simple` picks up the configuration from `fabfile/confs.py` for the simple dataset. You need to add configuration for your dataset into this file.
  
  `create_table_info` creates a table entry for the dataset
  
  `bulk_sample_gen` samples the dataset in parallel 
  
  `create_robust_tree` builds the partitioning tree for the dataset
  
  `write_partitions` write the dataset partitioned into HDFS

* To run a bunch of queries

  ```
  cp ../data/simple_queries.log ~/queries.log
  fab setup:local_simple run_queries_formatted
  ```

The wiki contains additional information about setup and using Amoeba.
