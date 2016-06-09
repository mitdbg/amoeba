# Amoeba

Amoeba is an adaptive multi-dimensional index built on top of tables in distributed storage system.
It consists of two main components:
- Upfront Partitioner: Runs while uploading the dataset into HDFS. Ensures that we partition on 'as many' attributes as possible.
- Adaptive Repartitioner: Maintains a history of queries seen. Decided before executing a query if we can re-partition a part of the table to improve a global cost.

See the wiki for installation details and how to run the benchmark.
