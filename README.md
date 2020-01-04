Modified Project Files:
-----------------------
* producer_server.py
* kafka_server.py
* data_stream.py
* consumer_server.py

A `/screenshots` folder is also included, as specified in project requirements.

Note that `police-department-calls-for-service.json` is compressed as the file is too large for GitHub.

How To Run:
-----------
In separate terminals:
1. `cd config && /usr/bin/zookeeper-server-start zookeeper.properties`
2. `cd config && /usr/bin/kafka-server-start server.properties`
3. `python kafka_server.py` to start producing data
4. `kafka-console-consumer --bootstrap-server localhost:9092 --topic org.sf.crime.stats --from-beginning` to see data produced from 3)
5. `python consumer_server.py` to see data produced from 3)
6. `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py` to submit a Spark job. Click on 'Preview' to go to Spark UI.

Questions:
----------
1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Modifying "spark.streaming.kafka.maxRatePerPartition" and "spark.streaming.receiver.maxRate" helped increase throughput of data.