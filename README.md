# sfCrimeAnalysis
San francisco crime report Analysis using spark streaming and kafka

1) How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

By checking processedRowsPerSecond in the spark steaming output which indicates the throughput. Changning the number maxRatePerPartition,executor memory,number of partitions will change the throughput 

2)What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

maxRatePerPartition
spark.executor.memory

if changing these values makes it low latency and high throughput we can say its optimal. If the the data volume is high we can also increase the number of kafka partitions to increase the parallelism.
