from pyspark.sql import SparkSession

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("atm-visits").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

atmVisitsRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","atm-visits")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
atmVisitsStreamingDF = atmVisitsRawStreamingDF.selectExpr("cast(key as string) transactionId", "cast(value as string) location")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
atmVisitsStreamingDF.createOrReplaceTempView("ATMVisits")

# Using spark.sql we can select any valid select statement from the spark view
atmVisitsSelectStarDF=spark.sql("select * from ATMVisits")

# this takes the stream and "sinks" it to the console as it is updated one message at a time:
# +---------+-----+
# |      key|value|
# +---------+-----+
# |241325569|Syria|
# +---------+-----+

atmVisitsSelectStarDF.selectExpr("cast(transactionId as string) as key", "cast(location as string) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "atm-visit-updates")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()


