# for spark jobs to listen to kafka events

from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

def main():
    # Sets up the Spark session and configures it to use the necessary Kafka and AWS packages and sets the AWS credentials for S3 access.
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages", # automatically download the package from the Maven repository when the Spark session starts
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1," 
            "com.amazonaws:aws-java-sdk:1.11.469"   
        )\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()

    # Adjust the log level to mnimize the console output on the executors
    spark.sparkContext.setLogLevel("WARN")

    # Vehicle Schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])

    # GPS Schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True),
    ])

    # Traffic Schema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])

    # Weather Schema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    # Emergency Schema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    # reads data from a specified Kafka topic, deserializes the JSON data according to the provided schema, and sets a watermark for handling late data.
    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value as STRING)') # casts the value field of the Kafka message from its default binary format to a string format.
                .select(from_json(col('value'), schema).alias('data')) # parses the JSON string into a Spark DataFrame with a specified schema.
                .select('data.*') # selects all the fields from the parsed JSON struct.
                .withWatermark('timestamp', '2 minutes') # helps manage late data, tells Spark to wait for up to 2 minutes for late data to arrive, which is useful for time-based aggregations.
        )
    
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return(input.writeStream
               .format('parquet') # output format, a columnar storage file format that's efficient for large-scale data
               .option('checkpointLocation', checkpointFolder) # to recover stream's data in case of failures
               .option('path', output) # s3 path where data will be written
               .outputMode('append')
               .start()
            )

    # Dataframes: similar to a table in a relational db
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    # Join all the dfs with id and timestamp
    # Each query writes the streaming data from its respective DataFrame to a specific S3 location with a dedicated checkpoint directory.
    query1 = streamWriter(vehicleDF, 's3a://spark-streaming-data-quang01/checkpoints/vehicle_data', 's3a://spark-streaming-data-quang01/data/vehicle_data')

    query2 = streamWriter(gpsDF, 's3a://spark-streaming-data-quang01/checkpoints/gps_data', 's3a://spark-streaming-data-quang01/data/gps_data')

    query3 = streamWriter(trafficDF, 's3a://spark-streaming-data-quang01/checkpoints/traffic_data', 's3a://spark-streaming-data-quang01/data/traffic_data')

    query4 = streamWriter(weatherDF, 's3a://spark-streaming-data-quang01/checkpoints/weather_data', 's3a://spark-streaming-data-quang01/data/weather_data')

    query5 = streamWriter(emergencyDF, 's3a://spark-streaming-data-quang01/checkpoints/emergency_data', 's3a://spark-streaming-data-quang01/data/emergency_data')

    query5.awaitTermination() # blocks the main thread and waits for the termination of query5.

if __name__ == "__main__":
    main()

# docker exec -it spark-master-1 spark-submit \ --master spark://spark-master:7077 \ --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark-city.py