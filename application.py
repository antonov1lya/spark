import time
import psutil
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_timestamp, floor, hour
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

OPTIMIZED=True

start_time = time.time()

spark = SparkSession.builder.appName("application").master("spark://spark-master:7077").getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/data/nyc_yellow_taxi.csv", header=True, inferSchema=True)
df = df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("duration", floor((unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60).cast("integer"))
df = df.withColumn("hour", hour(col("tpep_pickup_datetime")))
df = df.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")


train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
if OPTIMIZED:
    train_df.cache()

target_col = "total_amount"
numeric_cols = [col for col in df.columns if df.schema[col].dataType.typeName() in ["integer", "double"] and col != target_col]
assembler = VectorAssembler(
    inputCols=numeric_cols,
    outputCol="features_unscaled"
)
scaler = StandardScaler(
    inputCol="features_unscaled",
    outputCol="features",
    withStd=True,
    withMean=True
)
gbt = GBTRegressor(
    featuresCol="features",
    labelCol=target_col,
    predictionCol="prediction",
    maxIter=100,
    seed=42
)
pipeline = Pipeline(stages=[assembler, scaler, gbt])
model = pipeline.fit(train_df)

predictions = model.transform(test_df)
evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")
r2_score = evaluator.evaluate(predictions)
print("R^2: %.2f" % (r2_score))
end_time = time.time()
print("Elapsed time %.2f sec" % (end_time - start_time))
memory_usage = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
print("Memory usage %.2f MB" % (memory_usage))

spark.stop()
