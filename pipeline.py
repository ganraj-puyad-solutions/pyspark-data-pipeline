from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sha2
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark
spark = SparkSession.builder.appName("DataPipelineProject").getOrCreate()

print("🚀 Pipeline started")

# Schema definition
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("purchase_amount", IntegerType(), True)
])

# Load data
df = spark.read.csv("data/raw_data.csv", header=True, schema=schema)
print("✅ Data loaded")

# 1. Cleaning
df_clean = df.filter(col("age").isNotNull())
print("✅ Data cleaned")

# 2. Transformation
df_transformed = df_clean.withColumn(
    "age_group",
    when(col("age") < 30, "Young")
    .when(col("age") < 50, "Adult")
    .otherwise("Senior")
)
print("✅ Transformation done")

# 3. Hashing
df_masked = df_transformed.withColumn(
    "email_hashed",
    sha2(col("email"), 256)
).drop("email")

print("✅ Email hashing applied")

# 4. Validation
df_valid = df_masked.filter(col("purchase_amount") > 0)
print("✅ Validation complete")

# 5. Aggregation
df_agg = df_valid.groupBy("country").avg("purchase_amount")
print("✅ Aggregation complete")

# 6. Save output
df_valid.write.mode("overwrite").csv("data/processed_clean", header=True)
df_agg.write.mode("overwrite").csv("data/processed_agg", header=True)

print("✅ Data saved successfully")

spark.stop()