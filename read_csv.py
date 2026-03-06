from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("delimiter", ";").csv('csvs/teste_csv_normal.csv', header=True)
df = df.withColumn('data', F.to_timestamp(F.col('data'), 'yyyy-MM-dd-HH.mm.ss.SSSSSS'))
df.show(truncate = False)
df.printSchema()

last_execution = datetime(2024,4,1)
df = df.filter(F.col('data') > F.lit(last_execution))
df.select('data').show()

### Ler csv com bug
df = spark.read.text('csvs/teste_csv_bug.csv')
df.show(truncate = False)

primeira = df.head(1)[0].value
ultima = df.tail(1)[0].value

df = df.filter(
    (F.col('value') != F.lit(primeira)) &
    (F.col('value') != F.lit(ultima))
)

df = df.withColumn("split_col", F.split(F.col("value"), ";"))
df = df.withColumn("data", F.col("split_col").getItem(0)) \
       .withColumn("valor", F.col("split_col").getItem(1)) \
       .drop("split_col", "value")

df.show(truncate = False)
df.printSchema()
