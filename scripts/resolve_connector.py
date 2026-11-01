from pathlib import Path
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[1]").appName("kinda-build")
         .config("spark.jars.ivy", "/opt/kinda/ivy")
         .config("spark.jars.ivySettings", "/opt/kinda/ivysettings.xml")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1")
         .getOrCreate())
assert spark.range(1).count() == 1
spark.stop()
assert list(Path("/opt/kinda/ivy/jars").glob("*spark-sql-kafka*"))
