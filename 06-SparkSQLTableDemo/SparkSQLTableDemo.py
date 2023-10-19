from pyspark.sql import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .config("spark.sql.warehouse.dir", "output") \
        .getOrCreate()

    logger = Log4j(spark)
    hiveWarehouse = spark.sparkContext.getConf().get("spark.sql.warehouse.dir")
    print(f"{hiveWarehouse=}")
    print()
    exit(1)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")

    print(str(spark.catalog.listTables("AIRLINE_DB")))
