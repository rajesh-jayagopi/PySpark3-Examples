import os
import sys
import tempfile

from pyspark.sql import SparkSession

# from 01_HelloSpark.lib.utils import get_spark_app_config
_tmp = __import__("01_HelloSpark.lib.utils", fromlist=["get_spark_app_config"])

if __name__ == "__main__":
    conf = _tmp.get_spark_app_config()  # get_spark_app_config()

    os.environ['PYSPARK_PYTHON'] = sys.executable  # Set Python file path need for tempfile
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession \
        .builder \
        .appName("01_HelloSpark") \
        .master("local[2]") \
        .getOrCreate()

    with tempfile.TemporaryDirectory() as d:
        # Write a DataFrame into a CSV file
        df = spark.createDataFrame([{"age": 40, "name": "Kanan"}, {"age": 39, "name": "Stella"},
                                    {"age": 20, "name": "Null"}])
        df.show()
        df.write.mode("overwrite").format("csv").save(d)

        # Read the CSV file as a DataFrame. Replace "Null" values using option 'nullValue' option to 'null'.
        spark.read.schema(df.schema) \
            .option("nullValue", "Null") \
            .format('csv').load(d).show()

    spark.stop()
