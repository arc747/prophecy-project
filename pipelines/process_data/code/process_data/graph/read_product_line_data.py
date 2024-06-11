from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from process_data.config.ConfigStore import *
from process_data.functions import *

def read_product_line_data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("json")\
        .schema(
          StructType([
            StructField("_corrupt_record", StringType(), True), StructField("date_introduced", StringType(), True), StructField("extra_field", StringType(), True), StructField("main_category", StringType(), True), StructField("name", StringType(), True), StructField("price_usd_cents", StringType(), True), StructField("product_id", LongType(), True), StructField("sub_category", StringType(), True)
        ])
        )\
        .load("dbfs:/course_lab_originals/rainforest_biz_sources/product_line_b/")
