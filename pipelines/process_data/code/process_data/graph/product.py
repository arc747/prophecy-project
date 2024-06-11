from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from process_data.config.ConfigStore import *
from process_data.functions import *

def product(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load("dbfs:/course_lab_originals/rainforest_biz_sources/sales_parquet/")
