from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from process_data.config.ConfigStore import *
from process_data.functions import *

def join_by_product_id(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.product_id") == col("in1.product_id")), "right_outer")\
        .select(col("in0.customer_id").alias("customer_id"), col("in1.product_id").alias("product_id"), col("in1.price_usd_cents").alias("price_usd_cents"), col("in1.sub_category").alias("sub_category"), col("in1.name").alias("name"), col("in1.main_category").alias("main_category"), col("in1.date_introduced").alias("date_introduced"), col("in0.sale_date").alias("sale_date"))
