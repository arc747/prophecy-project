from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from process_data.config.ConfigStore import *
from process_data.functions import *

def reformatted_data_projection(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("product_id"), 
        col("price_usd_cents"), 
        col("sub_category"), 
        col("name"), 
        col("main_category"), 
        col("date_introduced")
    )
