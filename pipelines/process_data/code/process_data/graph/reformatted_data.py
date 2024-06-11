from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from process_data.config.ConfigStore import *
from process_data.functions import *

def reformatted_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("product_id"), 
        col("customer_id"), 
        col("date_introduced"), 
        col("name"), 
        col("sale_date"), 
        col("price_usd_cents"), 
        col("sub_category"), 
        col("main_category"), 
        concat(
            lit("Q"), 
            quarter(col("sale_date")).cast(StringType()), 
            lit(" "), 
            year(to_date(col("sale_date"))).cast(StringType())
          )\
          .alias("sale_qtr")
    )
