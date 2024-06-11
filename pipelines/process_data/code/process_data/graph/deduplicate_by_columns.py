from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from process_data.config.ConfigStore import *
from process_data.functions import *

def deduplicate_by_columns(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(
        ["customer_id",  "date_introduced",  "name",  "sale_date",  "name",  "sub_category",  "main_category",  "product_id",          "product_id",  "price_usd_cents"]
    )
