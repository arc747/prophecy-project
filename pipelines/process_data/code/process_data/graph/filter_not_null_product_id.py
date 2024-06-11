from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from process_data.config.ConfigStore import *
from process_data.functions import *

def filter_not_null_product_id(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(col("product_id").isNotNull())
