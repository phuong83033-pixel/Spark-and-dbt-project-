from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
class transformation:
    
    def dedup(self,df,dedup_cols:List,cdc:str):
         
        df = df.withColumn("dedupKey", concat(*dedup_cols))
        df = df.withColumn("dedupCounts",row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop("dedupKefile y", "dedupCounts")

        return df

    def process_timestamp(self,df):
        df = df.withColumn("process_timestamp",current_timestamp())
        return df