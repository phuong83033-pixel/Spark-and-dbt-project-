from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
class transformation:
    
    def dedup(self,df,dedub_cols:List,cdc:str):
         
        df = df.withColumn("dedupKey", concat(*dedup_cols))
        df.withColumn("dedupCounts", row_number().over(Window.partitonBy("dedupKey").orderBy(cdc)))
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop("dedupKey", "dedupCounts")

        return df
