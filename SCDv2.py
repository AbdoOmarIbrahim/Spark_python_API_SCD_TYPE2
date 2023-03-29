from pyspark.sql import SparkSession,functions as F
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import current_timestamp, to_date, lit, when, coalesce, greatest, least,udf, lit, when, date_sub
import json
from pyspark.sql import Row
from pyspark.sql import Window
from datetime import datetime
from pyspark.sql import SQLContext
from os.path import abspath
import sys
reload(sys)

sys.setdefaultencoding('utf-8')

# create a SparkSession
spark = SparkSession \
        .builder \
        .appName('SCD Type 2') \
        .master('local[*]') \
        .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java.jar") \
        .config("spark.jars", "/usr/share/java/mysql-connector-java.jar") \
        .config("spark.sql.hive.convertMetastoreParquet","false") \
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
        .enableHiveSupport() \
        .getOrCreate()
        
spark.sql("SHOW tables").show()




df_src = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/SCD").option("driver","com.mysql.jdbc.Driver").option("dbtable","customer_src").option("user","spark").option("password",'spark').load()

df_src.show()

#sqlContext.read.parquet("people.parquet")


df_trgt =spark.sql("select * from customer_trgt")

df_trgt=df_trgt.filter(df_trgt.end_date == '9999-12-31')


#spark.sql("select * from temp_customer_trgt")




#



#spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/SCD").option("driver","com.mysql.jdbc.Driver").option("dbtable","customer_trgt").option("user","spark").option("password",'spark').load()





#


#
#df_src.select("name").show()









joined_data=df_src.join(df_trgt,df_src.src_id==df_trgt.id,"outer")
joined_data.show()


change_type= when(joined_data["key"].isNull(),"I").when(joined_data["address"] != joined_data["prev_address"],"IU").otherwise("no_change")


joined_data = joined_data.withColumn("change_type",change_type)


end_date = datetime.strptime('9999-12-31', '%Y-%m-%d').date()
start_date=current_timestamp()

df_insert = joined_data.filter(joined_data.change_type =="I").select(joined_data.src_id.alias("id"),joined_data.src_name.alias("name"),joined_data.address.alias("prev_address")).withColumn("start_date",lit(start_date)).withColumn("end_date",lit(end_date))




#df_insert.show()

df_upsert = joined_data.filter(joined_data.change_type =="IU").select(joined_data.src_id.alias("id"),joined_data.src_name.alias("name"),joined_data.address.alias("prev_address"),joined_data.start_date,joined_data.end_date)

df_upsert=df_upsert.withColumn("start_date",lit(start_date)).withColumn("end_date",lit(end_date))

#df_upsert.show()

df_update = joined_data.filter(joined_data.change_type =="IU").select(joined_data.id.alias("id"),joined_data.name.alias("name"),joined_data.prev_address,joined_data.start_date,joined_data.start_date.alias("end_date"))

#df_update.show()

df_no_change=joined_data.filter(joined_data.change_type =="no_change").select(joined_data.id,joined_data.name,joined_data.prev_address,joined_data.start_date,joined_data.end_date)

#df_no_change.show()

df_final_result=df_insert.unionAll(df_upsert).unionAll(df_update).unionAll(df_no_change)
df_final_result.show()




window = Window.orderBy(F.col('id'))

df_final_result = df_final_result.withColumn('key', F.row_number().over(window))
df_final_result.show()





df_final_result.write.mode("overwrite").option("path", "/spark_data/temp/").saveAsTable("temp_customer_trgt")

df_final_store =spark.sql("select * from temp_customer_trgt")

df_final_store.write.mode('overwrite').option("path", "/spark_data/data/").saveAsTable("customer_trgt") 


spark.stop()


#
     
#

#end_date = when(joined_data["change_type"] == "update",  ) \
  #.otherwise(joined_data["effective_end_date"])
  






# stop the SparkSession


