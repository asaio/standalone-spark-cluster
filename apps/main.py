from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format
import pandas as pd

def init_spark():
  sql = SparkSession.builder\
    .appName("test-app")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql,sc

def main():
  #url = "jdbc:postgresql://demo-database:5432/mta_data"
  #properties = {
  #  "user": "postgres",
  #  "password": "casa1234",
  #  "driver": "org.postgresql.Driver"
  #}
  #file = "/opt/spark-data/MTA_2014_08_01.csv"
  sql,sc = init_spark()

  df_tod = sql.read.json('/opt/spark-data/tod_test_json.json')
  df_yt = sql.read.json('/opt/spark-data/yt_test_json.json')
  df_tod_pd = df_tod.toPandas()
  df_tod_columns = df_tod_pd.columns.tolist()
  df_yt_pd = df_yt.toPandas()
  df_yt_columns = df_yt_pd.columns.tolist()
  df_diff_new_pd = pd.json_normalize(df_yt_pd['item']).join(df_yt_pd[['type']]).sort_index(axis=1).merge(pd.json_normalize(df_tod_pd['item']).join(df_tod_pd[['type']]).sort_index(axis=1), how='outer', indicator=True)
  df_diff_new_pd = df_diff_new_pd[df_diff_new_pd['_merge'] == 'right_only']
  del df_tod
  del df_yt
  del df
  df_diff_new = sql.CreateDataFrame(df_diff_new_pd)
  df_diff_new.show(truncate=False)
  df_diff_new.write.mode('Overwrite').json('/opt/spark-data/delta.json')

  #df = sql.read.load(file,format = "csv", inferSchema="true", sep="\t", header="true"
  #    ) \
  #    .withColumn("report_hour",date_format(col("time_received"),"yyyy-MM-dd HH:00:00")) \
  #    .withColumn("report_date",date_format(col("time_received"),"yyyy-MM-dd"))
  #
  ## Filter invalid coordinates
  #df.where("latitude <= 90 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180") \
  #  .where("latitude != 0.000000 OR longitude !=  0.000000 ") \
  #  .write \
  #  .jdbc(url=url, table="mta_reports", mode='append', properties=properties) \
  #  .save()
  
if __name__ == '__main__':
  main()