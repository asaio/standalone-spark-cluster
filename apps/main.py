from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col,date_format,lit
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
  #df_tod.show(truncate=False)
  #df_tod.printSchema()
  df_yt = sql.read.json('/opt/spark-data/yt_test_json.json').withColumn('left', lit('left')).drop('type')
  #df_yt.show(truncate=False)
  #df_yt.printSchema()
  df_outer_spark = df_yt.join(df_tod, on=['item'], how='right_outer')
  df_diff = df_outer_spark.where(col('left').isNull())
  df_outer_spark.show()
  df_outer_spark.printSchema()
  df_diff.show()
  df_diff.write.mode('Overwrite').json('/opt/spark-data/delta.json')
  #df_tod_pd = df_tod.toPandas()
  #df_tod_columns = df_tod_pd.columns.tolist()
  #df_yt_pd = df_yt.toPandas()
  #df_yt_columns = df_yt_pd.columns.tolist()
  #print(df_yt_pd, df_tod_pd)
  #df_yt_pd_normalized = pd.json_normalize(df_yt_pd['item'])
  #df_tod_pd_normalized = pd.json_normalize(df_tod_pd['item'])
  #print(df_yt_pd_normalized, df_tod_pd_normalized)
  #df_diff_new_pd = df_yt_pd_normalized.merge(df_tod_pd_normalized, how='outer')
  #print(df_diff_new_pd)
  #df_diff_new_pd = df_diff_new_pd[df_diff_new_pd['_merge'] == 'right_only']
  #del df_tod
  #del df_yt
  #print(df_diff_new_pd)
  #df_diff_new = sql.CreateDataFrame(df_diff_new_pd)
  #df_diff_new.show(truncate=False)
  #df_diff_new.write.mode('Overwrite').json('/opt/spark-data/delta.json')

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
