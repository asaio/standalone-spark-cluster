import json
from operator import itemgetter
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col,date_format,lit
from pyspark.sql.types import StructType,ArrayType,StructField,StringType
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

  #df_tod = sql.read.json('/opt/spark-data/tod_test_json.json')
  #df_tod.select("item")
  df = sql.read.json('/opt/spark-data/table_record.json', multiLine=True, encoding = "UTF-8")
  schema = df.schema
  
  def flatten_schema(schema, prefix = None, sort = True) -> list:
    # List to hold the dynamically generated column names
    flattened_col_list = []
    for field in schema:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            flattened_col_list.append((name,dtype))
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            flattened_col_list.append((name,dtype))
            flattened_col_list += flatten_schema(dtype, prefix = name)
        else:
            flattened_col_list.append((name,dtype))
    if(sort == True):
        return sorted(flattened_col_list, key=lambda given_tuple: given_tuple[0])
    else:
        return flattened_col_list

  def schema_to_select_expr(schema, prefix=''):
    select_cols = []
    array_column = ''
    for column in sorted(schema, key=lambda x: x.name):
      if isinstance(column.dataType, ArrayType):
        array_column = column.dataType
        column.dataType = column.dataType.elementType
      if isinstance(column.dataType, StructType):
        fields_in_col = []
        for field in sorted(column.jsonValue()['type']['fields'],
        key=lambda x: x['name']):
          #print(field, type(field))
          if ((field['type'] == 'string') & (('.' in field['name']) & (r'`' not in field['name']))):
            print(f"THIS FIELD HAS '.' IN IT, ADJUSTING: FROM {field['name']}")
            field['name'] = r'`'+field['name']+r'`'
            print(f"TO: {field['name']}")
          #print(field, type(field))
          #if array_column:
          #  new_struct = StructType([StructField.fromJson(field)])
          #else:
          #  new_struct = StructType([StructField.fromJson(field)])
          if ((array_column!=None) & (field['type'] not in ['struct', 'array'])):
            #print(column.name, field, field.items())
            new_struct = StructType([StructField(name=field['name'], dataType=StringType())])
          else:
            new_struct = StructType([StructField.fromJson(field)])
          new_prefix = prefix+'.'+column.name if prefix else column.name
          fields_in_col.extend(schema_to_select_expr(new_struct, prefix=new_prefix))
        if array_column:
          #print(new_struct)
          #print(fields_in_col)
          fields_in_array_col = []
          #for f in fields_in_col:
          #  unnested_f = f.split('.')[-1]
          #  fields_in_array_col.append(f'cast({f} as string) {unnested_f}')
          select_cols.append('array(struct('+','.join(fields_in_col)+f')) AS {column.name}')
          #print(select_cols)
        else:
          select_cols.append('struct('+','.join(fields_in_col)+f') AS {column.name}')
      else:
        if prefix:
          select_cols.append(prefix+'.'+column.name)
        else:
          select_cols.append(column.name)
    return select_cols

  #print(flatten_schema(df.schema))
  #print(schema_to_select_expr(df.schema))
  df_1 = df.union(df)
  df_2 = df.union(df).selectExpr(schema_to_select_expr(df.schema))
  #print(df_2.schema)
  df_1.printSchema()
  df_2.printSchema()
  df_1.show(truncate=False)
  df_2.show(truncate=False)
  #df_tod.show(truncate=False)
  #df_tod.printSchema()
  #df_yt = sql.read.json('/opt/spark-data/yt_test_json.json').drop('type')
  #df_yt.show(truncate=False)
  #df_yt.printSchema()
  #df_diff = df_tod.join(df_yt, on=["item.name"], how='leftanti')
  #df_diff.printSchema()
  #df_diff.show()
  #df_diff.write.mode('Overwrite').json('/opt/spark-data/delta.json')
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
