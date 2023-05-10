# Python function to flatten the data dynamically
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import udf, to_json, col, struct, array
from pyspark.sql.types import ArrayType, StructType, MapType, StringType
import json
from datetime import datetime
# Create outer method to return the flattened Data Frame
def flatten_schema(schema, prefix = None, sort = True) -> list:
    # List to hold the dynamically generated column names
    flattened_col_list = []
    for field in schema:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            flattened_col_list += flatten_schema(dtype, prefix = name)
        else:
            flattened_col_list.append(name)
    if(sort == True):
        return sorted(flattened_col_list)
    else:
        return flattened_col_list
def cast_struct_to_map(df: DataFrame, column_to_cast: str):
    def toMap(d):
        if d:
            return(json.loads(d))  
        else:
            return None
        
    # UDF returns a Map of Strings as Key:Value pair
    map_udf=udf(lambda d: toMap(d),MapType(StringType(),StringType()))
    
    df = df.withColumn("structtype_json_col", to_json('structtype_col'))
    df = df.withColumn("maptype_col", map_udf(col('structtype_json_col'))).\
        drop("structtype_json_col")
    return df
for column in flatten_schema(df.schema):
    df.withColumn('item', withField('item.parameters', col('item.parameters')))
def reorder_fields_in_column(df, fields_list):
    Match.__getitem__(g)
This is identical to m.group(g). This allows easier access to an individual group from a match:

>>>
m = re.match(r"(\w+) (\w+)", "Isaac Newton, physicist")
m[0]       # The entire match
'Isaac Newton'
m[1]       # The first parenthesized subgroup.
'Isaac'
m[2]       # The second parenthesized subgroup.
'Newton'
Named groups are supported as well:

>>>
m = re.match(r"(?P<first_name>\w+) (?P<last_name>\w+)", "Isaac Newton")
m['first_name']
'Isaac'
m['last_name']
'Newton'