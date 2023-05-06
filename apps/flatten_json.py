# Python function to flatten the data dynamically
from pyspark.sql import DataFrame
# Create outer method to return the flattened Data Frame
def flatten_json_df(_df: DataFrame) -> DataFrame:
    # List to hold the dynamically generated column names
    flattened_col_list = []
    
    # Inner method to iterate over Data Frame to generate the column list
    def get_flattened_cols(df: DataFrame, struct_col: str = None) -> None:
        for col in df.columns:
            if df.schema[col].dataType.typeName() != 'struct':
                if struct_col is None:
                    flattened_col_list.append(f"{col} as {col.replace('.','_')}")
                else:
                    struct_col = df.schema[col].name
                    t = struct_col + "." + col
                    flattened_col_list.append(f"{t} as {t.replace('.','_')}")
            else:
                struct_col = df.schema[col].name
                chained_col = struct_col +"."+ col if struct_col is not None else col
                get_flattened_cols(df.select(col+".*"), chained_col)
    
    # Call the inner Method
    get_flattened_cols(_df)
    
    # Return the flattened Data Frame
    return _df.selectExpr(flattened_col_list)