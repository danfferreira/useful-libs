from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType

def flatten_df(df):
    """
    Recursively flattens a DataFrame:
      - Expands any column of StructType into individual columns.
      - Explodes any column that is an ArrayType of StructType.
    Returns a new DataFrame that is fully flattened.
    """
    complex_fields = [field.name 
                      for field in df.schema.fields 
                      if isinstance(field.dataType, StructType) or 
                         (isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType))]
    
    while complex_fields:
        col_name = complex_fields[0]
        field_type = df.schema[col_name].dataType

        if isinstance(field_type, StructType):
            expanded = [col(f"{col_name}.{subfield.name}").alias(f"{col_name}_{subfield.name}") 
                        for subfield in field_type.fields]
            df = df.select("*", *expanded).drop(col_name)

        elif isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType):
            df = df.withColumn(col_name, explode(col(col_name)))
        
        complex_fields = [field.name 
                          for field in df.schema.fields 
                          if isinstance(field.dataType, StructType) or 
                             (isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType))]
    
    return df
