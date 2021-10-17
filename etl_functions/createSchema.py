from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType,TimestampType,DoubleType,DecimalType

def create_Schema(arg):
    d_types = {
        "varchar":StringType(),
        "integer":IntegerType(),
        "timestamp":TimestampType(),
        "double":DoubleType(),
        "date":DateType(),
        "decimal":DecimalType()
    }
    split_values= arg.split(",")
    sch= StructType()
    for i in split_values:
        x=i.split("|")
        sch.add(x[0],d_types[x[1]],True)
    return sch
