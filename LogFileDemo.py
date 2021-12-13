from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring_index, lower, col, upper, expr, concat, lit
import etl_functions.createSchema as etl

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("createSchemaRuntime") \
        .getOrCreate()

    sc= spark.sparkContext
    #read a manually defined schema file in the format column name | datatype

    textRDD1 = sc.textFile("schema/schema_file.txt")
    llist = textRDD1.collect()
    listToStr = ''.join([str(elem) for elem in llist])
    sch = etl.create_Schema(listToStr)
    #show schema StructType(List(StructField(id,IntegerType,true)...)
    print(sch)

    df= spark.read.csv(path='data/my_file.csv', schema=sch, header=False, sep=',')

    df.show()
    df.schema
    df.schema.fields
    df.dtype()
    # Convert all columns datatype to Lower or upper.
    #Approach 1 using for loop.

    df_lower = df

    for col_name in df_lower.columns:
        df_lower = df_lower.withColumn(col_name, lower(col(col_name)))

    df_lower.show()

    # approach 2 : using list comprehension. using upper here.

    df_using_lc = df.select(
        *[upper(col(col_name)).name(col_name) for col_name in df.columns]
    )

    #[Column<'upper(id) AS `id`'>, Column<'upper(cust_nr) AS `cust_nr`'>, Column<'upper(name) AS `name`'>, Column<'upper(lname) AS `lname`'>, Column<'upper(business_desc) AS `business_desc`'>]


    #my_list=[upper(col(col_name)) for col_name in df.columns]
    print("this is my list")
    #print(my_list)
    df_using_lc.show(10)


    # Add extra column as concate of name and lname

    df_with_new_col= df.withColumn("combined_name", concat(col("name"),lit(" "),col("lname")))

    df_with_new_col.show()

    #drop multiple columns

    drop_list=["name", "lname"]

    df_with_new_col.drop(*drop_list).show()

    #select list

    select_list=["id", "combined_name"]

    df_with_new_col.select(*select_list).show()


    """
        ## regular expression with spark
        
        file_df = spark.read.text("data/apache_logs.txt")
        file_df.printSchema()
    
        log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    
        logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                                 regexp_extract('value', log_reg, 4).alias('date'),
                                 regexp_extract('value', log_reg, 6).alias('request'),
                                 regexp_extract('value', log_reg, 10).alias('referrer'))
    
        logs_df.withColumn("date",substring_index("date", ":", 1) ).show()
    
        logs_df \
            .where("trim(referrer) != '-' ") \
            .withColumn("referrer", substring_index("referrer", "/", 3)) \
            .groupBy("referrer") \
            .count() \
            .show(100, truncate=False)
    """