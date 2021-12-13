from pyspark.sql import *
from pyspark.sql.functions import regexp_extract, substring_index, col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("xfr_transform") \
        .getOrCreate()

    df= spark.read.csv(path='data/my_file.csv', header=True, sep=',')

    df.show()

    #reading xfr file with spark
    #----------------------------
    #read_xfr= sc.textFile("xfr/xfr.dat")
    #read_xfr_header=read_xfr.first()
    #read_xfr=read_xfr.filter(lambda row:row!=read_xfr_header)
    #xfr_in_list= read_xfr.collect()

    #reading xfr file with python file read - open option
    #----------------------------
    with open("xfr/xfr.dat") as f:
        xfr_in_list = f.read().splitlines()

    print(xfr_in_list)

    xfr_split= [ col_name.split("|") for col_name in xfr_in_list]

    print(f"xx : {xfr_split}")


    #df=df.selectExpr("concat(id,cust_nr)","cust_nr", "substring(bus_tym,1,4)", "bus_tym","bus_desciprion"   )

    xfr = [(x[2] if x[2] else x[0])for x in xfr_split ]
    print(f"xfr: {xfr}")

    df = df.selectExpr(*((x[2] if x[2] else x[0])for x in xfr_split ))

    df.show()

    ## rename multiple columns
    df= df.toDF(*(x[1] for x in xfr_split))

    df.show()