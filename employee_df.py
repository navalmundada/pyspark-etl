from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("createSchemaRuntime") \
        .getOrCreate()

    sc= spark.sparkContext

    emp_df= spark.read.csv(path='data/emp.csv', inferSchema=True, header=True, sep=',')

    emp_df.show()

    emp_df.createOrReplaceTempView("emp")

    dept_df= spark.read.csv(path='data/dept.csv', inferSchema=True, header=True, sep=',')

    dept_df.createOrReplaceTempView("dept")

    dept_df.show()

    #show emp data whose salary > their managers
    emp_data=spark.sql("select e.* from emp e, emp m where e.manager_id=m.employee_id and e.salary>m.salary")

    emp_data.show()


    #get max salary per dept
    max_sal_per_dept="select e.DEPARTMENT_ID, d.DEPARTMENT_NAME, max(e.salary) from emp e, dept d " \
                     "where e.DEPARTMENT_ID=d.DEPARTMENT_ID " \
                     "group by e.DEPARTMENT_ID, d.DEPARTMENT_NAME"
    max_sal_dept= spark.sql(max_sal_per_dept)

    max_sal_dept.show()