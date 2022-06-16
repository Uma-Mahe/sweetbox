#Load the files to data frame
#"Welcome to My python file"
from pyspark.sql import SparkSession

spark=SparkSession.builder\
        .appName("Load")\
        .getOrCreate()

ordersDF=spark.read.csv("gs://mybucket38/salesorder/orders.csv", inferSchema=True, header=True)
productsDF=spark.read.csv("gs://mybucket38/salesorder/products.txt", inferSchema=True, header=True)
customersDF=spark.read.csv("gs://mybucket38/salesorder/customers.txt", inferSchema=True, header=True)
categoriesDF=spark.read.csv("gs://mybucket38/salesorder/categories.txt", inferSchema=True, header=True)
salesDF=spark.read.csv("gs://mybucket38/salesorder/Sales.csv", inferSchema=True, header=True)


"""ordersDF.show(2)
productsDF.show(2)
customersDF.show(2)
categoriesDF.show(2)
salesDF.show(2)
"""

spark-submit writeinhive.py

