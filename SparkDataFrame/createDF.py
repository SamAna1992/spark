from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row

if __name__ == "__main__":
	conf = SparkConf().setAppName("RddToDataFrame")
	## SparkContext is the entry point for an Spark Application
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	## Read a file from local FS and create RDD
	inpRdd = sc.textFile("file:///home/sumit/employee.txt")
	splitInpRdd = inpRdd.map(lambda part: part.split(","))
	## Convert each element of RDD into a Row 
	createRow = splitInpRdd.map(lambda x: Row(empid = x[0], name = x[1] , dept = x[2]))
	## Create a data frame from exisiting RDD
	myDf = sqlContext.createDataFrame(createRow)
	## prints the RDD
	myDf.show()
	## Stop the SparkContext
	sc.stop()