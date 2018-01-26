from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row

if __name__ == "__main__":
	conf = SparkConf().setAppName("RddToDataFrame")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	inpRdd = sc.textFile("file:///home/sumit/Desktop/certification/hdp_spark/employee.txt")
	splitInpRdd = inpRdd.map(lambda part: part.split(","))
	createRow = splitInpRdd.map(lambda x: Row(empid = x[0], name = x[1] , dept = x[2]))
	myDf = sqlContext.createDataFrame(createRow)
	myDf.show()
	sc.stop()