from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext,Row

if __name__ == "__main__":
	conf = SparkConf().setAppName("SparkSqlApp")
	## SparkContext is the entry point for a Spark App
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	inpRdd = sc.textFile("file:///home/sumit/employee.txt")
	splitInpRdd = inpRdd.map(lambda part: part.split(","))
	createRow = splitInpRdd.map(lambda x: Row(empid = x[0], name = x[1] , dept = x[2]))
	myDf = sqlContext.createDataFrame(createRow)
	myDf.show()
	## Register data frame as a Spark SQL table
	myDf.registerTempTable("mytable")
	## Find the details of employees who department is Finance
	dept_fin = sqlContext.sql("select * from mytable where dept IN ('Fin') ")
	## Print the dataFrame
	dept_fin.show()
	## Stop the SparkContext
	sc.stop()