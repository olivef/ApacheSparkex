  return hash(code.split("\t")[0])

if __name__ == "__main__":
    from pyspark import SparkContext
    sc = SparkContext(appName="FinalProject")
	file= sc.textFile("s3://xxxxx/gdeltfiles/")
	#file = file.repartition(160)
	tuples = file.flatMap(lambda x: x.split('\t{254}')).map(lambda fields: fields.split('\t'))
	#increasing the paralelism with partion by, it will create 300 tasks for the sortByKey and reduceByKey
	ALLpartioned=tuples.map(lambda x: (x[51]+'\t'+x[1],1)).partitionBy(300, hash_country).cache()  
	ALLresult=ALLpartioned.reduceByKey(lambda a,b:a+b).sortByKey()
	for x in ALLresult.collect():
		print x
	#to do: persist to hive table

