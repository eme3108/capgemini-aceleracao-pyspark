from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

### Transformações
def population_float(df):
	return df.withColumn('population',
		F.col('population').cast(DoubleType()))

def violentCrimes_float(df):
	return df.withColumn('ViolentCrimesPerPop',
		F.col('ViolentCrimesPerPop').cast(DoubleType()))

def violentCrimes(df):
	return (df.withColumn('violent_crimes',
		F.expr('population * ViolentCrimesPerPop')))

def blackPop(df):
	return (df.withColumn('black_population',
						F.expr('population * racepctblack')))

### Questions
def communityPoliceBudget(df):
	(df.filter((F.col('PolicOperBudg') > 0 )|
				(F.col('PolicOperBudg') != '?'))
		.select(F.max(
			F.struct(F.col('PolicOperBudg'), F.col('communityname'))).alias('highest_budget'))
		.select('highest_budget.communityname', 'highest_budget.PolicOperBudg')
		.show())

def violentCrimesCommunity(df):
	(df.select('communityname', 'violent_crimes')
		.sort('violent_crimes', ascending = False)
		.show(1))

def populationCommunity(df):
	(df.select('communityname', 'population')
		.sort('population', ascending = False)
		.show(1))

def highestBlackPop(df):
	(df.select('communityname', 'black_population')
		.sort('black_population', ascending = False)
		.show(1))

def highestPercentWage(df):
	(df.select('communityname', F.col('pctWWage').alias('salary'))
		.sort('salary', ascending = False)
		.show(1))

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	#print(df.show())

	df = population_float(df)
	df = violentCrimes_float(df)
	df = violentCrimes(df)
	df = blackPop(df)

	communityPoliceBudget(df) #Pergunta 1
	# violentCrimesCommunity(df) #Pergunta 2
	# populationCommunity(df)
	# highestBlackPop(df)
	# highestPercentWage(df)