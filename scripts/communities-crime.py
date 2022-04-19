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

def policeBudget(df):
	return (df.withColumn('PolicOperBudg',
				F.col('PolicOperBudg').cast(DoubleType())))

def whitePolicePct_float(df):
	return df.withColumn('PctPolicWhite',
		F.col('PctPolicWhite').cast(DoubleType()))

def medianFamily_float(df):
	return (df.withColumn('medFamInc',
				F.col('medFamInc').cast(DoubleType())))

def violentCrimes(df):
	return (df.withColumn('violent_crimes',
		F.expr('population * ViolentCrimesPerPop')))

def blackPop(df):
	return (df.withColumn('black_population',
						F.expr('population * racepctblack')))

def youngPct_float(df):
	return (df.withColumn('agePct12t21',
				F.col('agePct12t21').cast(DoubleType())))

def youngPopulation(df):
	return (df.withColumn('young_population',
				F.expr('population * agePct12t21')))

def whitePolice(df):
	return (df.withColumn('white_police',
				F.expr('population * PctPolicWhite')))

def blackPct_float(df):
	return (df.withColumn("racepctblack",
				F.col('racepctblack').cast(DoubleType())))

def whitePct_float(df):
	return ((df.withColumn("racePctWhite",
				F.col('racePctWhite').cast(DoubleType()))))

def asianPct_float(df):
	return (df.withColumn("racePctAsian",
				F.col('racePctAsian').cast(DoubleType())))

def hispPct_float(df):
	return (df.withColumn("racePctHisp",
				F.col('racePctHisp').cast(DoubleType())))


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

def highestYoungPopCommunity(df):
	(df.select('communityname', 'young_population')
		.sort('young_population', ascending = False)
		.show(1))

def correlationPoliceBudgetxViolentCrimes(df):
	corr_polBudg_vioCrim = round(df.corr('PolicOperBudg', 'violent_crimes', 'pearson'), 2)
	print(f"Correlation between Police Budget and Violent Crimes: {corr_polBudg_vioCrim}.")

def correlationWhitePolicexPoliceBudget(df):
	corr_whitePol_polBudg = round(df.corr('white_police', 'PolicOperBudg', 'pearson'), 2)
	print(f"Correlation between white police and police budget: {corr_whitePol_polBudg}.")

def correlationPoliceBudgetPopulation(df):
	corr_polBudg_population = round(df.corr('population', 'PolicOperBudg', 'pearson'), 2)
	print(f"Correlation between police budget and population: {corr_polBudg_population}.")

def correlationPopulationViolentCrimes(df):
	corr_pop_vioCrim = round(df.corr('population', 'violent_crimes', 'pearson'), 2)
	print(f'Correlation between population and violent crimes: {corr_pop_vioCrim}.')

def correlationMedianFamilyIncomeViolentCrimes(df):
	corr_medFamInc_vioCrim = round(df.corr('medFamInc', 'violent_crimes', 'pearson'), 2)
	print(f"Correlation between median family income and violent crimes: {corr_medFamInc_vioCrim}.")

def predominantRaceViolentCrimes(df):
	(df.select('communityname', 
				'racepctblack', 
				'racePctWhite', 
				'racePctAsian', 
				'racePctHisp', 
				F.greatest('racepctblack', 
							'racePctWhite', 
							'racePctAsian', 
							'racePctHisp').alias('predominant_race_perc'))
		.sort('violent_crimes', ascending = False)
		.show(10))

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
		          .load("/home/spark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	# print(df.show())


	df = population_float(df)
	df = violentCrimes_float(df)
	df = policeBudget(df)
	df = youngPct_float(df)
	df = violentCrimes(df)
	df = blackPop(df)
	df = youngPopulation(df)
	df = policeBudget(df)
	df = whitePolicePct_float(df)
	df = whitePolice(df)
	df = medianFamily_float(df)
	df = blackPct_float(df)
	df = whitePct_float(df)
	df = asianPct_float(df)
	df = hispPct_float(df)

	# communityPoliceBudget(df) #Pergunta 1
	# violentCrimesCommunity(df) #Pergunta 2
	# populationCommunity(df) #Pergunta 3
	# highestBlackPop(df) #Pergunta 4
	# highestPercentWage(df) #Pergunta 5
	# highestYoungPopCommunity(df) #Pergunta 6
	# correlationPoliceBudgetxViolentCrimes(df) #Pergunta 7
	# correlationWhitePolicexPoliceBudget(df) #Pergunta 8
	# correlationPoliceBudgetPopulation(df) #Pergunta 9
	# correlationPopulationViolentCrimes(df) #Pergunta 10
	# correlationMedianFamilyIncomeViolentCrimes(df) #Pergunta 11
	predominantRaceViolentCrimes(df)