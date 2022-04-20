from os import truncate
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.window import Window
 
#### Transformation
def removeWhiteSpaces(df):
    for i in df.columns:
        df = df.withColumn(i, F.trim(F.col(i)))

    return df

def hoursPerWeek_float(df):
    return (df.withColumn('hours-per-week', F.col('hours-per-week').cast('float')))

def finalWeight_int(df):
    return (df.withColumn('fnlwgt', F.col('fnlwgt').cast('int')))

def educationNum_int(df):
    return (df.withColumn('education_nbr', F.col('education-num').cast('int')))

def maritalStatus(df):
    return (df.withColumn('marital_status', 
                            F.when(F.col('marital-status').startswith('Married'), 'married')
                            .otherwise('unmarried')))

def whitePeople(df):
    return (df.withColumn('white', 
                                F.when(F.col('race') == 'White', 'yes')
                                .otherwise('no')))

#### Questions
def workerClassMost50K(df):
    print("Question 1")
    (df.filter(F.col('workclass') != '?')
        .groupBy('workclass', 'income')
        .agg(F.sum('fnlwgt').alias('workclass_income'))
        .withColumn("most_common_workclass_income", F.struct("workclass_income", "income"))
        .groupBy('workclass')
        .agg(F.max('most_common_workclass_income').alias("most_common_income"))
        .filter(F.col('most_common_income.income') == ">50K")
        .show())

def raceWorkHoursPerWeek(df):
    print("Question 2")
    df_aux = (df.groupBy(F.col('race').alias('racial'))
                .agg(F.sum('fnlwgt').alias('total_workers_race')))

    df = df.join(df_aux, df.race == df_aux.racial, 'inner').select(df.fnlwgt, df.race, df['hours-per-week'], df_aux.total_workers_race)
    
    (df.withColumn('partial_mean_hour', F.col('fnlwgt') * F.col('hours-per-week') / F.col('total_workers_race'))
        .groupBy('race')
        .agg(F.round(F.sum('partial_mean_hour'), 2).alias('mean_hour_per_week'))
        .show())

def sexProportion(df):
    print("Questão 3")
    df = (df.groupBy('sex')
        .agg(F.sum('fnlwgt').alias('total_sex')))
    
    df = (df.withColumn('sex_percent',
                                F.round(F.col('total_sex')/F.sum('total_sex')
                                    .over(Window.partitionBy())*100, 2)))
    
    df.select('sex', 'sex_percent').show()

def workclassWorksMore(df):
    print('Question 5')
    df_aux = (df.groupBy(F.col('workclass').alias('workclass_a'))
                .agg(F.sum('fnlwgt').alias('workers_workclass')))
    
    df = df.join(df_aux, df.workclass == df_aux.workclass_a, 'inner').select(df.workclass, df.fnlwgt, df['hours-per-week'],df_aux.workers_workclass)

    (df.withColumn('mean_partial_workclass', F.col('fnlwgt') * F.col('hours-per-week') / F.col('workers_workclass'))
        .groupBy('workclass')
        .agg(F.round(F.sum('mean_partial_workclass'), 2).alias('mean_workclass_works_week'))
        .sort('mean_workclass_works_week', ascending = False)
        .show(1))

def workclassEducationLevel(df):
    print("Question 6")
    (df.filter(F.col('workclass') != '?')
        .groupBy('education', 'workclass')
        .agg(F.sum('fnlwgt').alias('total_workers'))
        .withColumn('workers_education_workclass', F.struct('total_workers', 'workclass'))
        .groupBy('education')
        .agg(F.max('workers_education_workclass').alias('most_common_workclass_education'))
        .show())

def workclassSex(df):
    print('Question 7')
    (df.filter(F.col('workclass') != '?')
        .groupBy('sex', 'workclass')
        .agg(F.sum('fnlwgt').alias('total_workers'))
        .withColumn('workers_workclass_sex', F.struct('total_workers', 'workclass'))
        .groupBy('sex')
        .agg(F.max('workers_workclass_sex').alias('most_common_workclass_sex'))
        .show())

def educationRace(df):
    print("Question 8")
    (df.withColumn('education_race', F.struct(df['education_nbr'], 'education'))
        .groupBy('race')
        .agg(F.max('education_race').alias('highest_education_race'))
        .select('race', 'highest_education_race.education')
        .show())

def educationSexRaceSelfEmp(df):
    print("Question 9")
    (df.withColumn('workclass', F.when(F.col('workclass').startswith('Self-emp'), 'self-emp'))
        .filter(F.col('workclass') == 'self-emp')
        .groupBy('workclass', 'race', 'education', 'sex')
        .agg(F.sum('fnlwgt').alias('total_workers'))
        .withColumn('self_emp_', F.struct('total_workers', 'race', 'education', 'sex'))
        .groupBy('workclass')
        .agg(F.max('self_emp_').alias('race_education_sex'))
        .select('workclass','race_education_sex.race', 'race_education_sex.education', 'race_education_sex.sex')
        .show())

def ratioMarriedUnmarried(df):
    print("Question 10")
    df = (df.groupBy('marital_status')
            .agg(F.sum('fnlwgt').alias('total_marital_status')))

    total_married = df.collect()[0][1]
    total_unmarried = df.collect()[1][1]
    ratio_married_unmarried = total_married/total_unmarried
    print(f"The ratio between married and unmarried is {ratio_married_unmarried:.2f}.")

def raceUnmarried(df):
    print('Question 11')
    (df.groupBy('marital_status', 'race')
        .agg(F.sum('fnlwgt').alias('total_marital_race'))
        .withColumn('total_marital', F.struct('total_marital_race', 'race'))
        .groupBy('marital_status')
        .agg(F.max('total_marital').alias('total_race'))
        .filter(F.col('marital_status') == 'unmarried')
        .select('marital_status', 'total_race.race')
        .show())

def incomeMarriedUnmarried(df):
    print("Question 12")
    (df.groupBy('marital_status', 'income')
        .agg(F.sum('fnlwgt').alias('total_income'))
        .withColumn('freq_income_marital', F.struct('total_income', 'income'))
        .groupBy('marital_status')
        .agg(F.max('freq_income_marital').alias('most_common'))
        .select('marital_status', 'most_common.income')
        .show())

def incomeSex(df):
    print('Question 13')
    (df.groupBy('sex', 'income')
        .agg(F.sum('fnlwgt').alias('total_income'))
        .withColumn('freq_income_sex', F.struct('total_income', 'income'))
        .groupBy('sex')
        .agg(F.max('freq_income_sex').alias('most_common'))
        .select('sex', 'most_common.income')
        .show())

def incomeNationality(df):
    print('Question 14')
    (df.filter(F.col('native-country') != '?')
        .groupBy('income', 'native-country')
        .agg(F.sum('fnlwgt').alias('total_income'))
        .withColumn('freq_income_nation', F.struct('total_income', 'income'))
        .groupBy('native-country')
        .agg(F.max('freq_income_nation').alias('most_common'))
        .select('native-country', 'most_common.income')
        .show())

def ratioWhiteNonWhite(df):
    print('Question 15')
    df = (df.groupBy('white')
            .agg(F.sum('fnlwgt').alias('total_people')))
    
    white = df.collect()[1][1]
    non_white = df.collect()[0][1]
    ratio_white_non = white/non_white
    print(f'The ratio between white and non-white is {ratio_white_non:.2f}.')

if __name__ == "__main__":
    sc = SparkContext()
    spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

    df = (spark.getOrCreate().read
                  .format("csv")
                  .option("header", "true")
                  #.schema(schema_census_income)
                  .load("./data/census-income/census-income.csv"))
    # df.show()
    df = removeWhiteSpaces(df)
    df = hoursPerWeek_float(df)
    df = finalWeight_int(df)
    df = educationNum_int(df)
    df = maritalStatus(df)
    df = whitePeople(df)
    
    # workerClassMost50K(df)
    # raceWorkHoursPerWeek(df)
    # sexProportion(df)
    # workclassWorksMore(df)
    # workclassEducationLevel(df)
    # workclassSex(df)
    # educationRace(df)
    # educationSexRaceSelfEmp(df)
    # ratioMarriedUnmarried(df)
    # raceUnmarried(df)
    # incomeMarriedUnmarried(df)
    # incomeSex(df)
    # incomeNationality(df)
    ratioWhiteNonWhite(df)