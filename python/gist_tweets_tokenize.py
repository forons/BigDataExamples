df = spark.read.csv('/FileStore/tables/tweets_cleaned.csv', header='true', inferSchema='true')


df.show()


df.printSchema()


df.select('text').show(truncate=False)

df = df.where(f.col('lang') == 'en')


from pyspark.ml.feature import StopWordsRemover, Tokenizer

tokenizer = Tokenizer(inputCol='text', outputCol='words')

tweets = tokenizer.transform(df)

tweets.select('text', 'words').show(truncate=False)


stop_words_remover = StopWordsRemover(inputCol='words', outputCol='filtered')

tweets_filtered = stop_words_remover.transform(tweets)

tweets_filtered.select('words', 'filtered').show()


def remove_duplicates(l):
  return list(set(l))

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import *

removeUDF = udf(remove_duplicates, ArrayType(StringType()))

tDF = tweets_filtered.withColumn('filtered1', removeUDF('filtered'))

tDF.select('filtered', 'filtered1').show()


from pyspark.ml.fpm import FPGrowth, FPGrowthModel

fpgrowth = FPGrowth(itemsCol='filtered1', minSupport=0.2, minConfidence=0.2)
model = fpgrowth.fit(tDF)
model.freqItemsets.show()
model.associationRules.show()


model.transform(tDF).select('prediction').show()

