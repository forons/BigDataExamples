import pyspark
import pyspark.sql.functions as f

import pandas as pd

pd.set_option('display.max_colwidth', None)
pd.set_option('max_columns', None)

def show(df: pyspark.sql.DataFrame, limit: int = 40):
    return df.limit(limit).toPandas().head(limit)


spark = pyspark.sql.SparkSession.builder.master('local[*]').getOrCreate()

df = spark.read.format("json").load('/path-to/tweets/World_Cup_Final.json')

show(df)

df.printSchema()

df.count()

show(df.where((f.lower(f.col('text')).contains('havertz'))))

show(df.where((f.lower(f.col('text')).contains('havertz')) & (f.lower(f.col('text')).contains('42')) & (f.lower(f.col('text')).contains('goal'))))

show(df.sort('timestamp_ms', ascending=False))


show(df.where(f.col('place').isNotNull()).select('place.country').groupby('country').count().sort('count', ascending=False), limit=40)



df.withColumn('timestamp_ms_plus1', f.col('timestamp_ms') + 1).sort('timestamp_ms_plus1', ascending=False).limit(10).toPandas().head()


spark.sql("""
    SELECT * FROM df WHERE user.name LIKE '%Danie%' AND place.country LIKE '%italy%'
""").limit(10).toPandas().head()






df.select('Hashtags').limit(10).toPandas().head()




df.withColumn('HashtagList', f.split(f.col('Hashtags'), ',')).select('HashtagList').limit(10).toPandas().head()




a = df.withColumn('HashtagList', f.split(f.col('Hashtags'), ',')).select(f.explode('HashtagList').alias('hash')).select(f.lower('hash').alias('pippo')).groupby('pippo').count().sort('count', ascending=False)
a.where(a.pippo.contains('ita')).limit(10).toPandas().head()




==================SENTIMENT ANALYSIS


df.groupby('source').count().sort('count', ascending=False).show()


df_new = df_data.withColumn('SupportTeam', f.when(f.lower(f.col('Tweet')).contains('chelsea'), 'che').otherwise(f.when(f.lower(f.col('Tweet')).contains('manchester city'), 'city'))).where(f.col('SupportTeam').isNotNull())


from textblob import TextBlob
# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity


from pyspark.sql.types import *

polarity_detection_udf = f.udf(polarity_detection, FloatType())
df_k = df_new.withColumn("polarity", polarity_detection_udf("Tweet"))





df_a = df_k.withColumn('polarity', f.col('polarity').cast('float'))
df_a = df_a.where(df_k.polarity != 0.0)

# df_k = df_k.where((df_k.SupportTeam == 'france') & (f.lower(df_k.Tweet).contains('france')) & (~f.lower(df_k.Tweet).contains('croatia')))
df_a.groupby('SupportTeam').agg(f.min('polarity'), f.max('polarity'), f.mean('polarity'), f.stddev('polarity')).limit(10).toPandas().head()



















import matplotlib.pyplot as plt



data = df_a.toPandas()

data.set_index(pd.DatetimeIndex(data['timestamp_ms']), inplace=True)

data_cro = data[data['Tweet'].str.contains('croatia', case=False)]
# data_cro = data_cro[data_cro['Tweet'].str.contains('france', case=False) == False]

data_fra = data[data['Tweet'].str.contains('france', case=False)]
# data_fra = data_fra[data_fra['Tweet'].str.contains('croatia', case=False) == False]

polarity_series_cro = data_cro['polarity']
polarity_series_cro = polarity_series_cro.resample('2Min').mean()

polarity_series_fra = data_fra['polarity']
polarity_series_fra = polarity_series_fra.resample('2Min').mean()

polarity_series_cro.dropna(inplace=True)
polarity_series_fra.dropna(inplace=True)








fig = plt.figure(figsize=(10, 5))
ax = fig.add_subplot(111)
ax.set_title('ManCity vs Chelsea Sentiment')


polarity_series_cro.plot(ax=ax, color='cyan', label='ManCity')
polarity_series_fra.plot(ax=ax, color='Blue', label='Chelsea')
ax.legend(loc='lower right')
ax.set_xlabel('Time')
ax.set_ylabel('Sentiment')

key_events = [(pd.to_datetime('Sat May 30 19:00:00 +0000 2021'), 'Kick-Off'),
              (pd.to_datetime('Sat May 30 19:30:00 +0000 2021'), 'Some event'),
              (pd.to_datetime('Sat May 30 20:00:00 +0000 2021'), 'Some other event'),
              (pd.to_datetime('Sat May 30 20:30:00 +0000 2021'), 'Chelsea goal! (Havertz)'),
              (pd.to_datetime('Sat May 30 21:00:00 +0000 2021'), 'Half Time'),
              (pd.to_datetime('Sat May 30 21:30:00 +0000 2021'), 'Second Half'),
              (pd.to_datetime('Sat May 30 22:00:00 +0000 2021'), 'Let\'s assume something important happened here'),
              (pd.to_datetime('Sat May 30 22:30:00 +0000 2021'), 'Manchester City shot out of target'),
              (pd.to_datetime('Sat May 30 23:00:00 +0000 2021'), 'Some red card that didnt happen'),
              (pd.to_datetime('Sat May 30 23:30:00 +0000 2021'), 'Full Time')
              ]

for event in key_events:
    ax.axvline(event[0], color='black')
    ax.text(event[0] + pd.Timedelta(1, 'm'), 0.63, event[1], rotation=90, size=7)

plt.show()