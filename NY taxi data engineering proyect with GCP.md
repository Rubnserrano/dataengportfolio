<a name="br1"></a> 

**NY taxi data engineering proyect with GCP**

Tools that we are going to use:

Lucid to create a diagram of the tables

Jupyter Notebooks

Pyspark

GCP tools like Google Cloud Storage, Dataproc, Big Query and Looker

**Basic concepts**

[Fact table]:

contains quantitative measures or metrics that are used for analysis

tipically contains foreign keys that link to dimension tables

contains collumns that have [high cardinallity] **and change frequently**

a table that has a high variety of unique values. eg: user\_id

contains columns that are not useful for analysis by themselves, but necessary to calculate metrics

[Dimension table]:

contains columns that describe attributes of the data being analyzed

tipically contains primary keys that link to fact tables

contains columns that have [low cardinality] **and don't change frequently**

a table that has a limited variety of unique values. eg: gender (can be men, women, nb)

contains columns that can be used for grouping or filtering data for analysis

**1.- Download and load data into Jupyter**

Download parquet data in <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>[ ](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)and transform it into csv

with:

`df = pd.read\_parquet("C:\Users\ruben\Desktop\data engineering\projects\tlc\_nyc\yellow\_tripdata\_2022-

01\.parquet")

df.to\_csv("C:\\Users\\ruben\\Desktop\\data engineering\\projects\\tlc\_nyc\\yellow\_22-01.csv",

index=False)

**1.1.- Create the fact and dim tables diagram with LucidCharts**



<a name="br2"></a> 

**1.2.- Writing the transformations in jupyter notebook**

Creamos el código de las transfomaciones necesarias para pasar los datos en bruto a la configuración fact

table / dimension tables con pyspark.

El código quedaría algo así:

from pyspark.sql.functions import when

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, date\_format, hour

from pyspark.sql.functions import monotonically\_increasing\_id

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, date\_format, hour

from pyspark.sql.functions import monotonically\_increasing\_id

from pyspark.sql.functions import col

df = spark.read.parquet("C:\\Users\\ruben\\Desktop\\data

engineering\\projects\\tlc\_nyc\\pyspark\\yellow\_tripdata\_2022-01.parquet")

sample\_df = df.sample(fraction=1.0, withReplacement=False, seed=42)

sample\_df = sample\_df.withColumn('trip\_id', monotonically\_increasing\_id())



<a name="br3"></a> 

datetime\_dim = sample\_df[['tpep\_pickup\_datetime','tpep\_dropoff\_datetime']]

#Creamos un índice que nos valdrá de identificador.

datetime\_dim = datetime\_dim.withColumn("datetime\_id", monotonically\_increasing\_id())

#Dividimos el timestamp de pick y drop en sus respectivo hora, dia, mes, año y día de la semana

creando columnas con .withColumn

datetime\_dim = datetime\_dim.withColumn("pick\_hour", hour(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("pick\_day",

dayofmonth(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("pick\_month",

month(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("pick\_year", year(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("pick\_weekday",

dayofweek(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_hour",

hour(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_day",

dayofmonth(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_month",

month(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_year",

year(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_weekday",

dayofweek(datetime\_dim["tpep\_dropoff\_datetime"]))

\# Reordenamos el dataset

datetime\_dim = datetime\_dim.select(\*[['datetime\_id','tpep\_pickup\_datetime', 'pick\_hour',

'pick\_day', 'pick\_month', 'pick\_year', 'pick\_weekday',

'tpep\_dropoff\_datetime', 'drop\_hour', 'drop\_day', 'drop\_month',

'drop\_year', 'drop\_weekday']] )

passenger\_count\_dim = sample\_df[['passenger\_count']]

passenger\_count\_dim = passenger\_count\_dim.withColumn("passenger\_count\_id",

monotonically\_increasing\_id())

passenger\_count\_dim = passenger\_count\_dim.select(\*[['passenger\_count\_id', 'passenger\_count']])

trip\_distance\_dim = sample\_df[['trip\_distance']]

trip\_distance\_dim = trip\_distance\_dim.withColumn("trip\_distance\_id",

monotonically\_increasing\_id())

trip\_distance\_dim = trip\_distance\_dim.select(\*[['trip\_distance\_id', 'trip\_distance']])

rate\_code\_type = {

1 :"Standard rate",

2 :"JFK",



<a name="br4"></a> 

3 :"Newark",

4 :"Nassau or Westchester",

5 :"Negotiated fare",

6:"Group ride"

}

rate\_code\_dim = sample\_df[['RatecodeID']]

rate\_code\_dim = rate\_code\_dim.withColumn("rate\_code\_id", monotonically\_increasing\_id())

\# ¿Por qué esto no funciona? Que forma tengo de hacerlo más eficaz?

\# for code, name in rate\_code\_type.items():

\#

rate\_code\_dim = rate\_code\_dim.withColumn("rate\_code\_name", when(col("RatecodeID") ==

code, name))

rate\_code\_dim = rate\_code\_dim.withColumn("rate\_code\_name", when(col("RatecodeID") == 1,

rate\_code\_type[1])

.when(col("RatecodeID") == 2, rate\_code\_type[2])

.when(col("RatecodeID") == 3, rate\_code\_type[3])

.when(col("RatecodeID") == 4, rate\_code\_type[4])

.when(col("RatecodeID") == 5, rate\_code\_type[5])

.when(col("RatecodeID") == 6, rate\_code\_type[6])

.otherwise("Unknown"))

rate\_code\_dim = rate\_code\_dim.select(\*[['rate\_code\_id', 'RatecodeID', 'rate\_code\_name']])

payment\_type\_name = {

1:"Credit card",

2:"Cash",

3:"No charge",

4:"Dispute",

5:"Unknown",

6:"Voided trip"

}

payment\_type\_dim = sample\_df[['payment\_type']]

payment\_type\_dim = payment\_type\_dim.withColumn("payment\_type\_id",

monotonically\_increasing\_id())

payment\_type\_dim = payment\_type\_dim.withColumn("payment\_type\_name", when(col("payment\_type") ==

1, rate\_code\_type[1])

.when(col("payment\_type") == 2, payment\_type\_name[2])

.when(col("payment\_type") == 3, payment\_type\_name[3])

.when(col("payment\_type") == 4, payment\_type\_name[4])

.when(col("payment\_type") == 5, payment\_type\_name[5])

.when(col("payment\_type") == 6, payment\_type\_name[6])



<a name="br5"></a> 

.otherwise("Unknown"))

payment\_type\_dim = payment\_type\_dim.select(\*[['payment\_type\_id', 'payment\_type',

'payment\_type\_name']])

taxi\_zone = spark.read.csv("C:\\Users\\ruben\\Desktop\\data

engineering\\projects\\tlc\_nyc\\datasets\\taxi\_zone.csv", header = True)

pickup\_location\_dim = sample\_df[['PULocationID']]

pickup\_location\_dim = pickup\_location\_dim.withColumn('pickup\_location\_id',

monotonically\_increasing\_id())

pickup\_location\_dim = pickup\_location\_dim.join(taxi\_zone, pickup\_location\_dim["PULocationID"]

== taxi\_zone["LocationID"], "left")

pickup\_location\_dim = pickup\_location\_dim.select(\*[['pickup\_location\_id', 'PULocationID',

'Borough', 'Zone', 'service\_zone']])

pickup\_location\_dim = pickup\_location\_dim.withColumnRenamed("Borough", "Borough\_pickup") \

.withColumnRenamed("Zone", "Zone\_pickup") \

.withColumnRenamed("service\_zone", "service\_zone\_pickup")

drop\_location\_dim = sample\_df[['DOLocationID']]

drop\_location\_dim = drop\_location\_dim.withColumn('drop\_location\_id',

monotonically\_increasing\_id())

drop\_location\_dim = drop\_location\_dim.join(taxi\_zone, drop\_location\_dim["DOLocationID"] ==

taxi\_zone["LocationID"], "left")

drop\_location\_dim = drop\_location\_dim.select(\*[['drop\_location\_id', 'DOLocationID', 'Borough',

'Zone', 'service\_zone']])

drop\_location\_dim = drop\_location\_dim.withColumnRenamed("Borough", "Borough\_drop") \

.withColumnRenamed("Zone", "Zone\_drop") \

.withColumnRenamed("service\_zone", "service\_zone\_drop")

\# Realizar la unión de DataFrames con diferentes columnas de unión

fact\_table = sample\_df.join(passenger\_count\_dim, sample\_df["trip\_id"] ==

passenger\_count\_dim["passenger\_count\_id"], "inner") \

.join(trip\_distance\_dim, sample\_df["trip\_id"] == trip\_distance\_dim["trip\_distance\_id"],

"inner") \

.join(rate\_code\_dim, sample\_df["trip\_id"] == rate\_code\_dim["rate\_code\_id"], "inner") \

.join(datetime\_dim, sample\_df["trip\_id"] == datetime\_dim["datetime\_id"], "inner") \

.join(payment\_type\_dim, sample\_df["trip\_id"] == payment\_type\_dim["payment\_type\_id"],

"inner") \

.join(pickup\_location\_dim, sample\_df["trip\_id"] ==

pickup\_location\_dim["pickup\_location\_id"], "inner") \

.join(drop\_location\_dim, sample\_df["trip\_id"] == drop\_location\_dim["drop\_location\_id"],

"inner")



<a name="br6"></a> 

fact\_table = fact\_table.select(\*[['trip\_id','VendorID', 'datetime\_id', 'passenger\_count\_id',

'trip\_distance\_id', 'rate\_code\_id', 'store\_and\_fwd\_flag', 'pickup\_location\_id',

'drop\_location\_id',

'payment\_type\_id', 'fare\_amount', 'extra', 'mta\_tax', 'tip\_amount',

'tolls\_amount',

'improvement\_surcharge', 'airport\_fee', 'congestion\_surcharge',

'total\_amount']])

fact\_table.show(10)

**Google Cloud Platform**

Una vez creado la template de trasformación en un notebook, vamos a modificarla en un archivo .py para

que coja los datos de google cloud storage los procese y los mande a BigQuery.

El código del archivo .py configurado para usarse en un cluster de Dataproc es el siguiente:

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EjemploSpark").getOrCreate()

df = spark.read.parquet("gs://batch\_taxis/yellow\_tripdata\_2022-01.parquet")

from pyspark.sql.functions import when

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, date\_format, hour

from pyspark.sql.functions import monotonically\_increasing\_id

from pyspark.sql.functions import year, month, dayofmonth, dayofweek, date\_format, hour

from pyspark.sql.functions import monotonically\_increasing\_id

from pyspark.sql.functions import col

sample\_df = df.sample(fraction=1.0, withReplacement=False, seed=42)

sample\_df = sample\_df.withColumn('trip\_id', monotonically\_increasing\_id())

datetime\_dim = sample\_df[['tpep\_pickup\_datetime','tpep\_dropoff\_datetime']]

#Creamos un Ãndice que nos valdrÃ¡ de identificador.

datetime\_dim = datetime\_dim.withColumn("datetime\_id", monotonically\_increasing\_id())

#Dividimos el timestamp de pick y drop en sus respectivo hora, dia, mes, aÃ±o y dÃa de la

semana creando columnas con .withColumn

datetime\_dim = datetime\_dim.withColumn("pick\_hour", hour(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("pick\_day",

dayofmonth(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("pick\_month",

month(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("pick\_year", year(datetime\_dim["tpep\_pickup\_datetime"]))



<a name="br7"></a> 

datetime\_dim = datetime\_dim.withColumn("pick\_weekday",

dayofweek(datetime\_dim["tpep\_pickup\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_hour",

hour(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_day",

dayofmonth(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_month",

month(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_year",

year(datetime\_dim["tpep\_dropoff\_datetime"]))

datetime\_dim = datetime\_dim.withColumn("drop\_weekday",

dayofweek(datetime\_dim["tpep\_dropoff\_datetime"]))

\# Reordenamos el dataset

datetime\_dim = datetime\_dim.select(\*[['datetime\_id','tpep\_pickup\_datetime', 'pick\_hour',

'pick\_day', 'pick\_month', 'pick\_year', 'pick\_weekday',

'tpep\_dropoff\_datetime', 'drop\_hour', 'drop\_day', 'drop\_month',

'drop\_year', 'drop\_weekday']] )

passenger\_count\_dim = sample\_df[['passenger\_count']]

passenger\_count\_dim = passenger\_count\_dim.withColumn("passenger\_count\_id",

monotonically\_increasing\_id())

passenger\_count\_dim = passenger\_count\_dim.select(\*[['passenger\_count\_id', 'passenger\_count']])

trip\_distance\_dim = sample\_df[['trip\_distance']]

trip\_distance\_dim = trip\_distance\_dim.withColumn("trip\_distance\_id",

monotonically\_increasing\_id())

trip\_distance\_dim = trip\_distance\_dim.select(\*[['trip\_distance\_id', 'trip\_distance']])

rate\_code\_type = {

1 :"Standard rate",

2 :"JFK",

3 :"Newark",

4 :"Nassau or Westchester",

5 :"Negotiated fare",

6:"Group ride"

}

rate\_code\_dim = sample\_df[['RatecodeID']]

rate\_code\_dim = rate\_code\_dim.withColumn("rate\_code\_id", monotonically\_increasing\_id())

\# Â¿Por quÃ© esto no funciona? Que forma tengo de hacerlo mÃ¡s eficaz?

\# for code, name in rate\_code\_type.items():

\#

rate\_code\_dim = rate\_code\_dim.withColumn("rate\_code\_name", when(col("RatecodeID") ==

code, name))



<a name="br8"></a> 

rate\_code\_dim = rate\_code\_dim.withColumn("rate\_code\_name", when(col("RatecodeID") == 1,

rate\_code\_type[1])

.when(col("RatecodeID") == 2, rate\_code\_type[2])

.when(col("RatecodeID") == 3, rate\_code\_type[3])

.when(col("RatecodeID") == 4, rate\_code\_type[4])

.when(col("RatecodeID") == 5, rate\_code\_type[5])

.when(col("RatecodeID") == 6, rate\_code\_type[6])

.otherwise("Unknown"))

rate\_code\_dim = rate\_code\_dim.select(\*[['rate\_code\_id', 'RatecodeID', 'rate\_code\_name']])

payment\_type\_name = {

1:"Credit card",

2:"Cash",

3:"No charge",

4:"Dispute",

5:"Unknown",

6:"Voided trip"

}

payment\_type\_dim = sample\_df[['payment\_type']]

payment\_type\_dim = payment\_type\_dim.withColumn("payment\_type\_id",

monotonically\_increasing\_id())

payment\_type\_dim = payment\_type\_dim.withColumn("payment\_type\_name", when(col("payment\_type") ==

1, rate\_code\_type[1])

.when(col("payment\_type") == 2, payment\_type\_name[2])

.when(col("payment\_type") == 3, payment\_type\_name[3])

.when(col("payment\_type") == 4, payment\_type\_name[4])

.when(col("payment\_type") == 5, payment\_type\_name[5])

.when(col("payment\_type") == 6, payment\_type\_name[6])

.otherwise("Unknown"))

payment\_type\_dim = payment\_type\_dim.select(\*[['payment\_type\_id', 'payment\_type',

'payment\_type\_name']])

taxi\_zone = spark.read.csv("gs://batch\_taxis/taxi\_zone.csv", header = True)

pickup\_location\_dim = sample\_df[['PULocationID']]

pickup\_location\_dim = pickup\_location\_dim.withColumn('pickup\_location\_id',

monotonically\_increasing\_id())

pickup\_location\_dim = pickup\_location\_dim.join(taxi\_zone, pickup\_location\_dim["PULocationID"]



<a name="br9"></a> 

== taxi\_zone["LocationID"], "left")

pickup\_location\_dim = pickup\_location\_dim.select(\*[['pickup\_location\_id', 'PULocationID',

'Borough', 'Zone', 'service\_zone']])

pickup\_location\_dim = pickup\_location\_dim.withColumnRenamed("Borough", "Borough\_pickup") \

.withColumnRenamed("Zone", "Zone\_pickup") \

.withColumnRenamed("service\_zone", "service\_zone\_pickup")

drop\_location\_dim = sample\_df[['DOLocationID']]

drop\_location\_dim = drop\_location\_dim.withColumn('drop\_location\_id',

monotonically\_increasing\_id())

drop\_location\_dim = drop\_location\_dim.join(taxi\_zone, drop\_location\_dim["DOLocationID"] ==

taxi\_zone["LocationID"], "left")

drop\_location\_dim = drop\_location\_dim.select(\*[['drop\_location\_id', 'DOLocationID', 'Borough',

'Zone', 'service\_zone']])

drop\_location\_dim = drop\_location\_dim.withColumnRenamed("Borough", "Borough\_drop") \

.withColumnRenamed("Zone", "Zone\_drop") \

.withColumnRenamed("service\_zone", "service\_zone\_drop")

\# Realizar la uniÃ³n de DataFrames con diferentes columnas de uniÃ³n

fact\_table = sample\_df.join(passenger\_count\_dim, sample\_df["trip\_id"] ==

passenger\_count\_dim["passenger\_count\_id"], "inner") \

.join(trip\_distance\_dim, sample\_df["trip\_id"] == trip\_distance\_dim["trip\_distance\_id"],

"inner") \

.join(rate\_code\_dim, sample\_df["trip\_id"] == rate\_code\_dim["rate\_code\_id"], "inner") \

.join(datetime\_dim, sample\_df["trip\_id"] == datetime\_dim["datetime\_id"], "inner") \

.join(payment\_type\_dim, sample\_df["trip\_id"] == payment\_type\_dim["payment\_type\_id"],

"inner") \

.join(pickup\_location\_dim, sample\_df["trip\_id"] ==

pickup\_location\_dim["pickup\_location\_id"], "inner") \

.join(drop\_location\_dim, sample\_df["trip\_id"] == drop\_location\_dim["drop\_location\_id"],

"inner")

fact\_table = fact\_table.select(\*[['trip\_id','VendorID', 'datetime\_id', 'passenger\_count\_id',

'trip\_distance\_id', 'rate\_code\_id', 'store\_and\_fwd\_flag', 'pickup\_location\_id',

'drop\_location\_id',

'payment\_type\_id', 'fare\_amount', 'extra', 'mta\_tax', 'tip\_amount',

'tolls\_amount',

'improvement\_surcharge', 'airport\_fee', 'congestion\_surcharge',

'total\_amount']])

fact\_table.show(10)

fact\_table.printSchema()

tablesnames = ['passenger\_count\_dim', 'trip\_distance\_dim','rate\_code\_dim','datetime\_dim',

'payment\_type\_dim', 'pickup\_location\_dim', 'drop\_location\_dim']



<a name="br10"></a> 

\# fact\_table.write.csv("gs://batch\_taxis/data001.csv")

\# fact\_table.write.format("bigquery") \

\#

\#

.option("table", "data\_taxi\_batch.clean\_data") \

.save()

bucket = "batch\_taxis"

spark.conf.set('temporaryGcsBucket', bucket)

tablesnames = [fact\_table, passenger\_count\_dim, trip\_distance\_dim,rate\_code\_dim,datetime\_dim,

payment\_type\_dim, pickup\_location\_dim, drop\_location\_dim]

rutasnames = ['data\_taxi\_batch.fact\_table', 'data\_taxi\_batch.passenger', '

data\_taxi\_batch.tripdistance', 'data\_taxi\_batch.ratecode', 'data\_taxi\_batch.datetime',

'data\_taxi\_batch.paymenttype', 'data\_taxi\_batch.pickuploc', 'data\_taxi\_batch.droploc']

for tablasaux, rutasaux in zip(tablesnames, rutasnames):

tablasaux.write.format('bigquery') \

.option('table', rutasaux) \

.option('mode', 'overwrite') \

.save()

spark.stop()

**Arquitectura Batch Pipeline en GCP**

**Dataproc**

**Submit job en Dataproc**



<a name="br11"></a> 

Lo primero que vamos a hacer es descargar el archivo .py que guardo en un repositorio de github para

guardar las actualizaciones del código desde allí.

git -C ~ clone https://github.com/Rubnserrano/dataengportfolio

cd dataengportoflio

gsutil cp tlc\_new.py gs://batch\_taxis/

Ahora mandamos el job a un cluster de dataproc

gcloud dataproc jobs submit pyspark gs://batch\_taxis/tlc\_new.py

--region=us-central1

--cluster=prueba123241

**BigQuery**

Una vez el trabajo ha finalizado, en tendremos las tablas del diagrama E-R. Uniremos todas las tablas según

las claves para crear una tabla de analíticas.

**Query para unir todos los datos**

`` CREATE OR REPLACE TABLE taxi-project-405909.data\_taxi\_batch.analitycs` AS (

SELECT

f.trip\_id,

d.tpep\_pickup\_datetime,

d.tpep\_dropoff\_datetime,

p.passenger\_count,

t.trip\_distance,

r.rate\_code\_name,

pay.payment\_type\_name,

f.fare\_amount,

f.extra,

f.mta\_tax,

f.tip\_amount,

f.tolls\_amount,

f.improvement\_surcharge,

f.total\_amount,

pick.Borough\_pickup,

pick.Zone\_pickup,

k.Borough\_drop,

k.Zone\_drop,

FROM

taxi-project-405909.data\_taxi\_batch.fact\_table f



<a name="br12"></a> 

JOIN taxi-project-405909.data\_taxi\_batch.datetime d ON f.datetime\_id=d.datetime\_id

JOIN taxi-project-405909.data\_taxi\_batch.passenger p ON p.passenger\_count\_id=f.passenger\_count\_id

JOIN taxi-project-405909.data\_taxi\_batch.tripdistance t ON t.trip\_distance\_id=f.trip\_distance\_id

JOIN taxi-project-405909.data\_taxi\_batch.ratecode r ON r.rate\_code\_id=f.rate\_code\_id

JOIN taxi-project-405909.data\_taxi\_batch.pickuploc pick ON pick.pickup\_location\_id=f.pickup\_location\_id

JOIN taxi-project-405909.data\_taxi\_batch.droploc k ON k.drop\_location\_id=f.drop\_location\_id

JOIN taxi-project-405909.data\_taxi\_batch.paymenttype pay ON

pay.payment\_type\_id=f.payment\_type\_id)```

Con el 100% de los datos de 2022 en local tarda 18s.

**Looker Studio**

**Report para datos Enero del 2022**

