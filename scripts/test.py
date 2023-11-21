import requests
from pyspark.sql.types import *
from pyspark.sql import SparkSession,Row
import psycopg2

import os

#os.environ["CLASSPATH"] = "..\lib\postgresql-42.2.14.jar"

spark = SparkSession.builder.appName('SparkByExamples.com')\
    .config("spark.driver.extraClassPath", "..\lib\postgresql-42.2.14.jar")\
    .config("spark.executor.extraClassPath", "..\lib\postgresql-42.2.14.jar")\
    .getOrCreate()

x = requests.get('https://filesamples.com/samples/code/json/sample4.json')
data = x.json()
people = data['people']

db_url = "jdbc:postgresql://127.0.0.1:5432/postgres"
db_properties  = {
    "username": "postgres",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}
print(people)

schema = StructType([
            StructField('firstName', StringType(),True),
            StructField('lastName', StringType(),True),
            StructField('gender', StringType(),True),
            StructField('age', IntegerType(),True),
            StructField('number', StringType(),True),
            ])

df = spark.createDataFrame(people,schema)
df.show()
#rdd=df.rdd
#datas=rdd.map(tuple)
#data1=datas.collect()
df.write.jdbc(db_url,'public.people', properties=db_properties)
print("***SCRITTURA POSTGRES OK***")
#rdd = df.rdd
#data1 = rdd.map(tuple)
#data1= data1.collect()

#conn = psycopg2.connect(
#  database="py-test", user='postgres', password='admin', host='localhost', port= '5432'
#)
#cursor = conn.cursor()
#cursor.execute("select version()")
#data = cursor.fetchone()
#print("Connection established to: ",data)
#values_to_insert = []
#add=list(people[0].items())
#print(type(add))

#for person in people:
 #   add = list(person.items())
  #  values_to_insert.append(add)

#print(data1)
'''df.select("firstName","lastName","gender","age", "number").write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/py-test") \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "people") \
    .option("user", "postgres")\
    .option("password", "admin")\
    .save()
'''


#postgres_insert_query = " INSERT INTO people (\"firstName\", \"lastName\", gender, age, number) VALUES (%s,%s,%s,%s,%s)"


#cursor.execute(postgres_insert_query,data1)
#conn.commit()
#count = cursor.rowcount
#print(count, "Record inserted successfully into mobile table")


