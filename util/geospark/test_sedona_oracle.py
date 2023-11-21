import os
import traceback

from pyspark.sql import SparkSession, udf
from pyspark import SparkConf, SparkContext
from sedona.utils.adapter import Adapter
from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator
from pyspark.sql.functions import lit, col
from pyspark.sql.types import IntegerType, BooleanType, DateType, BinaryType

# os.environ["CLASSPATH"] = "postgresql-42.2.14.jar"
# os.environ["CLASSPATH"] = "ojdbc8.jar"


sedona_spark_2_3_3 = "org.apache.sedona:sedona-python-adapter-2.4_2.11:1.1.1-incubating,"
sedona_spark_3_0_2 = "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating,"

spark = SparkSession.builder.appName("sedonatest"). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-2.4_2.11:1.1.1-incubating,'
           'org.datasyslab:geotools-wrapper:1.1.0-25.2'). \
    config("spark.driver.extraClassPath", "c:/temp/ojdbc8.jar;c:/temp/xdb.jar"). \
    config("spark.executor.extraClassPath", "c:/temp/ojdbc8.jar;c:/temp/xdb.jar"). \
    getOrCreate()

SedonaRegistrator.registerAll(spark)

# oracle
db_host = "127.0.0.1"
db_port = "1521"
db_service = "ORCLCDB"
db_driver = "oracle.jdbc.driver.OracleDriver"
db_user = "ingestion"
db_password = "ingestion"

db_url = "jdbc:oracle:thin:@{}:{}/{}".format(db_host, db_port, db_service)
db_properties = {
    'host': db_host,
    'user': db_user,
    'password': db_password,
    'service': db_service,
    'driver': db_driver
}

db_table = "(SELECT SDO_UTIL.TO_WKBGEOMETRY(GEOM_32632) geom FROM INGESTION.CATASTO_GALLERIE cg WHERE rownum < 2)"
df_oracle = spark.read.jdbc(db_url, db_table, properties=db_properties)
df_oracle.createOrReplaceTempView("df_oracle")
df_oracle.show()
df_oracle.write.jdbc(db_url, "INGESTION.CATASTO_GALLERIE_BLOB", properties=db_properties, mode="append")

'''
db_table="(SELECT SDO_UTIL.TO_WKTGEOMETRY(GEOM_32632) geom FROM INGESTION.CATASTO_GALLERIE cg WHERE rownum < 2)"

#df_oracle= spark.read.jdbc(db_url, "ingestion.catasto_gallerie", properties=db_properties)
#df_oracle= spark.read.jdbc(db_url, db_table, properties=db_properties)
df_oracle= spark.read.jdbc(db_url, db_table, properties=db_properties)
df_oracle.createOrReplaceTempView("df_oracle")
df_oracle.show()

df_oracle.write.jdbc(db_url, "INGESTION.CATASTO_GALLERIE_varchar", properties=db_properties, mode="append")

df_blob = spark.sql("select ST_AsBinary(ST_GeomFromWKT(geom)) geom from df_oracle")
df_blob.write.jdbc(db_url, "INGESTION.CATASTO_GALLERIE_BLOB", properties=db_properties, mode="append")

#df_blob_2 =  df_oracle.withColumn("geom", col('geom').cast(BinaryType()))
#df_blob_2.write.jdbc(db_url, "INGESTION.CATASTO_GALLERIE_BLOB", properties=db_properties, mode="append")
'''

'''
df_write = spark.sql("select ST_GeomFromWKT(geom) geom from df_oracle")
df_write.show()
df_write.write.jdbc(db_url, "INGESTION.CATASTO_GALLERIE_TEST", properties=db_properties, mode="append")
'''
# resultsDF = spark.sql("SELECT ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ',') AS polygonshape")
