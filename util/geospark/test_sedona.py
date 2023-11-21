import os
import traceback

from pyspark.sql import SparkSession, udf
from pyspark import SparkConf, SparkContext
from sedona.utils.adapter import Adapter
from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator

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
    config("spark.driver.extraClassPath", "c:/temp/postgresql-42.2.14.jar"). \
    config("spark.executor.extraClassPath", "c:/temp/postgresql-42.2.14.jar"). \
    getOrCreate()

SedonaRegistrator.registerAll(spark)

# oracle
'''
db_host = "10.64.20.10"
db_port = "1521"
db_service = "dssdbsvi"
db_driver = "oracle.jdbc.driver.OracleDriver"
db_user = "geodss_dev"
db_password = "EndpointS3cur1t1"

db_url = "jdbc:oracle:thin:@{}:{}/{}".format(db_user, db_port, db_service)
db_properties = {
    'host': db_host,
    'user': db_user,
    'password': db_password,
    'service': db_service,
    'driver': db_driver
}
db_table="(SELECT * FROM geodss_dev.ANTM_SOGLIE cg WHERE rownum <10)"
df_oracle= spark.read.jdbc(db_url, "ANTM_SOGLIE", properties=db_properties)
#resultsDF = spark.sql("SELECT ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ',') AS polygonshape")
df_oracle.show()
'''
# print(spark.catalog.listFunctions())
host = "10.206.226.173"
port = "5432"
database = "gis"
user = "usergis"
password = "usergis01"
schema = "network_cav"
driver = "org.postgresql.Driver"

url = "jdbc:postgresql://{}:{}/{}".format(host, port, database)
properties = {
    'host': host,
    'port': port,
    'user': user,
    'password': password,
    'database': database,
    'schema': schema,
    'driver': driver
}

try:
    # query="(select ST_GeomFromText(ST_AsText(geom)) as BinaryGeometry from network_cav.areas) as t"
    # query2="(select ST_ASEWKT(ST_GeomFromWKB(geom)) as BinaryGeometry from network_cav.areas) as t"
    query = "(select ST_AsText(geom) geom from network_cav.areas) as t"
    df_pg2 = spark.read.jdbc(url,
                             query,
                             properties=properties)
    df_pg2.createOrReplaceTempView("areas_v")

    df_pg3 = spark.sql("select ST_GeomFromWKT(geom) geom from areas_v")
    # df_pg3.show(truncate=False)
    # df_pg3.printSchema() #type: geometry
    df_pg3.createOrReplaceTempView("areas_write")
    # ST_WktFromGeom=udf(lambda x: x.toText, geometry())
    df_write = spark.sql("select ST_AsBinary(geom) geom from areas_write")
    # df_write.show(False)
    print(df_write.dtypes)
    # df_write.write.jdbc(url, "areas_test", properties=properties,mode="append")
    df_write.write.jdbc(url, "network_cav.areas_test", properties=properties, mode="append")



except Exception as e:
    traceback.print_exc()
