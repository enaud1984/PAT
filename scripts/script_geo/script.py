from ingestion.class_tool import Storable
from ingestion.converter import JsonConverter
from ingestion.chain_of_responsability import DataProcess, ManagerChain
from ingestion.utility import Common
from ingestion.utility import Spark

'''
class ScriptProcess:
    def process(self, req):
        storable = Storable()
        storable.data(req[0].data())
        storable.id_storable("1")

        storable_list = [storable]
        return storable_list



#os.environ["CLASSPATH"] = "ImpalaJDBC41.jar"
#os.environ["CLASSPATH"] = "ojdbc8.jar"
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars C:/temp/ojdbc8.jar --driver-class-path C:/temp/ojdbc8.jar ./bin/spark-shell'

DataProcess.process=ScriptProcess.process
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
flow = ManagerChain(ROOT_DIR)
flow.execute()
'''

spark_conf = {
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "hive.exec.dynamic.partition": "true",
    "hive.exec.dynamic.partition.mode": "nonstrict",
    "hive.exec.max.dynamic.partitions": "1000",
    "hive.exec.max.dynamic.partition.pernode": "100",
    "spark.driver.extraClassPath": "c:/temp/ojdbc8.jar",
    "spark.executor.extraClassPath": "c:/temp/ojdbc8.jar"
}

import os

print("PYTHONPATH", os.getenv("PYTHONPATH"))
spark = Spark("test_spark", spark_conf)
print(spark)
