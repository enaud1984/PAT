import json
import logging
import os

from pat.class_tool import Storable
from pat.chain_of_responsability import DataProcess, ManagerChain, MemoryMap
from pat.utility import Common
from pyspark.sql.functions import col, lit


class ScriptProcess:
    def process(self, req):
        source_list = Common.to_list(req)
        storableList = []
        for source in source_list:
            logging.info(f"{source.data}")
            sourseAll = source.src_name
            df = source.data
            df.show()

            df = df.withColumn("year", lit(1))
            df = df.withColumn("month", lit(2))
            df = df.withColumn("day", lit(3))
            df = df.withColumnRenamed("km_percorsi", "treno_km")
            df = df.drop("progressivo")
            df.show()
            storable = Storable()
            storable.data = df
            storable.id_storable = "impala"
            storableList.append(storable)

        '''
        df = converter.toDataFrameFromList(list_)
        storable1 = Storable()
        storable1.data(df)
        storable1.id_storable("1")
        '''

        return storableList


# os.environ["CLASSPATH"] = "ojdbc8.jar"

# os.environ["CLASSPATH"] = "../../lib/ImpalaJDBC41.jar"
os.environ["CLASSPATH"] = "../../../lib/postgresql-42.2.14.jar"

DataProcess.process = ScriptProcess.process
# CONF_JSON = os.path.dirname(os.path.abspath(__file__)) + "/conf.json"
flow = ManagerChain()
flow.execute()
