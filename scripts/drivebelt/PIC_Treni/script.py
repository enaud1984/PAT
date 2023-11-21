import json
import logging
import os
from pat.class_tool import Storable
from pat.converter import JsonConverter
from pat.chain_of_responsability import DataProcess, ManagerChain
from pat.utility import Common
from pyspark.sql.functions import col, lit, substring


class ScriptProcess:
    def process(self, req):
        source_list = Common.to_list(req)
        storableList = []
        for source in source_list:
            logging.info(f"{source.data}")
            df = source.data
            df = df.na.fill("", ["id_treno", "id_tratta", "origine", "destinazione", "tipologia_trazione",
                                 "nodo_riferimento"])
            df.show()

            df = df.withColumnRenamed("km_percorsi", "treno_km")
            df = df.drop("progressivo")
            df = df.withColumn("year", substring('id_treno', 1, 2).cast('int') + 2000)
            df = df.withColumn("month", substring('id_treno', 3, 2).cast('int'))
            df = df.withColumn("day", substring('id_treno', 5, 2).cast('int'))
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
# os.environ["CLASSPATH"] = "../../../lib/postgresql-42.2.14.jar"

DataProcess.process = ScriptProcess.process
# CONF_JSON = os.path.dirname(os.path.abspath(__file__)) + "/conf.json"
flow = ManagerChain()
flow.init_default_logger()
flow.execute()
