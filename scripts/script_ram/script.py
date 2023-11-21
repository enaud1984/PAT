import os
from datetime import datetime

from ingestion.class_tool import Storable
from ingestion.chain_of_responsability import DataProcess, ManagerChain
from pyspark.sql.functions import lit
from ingestion.utility import Json


class ScriptProcess:
    def process(self, req):
        df_local = None
        df_remote = None
        date_time_str = Json.get_value(self.option, "now")
        now = datetime.strptime(date_time_str, '%d-%m-%Y %H:%M:%S')

        for src in req:
            if src.id_source() == "local":
                df_local = src.data()
            elif src.id_source() == "remote":
                df_remote = src.data()

        df_1 = df_local.subtract(df_remote)
        df_2 = df_remote.subtract(df_local)

        # df_1.show(10,False)
        # df_2.show(10,False)

        cond = self.option["primary_key"]
        df_upd = df_1.join(df_2, cond, 'inner').select(*[x for x in df_1.columns if x in cond],
                                                       lit("U").alias("OPERATION"), lit(now).alias("TS"))
        df_del = df_1.join(df_2, cond, "left_anti").select(*[x for x in df_1.columns if x in cond],
                                                           lit("D").alias("OPERATION"), lit(now).alias("TS"))
        df_ins = df_2.join(df_1, cond, "left_anti").select(*[x for x in df_1.columns if x in cond],
                                                           lit("I").alias("OPERATION"), lit(now).alias("TS"))

        df = df_upd.union(df_del).union(df_ins)
        # df.show(15,False)

        storable_1 = Storable()
        storable_1.data(df)
        storable_1.id_storable("1")

        storable_2 = Storable()
        storable_2.data(None)
        storable_2.id_storable("2")

        storable_list = []
        storable_list.append(storable_1)
        storable_list.append(storable_2)
        return storable_list


os.environ["CLASSPATH"] = "../../lib/postgresql-42.2.14.jar"

DataProcess.process = ScriptProcess.process
ROOT_DIR = os.path.dirname(os.path.abspath(__file__)) + "/conf.json"
flow = ManagerChain(ROOT_DIR)
flow.execute()
