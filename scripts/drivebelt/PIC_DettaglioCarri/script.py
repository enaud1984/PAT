import json
import logging
import os
from pat.class_tool import Storable
from pat.converter import JsonConverter
from pat.chain_of_responsability import DataProcess, ManagerChain, mapped_values
from pat.utility import Common
from pyspark.sql.functions import col, lit, substring

# DataOraPrevistoArrivo as arrivo_dttm::timestamp(%Y-%m-%dT%H:%M:%SZ)


schema = [
    {
        "IDTreno#id_treno": "",
        "IDTratta#id_tratta": "",
        "DataOraPrevistoArrivo#arrivo_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None,
        "DataOraPrevistoPartenza#partenza_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None,
        "Origine#origine": "",
        "Destinazione#destinazione": "",
        "IDCarro#id_carro": "",
        "KmPercorsi#distanza_percorsa(int)": None,
        "CarroRetrofillato#carro_retr_flg": "",
        "DataDiIngestion#ingestion_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None,
        "DataDiModifica#modifica_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None
    }
]


class ScriptProcess:
    def process(self, req):
        converter = JsonConverter(schema)
        list_ = []
        source_list = Common.to_list(req)

        for source in source_list:
            logging.info(f"{source.data}")
            data_list = Common.to_list(source.data)
            try:
                source_current = ""
                for index, data in enumerate(data_list):
                    source_current = source.src_name[index]
                    print("***FILE JSON***", source_current)
                    s1 = json.dumps(data.decode("utf-8"))
                    d2 = json.loads(s1)
                    list_.extend(converter.toList(d2, header=False))
            except Exception as e:
                print("ERROR", e)
                print("***FILE JSON***", source_current)
                raise e
        spark = mapped_values.get(self.id_transaction).get("spark")

        # list_.insert(0,converter.getHeader())
        df = converter.toDataFrameFromList(list_, spark.spark)
        df.show()

        df = df.withColumn("year", substring('id_treno', 1, 2).cast('int') + 2000)
        df = df.withColumn("month", substring('id_treno', 3, 2).cast('int'))
        df = df.withColumn("day", substring('id_treno', 5, 2).cast('int'))
        df.show()

        logging.info(f"PicDettaglioCarri - len(list_) - {len(list_)}")
        logging.info(f"PicDettaglioCarri - list_ - {list_}")

        storable = Storable()
        storable.data = df
        storable.id_storable = "impala"

        '''
        df = converter.toDataFrameFromList(list_)
        storable1 = Storable()
        storable1.data(df)
        storable1.id_storable("1")
        '''

        return [storable]


# os.environ["CLASSPATH"] = "ojdbc8.jar"

# os.environ["CLASSPATH"] = "../../lib/ImpalaJDBC41.jar"
# os.environ["CLASSPATH"] = "../../lib/postgresql-42.2.14.jar"

DataProcess.process = ScriptProcess.process
# CONF_JSON = os.path.dirname(os.path.abspath(__file__)) + "/conf.json"
flow = ManagerChain()
flow.init_default_logger()
flow.execute()
