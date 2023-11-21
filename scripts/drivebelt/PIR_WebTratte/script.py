import json
import logging
import os
from pat.class_tool import Storable
from pat.converter import JsonConverter
from pat.chain_of_responsability import DataProcess, ManagerChain, mapped_values
from pat.utility import Common

# DataOraPrevistoArrivo as arrivo_dttm::timestamp(%Y-%m-%dT%H:%M:%SZ)

schema = [
    {
        "IDTratta#id_tratta": "",
        "ClasseTratta#classe": "",
        "LunghezzaTratta#lunghezza(double)": None,
        "Regione#regione": "",
        "EstensioneTerritoriale#estensione_territoriale(double)": None,
        "DoppioBinario#doppio_binario_flg": "",
        "TipologiaRete#tipologia_rete": "",
        "PresenzaCTC#ctc_flg": "",
        "PresenzaSCMT#scmt_flg": "",
        "PresenzaERMTS#ermts_flg": "",
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
            sourseAll = source.src_name
            data_list = Common.to_list(source.data)
            i = 0
            try:
                source_current = ""
                for data in data_list:
                    source_current = sourseAll[i]
                    print("***FILE JSON***", source_current)
                    s1 = json.dumps(data.decode("utf-8"))
                    d2 = json.loads(s1)
                    list_.extend(converter.toList(d2, header=False))
                    i += 1
            except Exception as e:
                print("ERROR", e)
                print("***FILE JSON***", source_current)
                raise e
        spark = mapped_values.get(self.id_transaction).get("spark")

        # list_.insert(0,converter.getHeader())
        df = converter.toDataFrameFromList(list_, spark.spark)

        logging.info(f"PirWebTratte - len(list_) - {len(list_)}")
        logging.info(f"PirWebTratte - list_ - {list_}")

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
