import json
import logging
import os

from pat.class_tool import Storable
from pat.converter import JsonConverter
from pat.chain_of_responsability import DataProcess, ManagerChain
from pat.utility import Common

# DataOraPrevistoArrivo as arrivo_dttm::timestamp(%Y-%m-%dT%H:%M:%SZ)

schema = [
    {
        "IDTreno#id_treno": "",
        "IDTratta#id_tratta": "",
        "DataOraPrevistoArrivo#arrivo_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None,
        "DataOraPrevistoPartenza#partenza_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None,
        "Origine#origine": "",
        "Destinazione#destinazione": "",
        "DataOraEffettivaArrivo#arrivo_eff_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None,
        "DataOraEffettivaPartenza#partenza_eff_dttm(timestamp|%Y-%m-%dT%H:%M:%SZ)": None,
        "TipologiaTrazione#tipologia_trazione": "",
        "TrenoKM#treno_km(double)": None,
        "DistanzaPercorsa#distanza_percorsa(double)": None,
        "NodoRiferimento#nodo_riferimento": "",
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
            logging.info("%s", source.data())
            sourseAll = source.filename()
            data_list = Common.to_list(source.data())
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
                print(e)
                self.db_log.log_file_update(run_id=self.id_transaction, filename=source_current, exit_code=-1)
                raise Exception(f"Errore durante processamento json {source_current}")

        spark = MemoryMap.hget(self.id_transaction, "spark")

        # list_.insert(0,converter.getHeader())
        df = converter.toDataFrameFromList(list_, spark.spark)

        logging.info("PicTreni - len(list_) - %s", len(list_))
        logging.info("PicTreni - list_ - %s", list_)

        storable = Storable()
        storable.data(df)
        storable.id_storable("impala")

        '''
        df = converter.toDataFrameFromList(list_)
        storable1 = Storable()
        storable1.data(df)
        storable1.id_storable("1")
        '''

        return [storable]


# os.environ["CLASSPATH"] = "ojdbc8.jar"

# os.environ["CLASSPATH"] = "../../lib/ImpalaJDBC41.jar"
os.environ["CLASSPATH"] = "../../lib/postgresql-42.2.14.jar"

DataProcess.process = ScriptProcess.process
CONF_JSON = os.path.dirname(os.path.abspath(__file__)) + "/conf.json"
flow = ManagerChain(CONF_JSON)
flow.execute(stream=True)
