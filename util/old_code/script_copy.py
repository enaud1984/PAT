import json
import logging

from common.class_tool import Storable
from common.converter import JsonConverter
from common.chain_of_responsability import DataProcess, ManagerChain
from common.utility import Common

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
        common = Common()
        source_list = common.toList(req)

        for source in source_list:
            logging.info("{source}")
            data_list = common.toList(source.data())
            for data in data_list:
                s1 = json.dumps(data.decode("utf-8"))
                d2 = json.loads(s1)
                list_.extend(converter.toList(d2, header=False))

        list_.insert(0,converter.getHeader())

        logging.info(f"PicTreni - len(list_) - {len(list_)}")
        logging.info(f"PicTreni - list_ - {list_}")

        storable = Storable()
        storable.data(list_)
        storable.id_storable("1")

        storable_list = [storable]
        return storable_list

DataProcess.process=ScriptProcess.process
flow = ManagerChain()
flow.execute()
