import json
import logging
import os
from pat.class_tool import Storable
from pat.converter import JsonConverter
from pat.chain_of_responsability import DataProcess, ManagerChain, mapped_values
from pat.utility import Common

# DataOraPrevistoArrivo as arrivo_dttm::timestamp(%Y-%m-%dT%H:%M:%SZ)

schema = {
        "NVR": None,
        "VKM": None,
        "EVN": None,
        "EMSID": None,
        "TRP": None,
        "XMLVersion": None,
        "MeasurementIntervalStart": None,
        "MeasurementIntervalEnd": None,
        "CEBDID": None,
        "CEBDTimeStamp": None,
        "CEBDTimeStampQuality": None,
        "CEBDChannelID": None,
        "CEBDTractionSystem": None,
        "CEBDEnergyActiveConsumed": None,
        "CEBDEnergyActiveRegenerated": None,
        "CEBDEnergyReactiveConsumed": None,
        "CEBDEnergyReactiveRegenerated": None,
        "CEBDEnergyQuality": None,
        "CEBDLocationLatitude": None,
        "CEBDLocationLongitude": None,
        "CEBDLocationQuality": None,
        "DigestValue": None,
        "SignatureValue": None,
        "Modulus": None,
        "Exponent": None,
        "recordTimestamp": None,
        "model": None,
        "description": None,
        "companyName": None,
        "active": None,
        "companyID": None,
        "activationDate": None,
        "vehiclePlate": None,
        "fleet": None,
        "category": None
    }

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
                    list_.extend(converter.to_list(d2, header=False))
                    i += 1
            except Exception as e:
                print("ERROR", e)
                print("***FILE JSON***", source_current)
                raise e
        #spark = mapped_values.get(self.id_transaction).get("spark")

        #df = converter.toDataFrameFromList(list_, spark.spark)

        list_.insert(0, converter.get_header())
        logging.info(f"IstatMerciPericolose - len(list_) - {len(list_)}")
        logging.info(f"IstatMerciPericolose - list_ - {list_}")

        storable = Storable()
        storable.data = list_
        storable.id_storable = "hive_jdbc"

        '''
        df = converter.toDataFrameFromList(list_)
        storable1 = Storable()
        storable1.data(df)
        storable1.id_storable("1")
        '''

        return [storable]


# os.environ["CLASSPATH"] = "ojdbc8.jar"

# os.environ["CLASSPATH"] = "../../lib/ImpalaJDBC41.jar"
#os.environ["CLASSPATH"] = "../../lib/postgresql-42.2.14.jar"
os.environ["CLASSPATH"] = "" \
    "C:/Almaviva/workspace_common/pattern/pattern/lib/postgresql-42.2.14.jar" \
    ";C:/temp/jar/*"\


os.environ["HADOOP_CONF_DIR"]="C:/Hadoop/conf/sem"


DataProcess.process = ScriptProcess.process
# CONF_JSON = os.path.dirname(os.path.abspath(__file__)) + "/conf.json"
flow = ManagerChain()
flow.init_default_logger()
flow.execute()
