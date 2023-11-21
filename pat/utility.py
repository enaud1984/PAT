#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-


import numbers
import subprocess
import sys
from datetime import datetime
from functools import partial, wraps
import logging
import logging.handlers
try:
    import memory_profiler
    flag_memory_profiler=True
except:
    flag_memory_profiler=False
    logging.debug("No memory_profiler Library")

try:
    from dateutil.parser import parse as dateparser
except ImportError:
    logging.debug("No dateutil Library")


import json
import re
import os
import hashlib
from datetime import timedelta

# SPARK

from pat import env

try:
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import HiveContext, DataFrame, SparkSession, SQLContext
    flagSpark = True
except ImportError:
    logging.debug("No Spark Library")
    flagSpark = False

# KAFKA
try:
    from kafka import KafkaConsumer
    from kafka import KafkaClient
    from kafka import *
    flagKafka = True
except ImportError:
    logging.debug("No Kafka Library")
    flagKafka = False

# per usare la classe fare i seguenti import:
# from utility import Common
# from utility import Logger
# from utility import Spark
# from utility import Hdfs


"""
v 2.2
- aggiunta hdfs_merge_files
- modifica copyLogHdfs
- aggiunta classe logger
- aggiunto in common.initializeVar() parametro per sezione e gestione schema dataframe (StructType) #GV
- modifica executeMany nella classe CxOracle
- aggiunto timoeut request

v 2.3 Antonino
- aggiunta set_key_value() in DbCxOracle
- aggiunta get_key_value() in DbCxOracle
- aggiunta campo 'info' in class ErrorException
- modifica request in class Http -> raise ErrorException(..., info=response)
- aggiuna func warning() in Logger

v 2.3.1 Giuseppe
- aggiunta "encoding="UTF-8", nencoding="UTF-8"" in connect in DbCxOracle

v 2.3.2 Antonino
- aggiunta getNumExecution() in Common
- aggiunta roundDate() in Common
- aggiunta parametro "parameters" in executeProcedure() di DBoracle
- aggiunta union_date_intervals() in Common

v 2.3.3 Michele
- modifica funzione getDfFromQuery da Oracle

v 2.3.4 Giuseppe Iossa
- aggiunta della classe FieldControl
- aggiunta della classe Adl

v 2.3.5 Antonino
- aggiunta classe Compattatore

v 2.3.6 Michele
- aggiunta hdfsMv nella classe Hdfs

v 2.3.7 Antonino
- aggiunta classe Compattatore (nuova versione)

v 2.3.8 Giuseppe
- modifica logEndExecution (scrittura in nuova tabella log_hdfs)
"""
logger = logging.getLogger(__name__)


def encryptStr(value):
    result = hashlib.sha256(value.encode())
    return result.hexdigest()


class PatValidationError(Exception):
    pass


if flagSpark:
    class Spark:
        spark = None
        sparkContext = None
        hiveContext = None

        def __init__(self, APP_NAME, configDict):
            conf = SparkConf().setAppName(APP_NAME)
            spark_cfg = SparkSession.builder.appName(APP_NAME).config(conf=conf)

            if configDict:
                for key, value in configDict.items():
                    spark_cfg = spark_cfg.config(key, value)

            self.spark = spark_cfg.enableHiveSupport().getOrCreate()
            # self.spark = spark_cfg.getOrCreate()
            self.sparkContext = self.spark.sparkContext
            self.sparkContext.setLogLevel("ERROR")

        def get_spark(self):
            return self.spark

        def get_spark_context(self):
            return self.sparkContext

        def get_application_id(self):
            return self.sparkContext.applicationId

        def get_sql_context(self):
            return SQLContext(self.sparkContext)

        def get_hive_context(self):
            if self.hiveContext is None:
                return HiveContext(self.sparkContext)
            else:
                return self.hiveContext

        def close(self):
            self.sparkContext.stop()

        # def __del__(self):
        #    self.sparkContext.stop()

        def add_df_hash_key(self, df, hive_key_list):
            getattr_list = [getattr(df, x) for x in hive_key_list]
            df = df.withColumn(
                "key",
                sha2(
                    concat_ws(
                        "||",
                        *getattr_list
                        # getattr(df,*data)
                    ),
                    256)
            )
            return df

        def get_df_from_csv(self, data, header=False):
            # .option("mode", "DROPMALFORMED") \
            return self.spark.read \
                .format("csv") \
                .option("header", header) \
                .load(data)

        # Modifica Michele
        def get_df_from_query(self, query, oracle_param=None):
            '''
               oracle_param di esempio
               oracle_param={"url":"127.0.0.1:1521", "user":"utente_login", "password":"utente_password"}
               per oracle query puo' essere o la tabella o la query con l'alias finale esempio:
                     (select * from te_viaggi where ROWNUM <2) te_viaggi
                e senza campi geometrici nella select
                oracle.jdbc.driver.OracleDriver
                .option("driver", "oracle.jdbc.driver.OracleDriver") \'
                .option("driver", "org.postgresql.Driver") \'
            '''

            if oracle_param is not None:
                print(repr(oracle_param))
                return self.spark.read \
                    .format("jdbc") \
                    .option("url", oracle_param["url"]) \
                    .option("dbtable", query) \
                    .option("user", oracle_param["user"]) \
                    .option("password", oracle_param["password"]) \
                    .load()
            else:
                # return self.getHiveContext().sql( query )
                return self.get_spark_context().sql(query)

        def get_df_lower_column(self, df: DataFrame):
            # return  (df.withColumnRenamed(c, c.lower()) for c in df.columns)
            df_lower = df.toDF(*[c.lower() for c in df.columns])
            return df_lower

        def get_df_upper_column(self, df: DataFrame):
            # return  (df.withColumnRenamed(c, c.lower()) for c in df.columns)
            df_upper = df.toDF(*[c.upper() for c in df.columns])
            return df_upper

if flagKafka:
    class Kafka:
        pass
    """
    class Kafka():
        CONSUMER_INSTANCE_NOT_FOUND = 40403

        def __init__(self, topicName, consumerName, consumerInstance, username, password, headers=None):
            if headers is None:
                headers = {
                    "Accept": "application/vnd.kafka.json.v2+json','Content-Type': 'application/vnd.kafka.json.v2+json"}
            self.consumerName = consumerName
            self.consumerInstance = consumerInstance
            self.topicName = topicName
            self.headers = headers
            self.username = username
            self.password = password

        def createConsumer(self, url):
            #consumer_creation = "https://130.162.113.13:1080/restproxy/consumers/{}".format(self.consumerName)
            consumer_creation=url
            payload = {"name": self.consumerName, "format": "json", "auto.offset.reset": "earliest"}
            resp = requests.post(consumer_creation, data=json.dumps(payload), headers=self.headers, verify=False,
                                 auth=(self.username, self.password))
            resp_dict = json.loads(resp.text)

            if (resp.status_code == 200):
                regex = "/consumers/(.*?)/"
                self.consumerInstance = str(re.findall(regex, resp_dict["base_uri"])[0])
                self.consumerName = str(resp_dict["instance_id"])
            return self.consumerName, self.consumerInstance

        def subscribe(self):
            consumer_subscription = "https://130.162.113.13:1080/restproxy/consumers/{}/instances/{}/subscription".format(
                self.consumerInstance, self.consumerName)
            payload = {"topics": [self.topicName]}
            print("Kafka - subscribe - consumer_subscription == " + consumer_subscription)
            print("Kafka - subscribe - self.topicName == " + self.topicName)

            resp = requests.post(consumer_subscription, data=json.dumps(payload), headers=self.headers, verify=False,
                                 auth=(self.username, self.password))
            print("Kafka - subscribe - resp.status_code = " + str(resp.status_code))

            if (resp.status_code < 200 or resp.status_code > 299):
                print("Kafka - subscribe - subscribe error")

        def deleteConsumer(self):
            consumer_delete = "https://130.162.113.13:1080//restproxy/consumers/{}/instances/{}".format(
                self.consumerInstance, self.consumerName)
            payload = {"consumerInstanceId": self.consumerInstance}
            resp = requests.delete(consumer_delete, data=json.dumps(payload), headers=self.headers, verify=False,
                                   auth=(self.username, self.password))
            if (resp.status_code != 200):
                print("deleteConsumer Error")

        def readTopic(self):
            print("Kafka - readTopic - init")
            topic_url = "https://130.162.113.13:1080/restproxy/consumers/{}/instances/{}/records/".format(
                self.consumerInstance, self.consumerName)
            print("Kafka - readTopic - topic_url = " + topic_url)
            resp = requests.get(topic_url, headers=self.headers, verify=False, auth=(self.username, self.password))

            print("Kafka - readTopic - resp.status_code = " + str(resp.status_code))
            if (resp.status_code == 200):
                return json.loads(resp.text)
            elif (resp.status_code == 404):
                print("Kafka - readTopic - Consumer instance not found.")
                raise Exception(self.CONSUMER_INSTANCE_NOT_FOUND)
                # raise Exception("CONSUMER_INSTANCE_NOT_FOUND")
                # Common.raiseException(None, None, self.CONSUMER_INSTANCE_NOT_FOUND)
            elif (resp.status_code == 406):
                print(
                    "Kafka - readTopic - Consumer format does not match the embedded format requested by the Accept header.")
                raise Exception("Consumer format does not match the embedded format requested by the Accept header.")
            elif (resp.status_code == 500):
                print("Kafka - readTopic - Error 500")
                data_error = json.loads(resp.text)
                print("Kafka - readTopic - Consumer problem in read Topics." + data_error['error_code'])
                self.deleteConsumer()
                raise Exception("Consumer problem in read Topics.")
    """

class Json:
    @staticmethod
    def _local_expand(leaf_dict: dict):
        """
        Resolve dict template by rules
        """
        if "json_template" in leaf_dict:
            template_string = leaf_dict.get("json_template")
            if "delta_date" in leaf_dict:
                delta_date = datetime.utcnow() + timedelta(**leaf_dict.get("delta_date"))
                template_string = datetime.strftime(delta_date, template_string)
            return template_string
        return None

    @staticmethod
    def expand_templates(dictionary: dict):
        """
        Resolve dict keys template to values
        """
        for k, v in dictionary.items():
            if isinstance(v, dict):
                dictionary[k] = Json._local_expand(v) or Json.expand_templates(v)
            elif isinstance(v, list):
                for index, item in enumerate(v):
                    if isinstance(item, dict):
                        dictionary[k][index] = Json._local_expand(item) or Json.expand_templates(item)
        return dictionary

    @staticmethod
    def loads(*json_array):
        json_union = None
        for json_in in json_array:
            json_tmp = json_in

            # cerco tutti i text contenuti in ${text}
            to_replace = re.findall(r"\${(.*?)\}", json_tmp)

            json_tmp_dict = json.loads(json_tmp)
            if json_union is not None:
                json_tmp_dict.update(json_union)

            for r in to_replace:
                nested_val = Json.get_value(json_tmp_dict, *r.split("."))
                if nested_val is None:
                    raise Exception(r + " not found in json")

                nested_val_str = nested_val
                if type(nested_val) == list:
                    nested_val = json.dumps(nested_val)
                    json_tmp = json_tmp.replace("\"${" + r + "}\"", str(nested_val))  # elimino gli apici
                else:
                    if type(nested_val) != str:  # aggiungo gli apici per la stringa
                        json_tmp = json_tmp.replace("\"${" + r + "}\"", str(nested_val))  # elimino gli apici
                    json_tmp = json_tmp.replace("${" + r + "}", str(nested_val))

                json_tmp_dict = json.loads(json_tmp)
            if json_union is None:
                json_union = json.loads(json_tmp)
            else:
                json_union.update(json.loads(json_tmp))
        print(f"json_union == {json_union}")
        return json_union

    @staticmethod
    def loadsWithTemplate(json_file):
        """
        Metodo Sostituzione Variabili con jinja2
        Ritorna un json partendo da un file json con variabili del tipo {{ foo.bar }} dichiarati nella sezione env dello stesso file.
        Si usa il punto per navigare al'0interno delle chiavi
        """
        try:
            import jinja2 as j2
            env = j2.Environment(
                loader=j2.FileSystemLoader(searchpath="./")
            )
            template = env.get_template(json_file)
            json_dict = json.loads(json_file)
            vars = Json.get_value(json_dict, "env")
            return json.loads(
                j2.Template(template.render(vars)).render(vars)
            )
        except ImportError:
            logger.error("No jinja2 lib installed")
            return json_file
        except Exception as e:
            logger.error("Error Template",e)
            return json_file

    # metodo per il controllo se la stringa  un json valido (is_json)
    @staticmethod
    def is_json(my_json):
        try:
            json_object = json.dumps(my_json)
        except ValueError as e:
            return False
        return True

    # ritorna None se key non esiste, altrimenti restituisce il valore
    @staticmethod
    def get_value(data, *args, defval=None):
        env_key = "_".join(args)
         #env_val = Common.get_env(env_key)
        env_val = None
        #Todo gestire env

        if env_val is not None:
            return env_val

        try:
            val = data
            for x in args:
                val = val[x]
            return val
        except:
            return defval

    @staticmethod
    def get_value_eval(data, *args, defval=None):
        try:
            val = data
            for x in args:
                val = val[x]
            return eval(val)
        except:
            return defval

    # ritorna true se la chiave è presente almeno una volta nel json (per json innestati)
    @staticmethod
    def check_key(json: dict, key: str):
        if key in json.keys():
            return True

        for elem in json.values():
            if isinstance(elem, dict):
                if Json.check_key(elem, key):
                    return True
            elif isinstance(elem, list) or isinstance(elem, tuple):
                for list_item in elem:
                    if isinstance(list_item, dict):
                        if Json.check_key(list_item, key):
                            return True
        return False

    @staticmethod
    def merge(dict1, dict2):
        """ merge ricorsivo di due dict, sovrascrive i valori in 'conflitto' """
        if not isinstance(dict1, dict) or not isinstance(dict2, dict):
            return dict2

        for k in dict2:
            if k in dict1:
                dict1[k] = Json.merge(dict1[k], dict2[k])
            else:
                dict1[k] = dict2[k]
        return dict1


class Common:

    # ritorna None se key non esiste, altrimenti restituisce il valore
    @staticmethod
    def get_env(env_key):
        env_val = None
        if env_key is not None :
            if os.environ.get(env_key) is not None:
                env_val = os.environ.get(env_key)
                logging.info(f"Common get_env : {env_key} = {env_val}")
            elif os.environ.get(env_key.upper()) is not None:
                env_val = os.environ.get(env_key.upper())
                logging.info(f"Common get_env : {env_key.upper()} = {env_val}")
            elif os.environ.get(env_key.lower()) is not None:
                env_val =os.environ.get(env_key.lower())
                logging.info(f"Common get_env : {env_key.lower()} = {env_val}")

        return env_val

    @staticmethod
    def get_date(text):
        valid_date_fmt = [
            '%d/%m/%Y %H:%M:%S',
            '%Y/%m/%d %H:%M:%S'
        ]

        if text is not None:
            for fmt in valid_date_fmt:
                try:
                    return datetime.strptime(text, fmt)
                except ValueError:
                    pass
            raise ValueError('no valid date format found')
        return datetime.now()

    @staticmethod
    def validateFile(data, schema=None, type=None):
        if not schema and not type:
            return
        if schema and not type:
            return Exception("Type non specificato")
        from jsonschema import validate
        import xmlschema
        import xml.etree.ElementTree as ET
        try:
            if type.startswith("json"):
                data = json.loads(data.decode("UTF-8"))
                if schema:
                    if type == "json_file":
                        with open(schema) as json_schema:
                            validate_schema = json.load(json_schema)
                    else:
                        validate_schema = json.loads(schema)
                    import ast
                    # dict_str = data.decode("UTF-8")  # binary to str
                    # mydata = ast.literal_eval(dict_str)
                    validate(instance=data, schema=validate_schema)
            if type.startswith("xml"):  # solo fileValidate
                data_str = data.decode("utf-8")
                tree = ET.fromstring(data_str)
                if schema:
                    xsd = xmlschema.XMLSchema(schema)
                    xsd.validate(data_str)
        except Exception as e:
            raise e

    @staticmethod
    def round_date(date, minutes_to_round=10):
        # Funzione per l'arrotondamento dei minuti di una data
        if int(minutes_to_round) == 10:
            roud_min = date.minute - date.minute % 10
            return date.replace(minute=roud_min, second=0, microsecond=0)
        elif int(minutes_to_round) == 5:
            roud_min = date.minute - date.minute % 5
            return date.replace(minute=roud_min, second=0, microsecond=0)
        return None

    # Se obj non e' di tipo list ritorna un array di 1 elemento che include obj
    @staticmethod
    def to_list(obj, iterate=False):
        if obj is None:
            return []

        ret_list = obj
        if not isinstance(ret_list, list):
            ret_list = [ret_list]
        elif iterate:
            ret_list_2 = []
            for item in ret_list:
                if isinstance(item, list):
                    ret_list_2.extend(item)
                else:
                    ret_list_2.append(item)
            return ret_list_2
        return ret_list


    '''
    Sotsituisce in una stringa tutte le occorrenze di 
    %Y, %m, %d, %H, %M, %S, %f con i rispettivi valori contenuti in timestamp
    se timestamp e' null viene creato come datetime.today()
    '''
    @staticmethod
    def replace_time(value, timestamp=None):
        if timestamp is None:
            timestamp = datetime.today()
        return value \
            .replace('%Y', str(timestamp.year)) \
            .replace('%m', "{:02d}".format(timestamp.month)) \
            .replace('%d', "{:02d}".format(timestamp.day)) \
            .replace('%H', "{:02d}".format(timestamp.hour)) \
            .replace('%M', "{:02d}".format(timestamp.minute)) \
            .replace('%S', "{:02d}".format(timestamp.second)) \
            .replace('%f', str(timestamp.microsecond))

    @staticmethod
    def get_protocol(url):
        return url.split("//")[0][:-1]

    '''
    restituisce il nome del file contenuto nell'url
    input : url="webhdfs://10.206.227.251:9870/ingestion/json_example/pic_treni_out_tmp/2022/02/10/file_name.xxx"
    example_1 = level=None, folder=None # return file_name.xxx
    example_2 = level=3, folder=None # return 2022/02/10/file_name.xxx
    example_3 = level=None, folder=json_example # return pic_treni_out_tmp/2022/02/10/file_name.xxx
    '''

    @staticmethod
    def get_filename_from_url(url, level=None, folder=None):
        path = os.path.split(url)[0]
        filename = os.path.split(url)[1]
        parent = os.path.split(path)[1]
        if level is None and folder is None:
            return filename

        if level == 0 or folder == parent:
            return filename

        if level is None:
            level_ = None
        else:
            level_ = level - 1

        return Common.get_filename_from_url(path, level=level_, folder=folder) + "/" + filename


#### DECORATORS ####
if flag_memory_profiler:
    class TimingAndProfile(object):
        def __init__(self, enable_profile=False):
            self.enable_profile = enable_profile

        def __call__(self, func=None, *args, **kwargs):
            if func is None:
                return partial(*args, **kwargs)

            @wraps(func)
            def wrapper(*args, **kwargs):
                m1 = memory_profiler.memory_usage()
                t1 = datetime.now()

                if self.enable_profile:
                    to_return = memory_profiler.profile(func)(*args, **kwargs)
                else:
                    to_return = func(*args, **kwargs)

                t2 = datetime.now()
                m2 = memory_profiler.memory_usage()

                logger.debug(
                    "TimingAndProfile - [{}] - Execution Time: {} seconds".format(func.__name__, (t2 - t1).total_seconds()))
                logger.debug("TimingAndProfile - [{}] -Memory Usage: {} MiB".format(func.__name__, m2[0] - m1[0]))

                return to_return

            return wrapper


##############################################################################################
############################     GESTIONE ECCEZIONI INIT     #################################
#############################################################################################

class FieldControl:
    def checkRequired(self, value, field_name):
        if value is None:
            error_message = "The field {} cannot be null\n".format(field_name)
            raise Exception(error_message)

    def checkDate(self, value, field_name, fuzzy=False, required=False, date_format="yyyy-MM-dd HH24:mi:ss"):
        try:
            if (required):
                self.checkRequired(value, field_name)
            return None if (value is None) else dateparser(value, fuzzy=fuzzy)
        except ValueError as e:
            error_message = "The field {} is not a date string".format(field_name)
            raise Exception(error_message)
        except Exception as err:
            raise err

    def checkString(self, value, field_name, fuzzy=False, required=False):
        try:
            if (required):
                self.checkRequired(value, field_name)

            if value is None:
                return None
            elif isinstance(value, str):
                return value
            else:
                error_message = "Required type for field {} is String".format(field_name)
                raise Exception(error_message)
        except Exception as err:
            raise err

    def checkNumber(self, value, field_name, fuzzy=False, required=False):
        try:
            if (required):
                self.checkRequired(value, field_name)

            if value is None:
                return None
            elif (isinstance(value, float) or isinstance(value, int) or isinstance(value, complex)):
                return value
            else:
                error_message = "Required type for field {} is a number of any type".format(field_name)
                raise Exception(error_message)
        except Exception as err:
            raise err

    def checkValuesInList(self, value, field_name, valueList, required=False):
        try:
            if (required):
                self.checkRequired(value, field_name)

            if value is None:
                return None
            elif value in valueList:
                return value
            else:
                error_message = "Field {} can only contain a value between {}".format(field_name, valueList)
                raise Exception(error_message)
        except Exception as err:
            raise err

    '''
   def checkValuesInList(self, value, field_name, valueList, required = False):
       try:
           if (required and value is None):
               self.checkRequired(value, field_name)
           elif (required and value is not None):
               if (value in valueList):
                   return value
               else:
                   error_message = "Field {} can only contain a value between [{}]".format(field_name, valueList)
                   raise ErrorException(exception=None, code=ErrorException.GENERIC_ERROR, info=error_message)
           elif (value is None):
               return None
           elif (value in valueList):
               return value
           else:
               error_message = "Field {} can only contain a value between [{}]".format(field_name, valueList)
               raise ErrorException(exception = None, code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err

   def checkString(self, value, field_name, required = False):
       try:
           if (required and value is None):
               self.checkRequired(value, field_name)
           elif (required and value is not None):
               if isinstance(value, str):
                   return value
               else:
                   error_message = "Required type for field {} is String".format(field_name)
                   raise ErrorException(exception=None, code=ErrorException.GENERIC_ERROR, info=error_message)
           elif (value is None):
               return None
           if isinstance(value, str):
               return value
           else:
               error_message = "Required type for field {} is String".format(field_name)
               raise ErrorException(exception = None, code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err

   def checkNumber(self, value, field_name, required = False):
       try:
           if (required and value is None):
               self.checkRequired(value, field_name)
           elif(required and value is not None):
               if (isinstance(value, float) or isinstance(value, int) or isinstance(value, complex)):
                   return value
               else:
                   error_message = "Required type for field {} is a number of any type".format(field_name)
                   raise ErrorException(exception=None, code=ErrorException.GENERIC_ERROR, info=error_message)
           elif (value is None):
               return None
           elif (isinstance(value, float) or isinstance(value, int) or isinstance(value, complex)):
               return value
           else:
               error_message = "Required type for field {} is a number of any type".format(field_name)
               raise ErrorException(exception = None, code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err
   '''


##############################################################################################
############################         AGGIUNTO NUOVO          #################################
##############################################################################################


'''
mydict = {'user': 'Bot', 'version': 0.15, 'items': 43, 'methods': 'standard', 'time': 1536304833437, 'logs': 'no', 'status': 'completed'}
columns = ', '.join("`" + str(x).replace('/', '_') + "`" for x in mydict.keys())
values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in mydict.values())
sql = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('mytable', columns, values)
print(sql)

https://webpy.org/cookbook/where_dict
'''


class SqlBuilder:
    def to_timestamp(self, dt):
        dt_ = datetime.strftime(dt, format="%Y-%m-%d %H:%M:%S")
        return f"to_timestamp('{dt_}', 'yyyy-MM-dd HH24:mi:ss')"

    '''
    def to_insert_(self, table, columns, tuples):
        fields = ','.join(map(str, columns))
        question = ','.join(['?'] * len(columns))
        query = f"insert into {table} ({fields}) values ({question})"
        queries = []

        for t in tuples:
            for v in t:
                v = self.to_value(v)
        return queries, tuples
    '''

    def get_insert_string(self, table, columns):
        fields = ','.join(map(str, columns))
        question = ','.join(['?'] * len(columns))
        query = f"insert into {table} ({fields}) values ({question})"
        return query

    def to_insert(self, table, columns, tuples=None, one_statement=False):
        if one_statement:
            if tuples is not None:
                fields = ','.join(map(str, columns))
                rows = []
                for t in tuples :
                    row = "(" + ','.join(self.to_value(v) for v in t) + ")"
                    rows.append(row)

                values = ','.join(rows)
                query = f"insert into {table} ({fields}) values {values}"
        else:
            query = self.get_insert_string(table, columns)
            if tuples is not None:
                queries = []
                for t in tuples:
                    q = query
                    for v in t:
                        q = q.replace("?", self.to_value(v), 1)
                    print(q)
                    queries.append(q)
                return queries
        return query

    def to_update(self, table, columns, tuples, where=None, ignore_null=False):
        queries = []
        '''
        for t in tuples:
            update_set = []
            update_where = ["1 = 1"]

            for index in range(len(t)):
                if columns[index] in where:
                    update_where.append(self.to_value2(columns[index], t[index]))
                elif t[index] is not None or ignore_null:
                    update_set.append(self.to_value2(columns[index], t[index]))
        '''

        '''
            for index in range(len(t)):
                if t[index] is not None or ignore_null: #<---?
                    update_set.append(self.to_value2(columns[index], t[index]))
            if where:
                for (key, value) in where.items():
                    if value is not None or ignore_null:
                        update_where.append(self.to_value2(key, value))

            upd = ",".join(update_set)
            whr = " and ".join(update_where)
            query = f"update {table} set {upd} where {whr}"
            print(query)
            queries.append(query)
        '''

        update_where = ["1=1"]
        upd = ",".join([f" {c} = ?" for c in columns])
        if where:
            for (key, value) in where.items():
                if value is not None or ignore_null:
                    update_where.append(self.to_value2(key, value))
        whr = " and ".join(update_where)
        queries = f"update {table} set {upd} where {whr}"
        print(queries)

        return queries

    def to_select(self, table, columns, where=None, ignore_null=False):
        fields = ','.join(map(str, columns))
        select_where = ["1=1"]
        if where:
            for (key, value) in where.items():
                if value is not None or ignore_null:
                    select_where.append(self.to_value2(key, value))
        whr = " and ".join(select_where)
        query = f"select {fields} from {table} where {whr}"
        print(query)
        return query

    def to_value2(self, k, v, is_where=False):
        if v is None:
            if is_where:
                return f"{k}=null"
            else:
                return f"{k} is null"
        if isinstance(v, numbers.Number):
            return f"{k}={v}"
        if isinstance(v, datetime):
            return f"{k}={self.to_timestamp(v)}"
        v = v.replace("'", "\\'")
        return f"{k}='{v}'"

    def to_value(self, v):
        if v is None:
            return "null"
        if isinstance(v, numbers.Number):
            return str(v)
        if isinstance(v, datetime):
            return f"{self.to_timestamp(v)}"
        v = v.replace("'", "\\'")
        return f"'{v}'"

    def to_value_(self, v):
        if isinstance(v, datetime):
            return f"{self.to_timestamp(v)}"
        return v


class ImpalaSqlBuilder(SqlBuilder):
    def to_timestamp(self, dt):
        dt_ = datetime.strftime(dt, format="%d/%m/%Y %H:%M:%S")
        return f"to_timestamp('{dt_}', 'dd/MM/yyyy HH:mm:ss')"


class PostgresSqlBuilder(SqlBuilder):
    def to_timestamp(self, dt):
        dt_ = datetime.strftime(dt, format="%Y-%m-%d %H:%M:%S")
        return f"to_timestamp('{dt_}', 'yyyy-MM-dd HH24:mi:ss')"


class HdfsSubprocess:

    # (hdfs_put)
    def hdfsPut(self, fpath: object, local_path: object, append: object = True) -> object:
        proc = subprocess.Popen( ["hdfs", "dfs", "-put", "-f", local_path, fpath], stdin=subprocess.PIPE )
        print( "retcode =", proc.returncode )
        proc.communicate()

##############################################################################################
############################     GESTIONE ECCEZIONI END      #################################
##############################################################################################

# gestione eccezione non gestita
old_exception = sys.excepthook


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    hdfs_date_format = datetime.today().strftime('%Y%m%d')

    # local_log_filename = "/tmp/handle_exception.log"

    # local_log_filename = "c:/temp/handle_exception.log"

    local_log_filename = env.get_log_handle_exception()

    formatter = logging.Formatter('%(asctime)s\t- %(name)s\t- %(levelname)s\t- (%(threadName)-10s)\t- %(message)s',
                                  "%Y-%m-%d %H:%M:%S")
    mylogger_handler = logging.FileHandler(local_log_filename)
    mylogger_handler.setLevel(logging.INFO)
    mylogger_handler.setFormatter(formatter)

    mylogger = logging.getLogger("ERROR")
    mylogger.addHandler(mylogger_handler)

    ENABLE_PRINT = os.environ.get('ENABLE_PRINT')
    if ENABLE_PRINT is not None:
        print('ENABLE_PRINT = ' + ENABLE_PRINT)

    mylogger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    mylogger.removeHandler(mylogger_handler)
    mylogger_handler.flush()
    mylogger_handler.close()
    old_exception(exc_type, exc_value, exc_traceback)


sys.excepthook = handle_exception
