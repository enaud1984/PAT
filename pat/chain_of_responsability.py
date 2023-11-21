import logging
import logging.handlers
import os
import sys
import traceback
from pat.class_tool import *
from pat.connector import Connector
from pat.utility import Common, Spark, Json, PatValidationError, HdfsSubprocess
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from pat import env
import collections

if "jaydebeapi" == env.db_log_library:
    from pat.db_log.jaydebeapi import DatabaseLog
elif "sql_alchemy" == env.db_log_library:
    from pat.db_log.sql_alchemy import DatabaseLog

logger = logging.getLogger("PAT")
logger.setLevel(logging.INFO)
logger.propagate = False
ch = logging.StreamHandler()
#ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
#logger.addHandler(ch)
mapped_values = collections.defaultdict(dict)


class PatException(Exception):
    def __init__(self, exception, module):
        super().__init__(str(exception))
        self.exc_type = type(exception).__name__
        self.module = module
        self.exception = exception


class BaseRing(ABC):
    """
    Interfaccia Base dei vari anelli.
    Si deve reimplementare il metodo run
    """

    def __init__(self, option, id_transaction):
        """
        Costruttore BaseRing
        Args:
            option: contiene le varie opzioni per costruire l'anello
            id_transaction: id della transazione
        """
        self.date_start = None
        self.date_end = None
        self.option = option
        self.id_transaction = id_transaction

    @abstractmethod
    def run(self, req):
        """
        Implementazione dell'anello

        Args:
            req: input dell'anello
        Return:
            Any: output anello
        """
        pass


class DataAcquisitionRead(BaseRing):
    """
        Anello per lettura input dati da sorgente
    """

    def __init__(self, option, id_transaction):
        super().__init__(option, id_transaction)
        self.validate_schema = None

    def validate_config(self):
        """
        Validazione config json relativo

        .. describe:: Parameteri pre-esistenti obbligatori:
        :param option[id_source]: identificativo sorgente
        :param option[url]: indirizzo sorgente dati
        :param option[param]: parametri di connessione
        :param id_transaction: id della transazione

        :return: True se json corretto altrimenti False, Descrizione Errore
        :rtype: Bool, String
        """

        value = ["id_source", "url", "param"]
        for c in self.option:
            if not set(value) == set(c.keys()):
                return False, "not id_source or url or param in option json"
            if not self.option["url"].startswith(Connector.connector_map):
                return False, "url protocol protocol don't handled"
        return True, None

    def run(self, req):
        """
           :param req:
           :type req: None
           :return: Lista di Oggetti Source
           :rtype: list(Source)
        """
        try:
            logger.info("DataAcquisitionRead start")
            self.date_start = datetime.utcnow()
            self.db_log = mapped_values.get(self.id_transaction).get("db_log")
            log_operation = DatabaseLog.LogOperation(run_id=self.id_transaction,
                                                     start_time=self.date_start,
                                                     operation="DataAcquisitionRead")
            self.db_log.insert_log(log_operation)
            continue_if_except = mapped_values.get(self.id_transaction).get("continue_if_except")
            connector = Connector()
            source_list = []
            option_list = Common.to_list(self.option)
            spark = mapped_values.get(self.id_transaction).get("spark")
            spark_ = None
            if spark is not None:
                spark_ = spark.spark
            for opt in option_list:
                id_source = Json.get_value(opt, "id_source")
                connector_read = connector.read(
                    Json.get_value(opt, "url"),
                    Json.get_value(opt, "param"),
                    spark=spark_
                )

                connector_read_list = Common.to_list(connector_read)
                for cr in connector_read_list:
                    src = Source()
                    src.id_source = id_source
                    src.src_name = cr.src_name
                    src.data = cr.data
                    source_list.append(src)
                    filename_list = Common.to_list(src.src_name)
                    data_list = Common.to_list(src.data)
                    i = 0
                    for fn in filename_list:
                        exit_code = 0
                        try:
                            Common.validateFile(data_list[i], schema=Json.get_value(opt, "validate", "schema"),
                                                type=Json.get_value(opt, "validate", "type"))
                        except Exception as e:
                            exit_code = -1
                            if continue_if_except:
                                # eliminazione della lista di source validi
                                source_list.remove(source_list[i])
                            else:
                                raise Exception(f"filename:{fn} is not a valid file. {e}")
                        finally:
                            i += 1
                            log_file = DatabaseLog.LogFile(run_id=self.id_transaction,
                                                           src_name=fn,
                                                           exit_code=exit_code
                                                           )
                            self.db_log.insert_log(log_file)
            self.date_end = datetime.utcnow()
            log_operation.end_time = self.date_end
            self.db_log.update_session(log_operation)
            logger.info("DataAcquisitionRead end")
            return source_list
        except Exception as e:
            logger.exception("DataAcquisitionRead exception")
            raise PatException(e, self.__class__.__name__)


class DataAcquisitionWrite(BaseRing):
    """
        Anello per scrittura input dati su hdfs, bypassabile cancellando la chiave:
        ["ingestion"]["DataAcquisitionWrite"] da conf.json
    """

    def validate_config(self):
        return True

    def run(self, req):
        """
           :param req: Lista di Oggetti Source
           :type req: list(Source)
           :return: Lista di Oggetti Source
           :rtype: list(Source)
        """

        try:
            logger.info("DataAcquisitionWrite start")
            self.date_start = datetime.utcnow()
            self.db_log = mapped_values.get(self.id_transaction).get("db_log")
            log_operation = DatabaseLog.LogOperation(run_id=self.id_transaction,
                                                     start_time=self.date_start,
                                                     operation="DataAcquisitionWrite")
            self.db_log.insert_log(log_operation)
            connector = Connector()
            option_list = Common.to_list(self.option)
            source_list = req
            for opt in option_list:
                url = Json.get_value(opt, "url")
                id_source = Json.get_value(opt, "id_source")
                for source in source_list:
                    if id_source == source.id_source:
                        data_list = Common.to_list(source.data)
                        filename_list = Common.to_list(source.src_name)

                        for index in range(len(data_list)):
                            data = data_list[index]

                            source_delete = Json.get_value(opt, "param", "source_delete")

                            source_name = filename_list[index]
                            filename = Common.get_filename_from_url(
                                source_name,
                                level=Json.get_value(opt, "param", "source_level"),
                                folder=Json.get_value(opt, "param", "source_folder")
                            )

                            destination_name = url + "/" + filename
                            logger.debug("DataAcquisitionWrite source_name : %s", source_name)
                            logger.debug("DataAcquisitionWrite filename : %s", filename)
                            logger.info("DataAcquisitionWrite write : %s", destination_name)
                            connector.write(
                                destination_name,
                                Json.get_value(opt, "param"),
                                data
                            )

                            logger.info("DataAcquisitionWrite source_delete : %s", source_delete)
                            if source_delete == "Y":
                                logger.info("DataAcquisitionWrite delete : %s", source_name)
                                connector.delete(
                                    source_name,
                                    Json.get_value(opt, "param")
                                )

                            '''
                            log_file = self.db_log.get_item(DatabaseLog.LogFile, self.id_transaction)
                            log_file.src_name = source_name
                            log_file.dst_name = destination_name
                            self.db_log.update_session(log_file)
                            '''

            self.date_end = datetime.utcnow()
            log_operation.end_time = self.date_end
            self.db_log.update_session(log_operation)
            logger.info("DataAcquisitionWrite end")
            return req
        except Exception as e:
            logger.exception("DataAcquisitionWrite exception")
            raise PatException(e, self.__class__.__name__)


class DataPreProcess(BaseRing):
    """
        Anello per preprocessamento input , bypassabile cancellando la chiave:
        ["ingestion"]["DataPreProcess"] da conf.json
    """

    def validate_config(self):
        return True

    def run(self, req):
        """
           :param req: Lista di Oggetti Source
           :type req: list(Source)
           :return: Lista di Oggetti Source
           :rtype: list(Source)
        """
        try:
            logger.info("DataPreProcess start")
            self.date_start = datetime.utcnow()
            self.db_log = mapped_values.get(self.id_transaction).get("db_log")
            log_operation = DatabaseLog.LogOperation(run_id=self.id_transaction,
                                                     start_time=self.date_start,
                                                     operation="DataPreProcess")
            self.db_log.insert_log(log_operation)

            res = self.pre_process(req)

            self.date_end = datetime.utcnow()
            log_operation.end_time = self.date_end
            self.db_log.update_session(log_operation)
            logger.info("DataPreProcess end")
            return res
        except Exception as e:
            logger.exception("DataPreProcess exception")
            raise PatException(e, self.__class__.__name__)

    def pre_process(self, req):
        return req


class DataProcess(BaseRing):
    """
       Anello per processamento input , bypassabile cancellando la chiave:
        [\\"ingestion\\"][\\"DataProcess\\"] da conf.json
        La funzione process(req) dovrà essere riscritta nel proprio script custom
        e riassegnarla come nell'esempio:

    .. highlight:: python
    .. code-block:: python

            class ScriptProcess:
                def process(self, req):
                    df= spark.createDataframe(req.data)
                    storable = Storable()
                    storable.data(df)
                    storable.id_storable("impala")
                    return [storable]
            DataProcess.process=ScriptProcess.process
    """

    def validate_config(self):
        return True

    def run(self, req):
        """
           :param req: Lista di Oggetti Source
           :type req: list(Source)
           :return: Lista di Oggetti Storable
           :rtype: list(Storable)
        """
        try:
            logger.info("DataProcess start")
            self.date_start = datetime.utcnow()
            self.db_log = mapped_values.get(self.id_transaction).get("db_log")
            log_operation = DatabaseLog.LogOperation(run_id=self.id_transaction,
                                                     start_time=self.date_start,
                                                     operation="DataProcess")
            self.db_log.insert_log(log_operation)

            storable_list = self.process(req)

            self.date_end = datetime.utcnow()
            log_operation.end_time = self.date_end
            self.db_log.update_session(log_operation)
            logger.info("DataProcess end")
            return storable_list
        except Exception as e:
            logger.exception("DataProcess exception")
            raise PatException(e, self.__class__.__name__)

    def process(self, req):
        return req


class DataStoring(BaseRing):
    """
    Anello per memorizzazione dei dati.
    """

    def validate_config(self):
        return True

    def run(self, req):
        """
           :param req: Lista di Oggetti Source
           :type req: list(Source)
        """
        try:
            logger.info("DataStoring start")
            self.date_start = datetime.utcnow()
            self.db_log = mapped_values.get(self.id_transaction).get("db_log")
            log_operation = DatabaseLog.LogOperation(run_id=self.id_transaction,
                                                     start_time=self.date_start,
                                                     operation="DataStoring")
            self.db_log.insert_log(log_operation)

            connector = Connector()
            option_list = Common.to_list(self.option)
            storable_list = Common.to_list(req)
            for opt in option_list:
                id_storable = Json.get_value(opt, "id_storable")
                for storable in storable_list:
                    if id_storable == storable.id_storable:
                        connector.write(
                            Json.get_value(opt, "url"),
                            Json.get_value(opt, "param"),
                            storable.data,
                            mapped_values.get(self.id_transaction).get("spark")
                        )
            self.date_end = datetime.utcnow()
            log_operation.end_time = self.date_end
            self.db_log.update_session(log_operation)
            logger.info("DataStoring end")

        except Exception as e:
            logger.exception("DataStoring exception")
            raise PatException(e, self.__class__.__name__)


class DataExecute(BaseRing):
    """
    Anello per memorizzazione dei dati.
    """

    def validate_config(self):
        return True

    def run(self, req):
        """
           :param req: Lista di Oggetti Source
           :type req: list(Source)
        """
        try:
            logger.info("DataExecute start")
            self.date_start = datetime.now()
            self.db_log = mapped_values.get(self.id_transaction).get("db_log")
            log_operation = DatabaseLog.LogOperation(run_id=self.id_transaction,
                                                             start_time=self.date_start,
                                                             operation=self.__class__.__name__
                                                             )
            self.db_log.insert_log(self.db_log_operation)

            connector = Connector()
            option_list = Common.to_list(self.option)
            for opt in option_list:
                connector.write(
                    Json.get_value(opt, "url"),
                    Json.get_value(opt, "param"),
                    None
                )

            self.date_end = datetime.utcnow()
            log_operation.end_time = self.date_end
            self.db_log.update_session(log_operation)

            logger.info("DataExecute end")
            return req

        except Exception as e:
            logger.error("DataExecute exception : %s", e)
            raise PatException(e, self.__class__.__name__)


class DataStream(BaseRing):
    """
    Inizializzazione e Partenza Streaming.
    """

    def validate_config(self):
        return True

    def run(self, req):
        """
           :param req: None per Streaming
        """
        try:
            logger.info("DataStream start")
            self.date_start = datetime.now()
            self.db_log = mapped_values.get(self.id_transaction).get("db_log")
            log_operation = DatabaseLog.LogOperation(run_id=self.id_transaction,
                                                             start_time=self.date_start,
                                                             operation=self.__class__.__name__
                                                             )
            self.db_log.insert_log(self.db_log_operation)

            connector = Connector()
            operation = ManagerChain.run
            connector.stream(
                Json.get_value(self.option, "url"),
                Json.get_value(self.option, "param"),
                spark=None,
                operation=operation
            )

            self.date_end = datetime.utcnow()
            log_operation.end_time = self.date_end
            self.db_log.update_session(log_operation)

            logger.info("DataExecute end")
        except Exception as e:
            logger.error("DataExecute exception : %s", e)
            raise PatException(e, self.__class__.__name__)


class BaseChain:
    """
    Classe BaseChain

    """

    def rings_list(self):
        """
        Funzione implementata nel :py:class:ManagerChain
        :return: lista di Classi BaseRing
        """
        return []

    def __init__(self, rings_list=None):
        """
          :param rings_list: Lista di Oggetti
          :type req: list(Object(BaseRing))
       """
        if not rings_list:
            self.rings = self.rings_list()

    def stream(self):
        option = Json.get_value(self.config, "DataStream")
        DataStream(option, self.id_transaction).run(None)

    def run(self, data=None):
        # chain = [c() for c in self.rings]
        """
        Esecuzione flusso (catena) mediante i run dei singoli anelli.
        :return: Risposta dell'ultimo anello.
        :raise: Se fallisce la funzione validate_config.
        """
        res = None
        req = data
        # Controllo config singolo anello
        try:
            self.validate_config()
        except Exception as e:
            raise e
        else:
            for c in self.rings:
                res = c.run(req)
                if res is None:
                    break
                req = res
            return res

    def validate_config(self):
        """
        Validazione config dei singoli anelli

        :raise: Se la singola validazione del config json dell'anello non è corretta
        """
        for c in self.rings:
            res, error = c.validate_config()
            if not res:
                raise PatValidationError(f"Chain {c.__name__} has config json not consistent. {error}")


class ManagerChain(BaseChain):
    """
    Classe gestore Chain
    """
    exit_code = 0
    start_time = datetime.utcnow().replace(microsecond=0)

    # rings deve essere automaticamente dal conf.json
    def __init__(self, conf_json=None):
        """
           :param conf_json: File json di configurazione
           :type req: string
        """
        if conf_json is None:
            file_json = "./conf.json"
        else:
            file_json = conf_json

        with open(file_json) as json_file:
            # json_file.read().replace("%Y","2000")
            # config = json.load(json_file, object_hook=lambda d: namedtuple('Config', d.keys())(*d.values()))
            file_str = json_file.read()
            file_str = file_str.replace('\n', '')
            # file_str = Common.replace_time(file_str)

        # config = json.loads(file_str)
        loaded_dict = Json.loads(file_str)
        config = Json.expand_templates(loaded_dict)
        self.config = config
        self.validate_config()
        self.id_transaction = f"{Json.get_value(self.config, 'app', 'name')}_{round(datetime.timestamp(datetime.now()) * 1000)}"
        self.exc_message = ""
        self.exc_type = "success"
        self.exc_module = ""
        super().__init__()

    def init_default_logger(self):
        logger_formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(name)s::%(module)s|%(message)s')
        logger_handle_rotation = logging.handlers.RotatingFileHandler(filename=env.get_log_path(self.id_transaction),
                                                                      maxBytes=10485760,
                                                                      mode="a",
                                                                      backupCount=5)
        logger_handle_rotation.setFormatter(logger_formatter)
        logging.getLogger().addHandler(logger_handle_rotation)
        logging.getLogger().setLevel(logging.INFO)

    def execute(self, stream=False):
        """
        Esecuzione Flusso Chain
        """
        try:
            logger.info("ManagerChain start")
            self.init_var()
            if stream:
                self.stream()
            else:
                self.run()
        except PatException as e:
            traceback.print_exc()
            logger.exception("ManagerChain exception")
            self.exc_module = e.module
            self.exc_type = e.exc_type
            self.exc_message = str(e)
        except Exception as e:
            traceback.print_exc()
            logger.exception("ManagerChain exception")
            self.exc_module = "Generic"
            self.exc_type = type(e).__name__
            self.exc_message = str(e)
        finally:
            try:
                # Update exception message
                setattr(self.log, 'message', self.exc_message)
                self.db_log.update_session(self.log)

                '''
                db_session = self.db_log.get_session()
                log_exit_code = db_session.query(DatabaseLog.LogExitCode).filter(DatabaseLog.LogExitCode.type == self.exc_type).filter(
                    DatabaseLog.LogExitCode.module == self.exc_module).first()
                '''

                exit_code = self.db_log.get_exit_code(
                    type=self.exc_type,
                    module=self.exc_module
                )

                if exit_code is None:
                    log_exit_code = DatabaseLog.LogExitCode(
                        type=self.exc_type,
                        module=self.exc_module
                    )
                    self.db_log.insert_log(log_exit_code)

                    exit_code = self.db_log.get_exit_code(
                        type=self.exc_type,
                        module=self.exc_module
                    )

                self.exit_code = exit_code

            except Exception as e2:
                traceback.print_exc()
                logger.exception("ManagerChain exception")
                self.exit_code = -1
            self.end()  # close spark, update log
            print("end")
        logger.info("ManagerChain end")

    def rings_list(self):
        """
        Determinazione delle Classi-Anello della chain in funzione della sezione ingestion nel config.json

        :return: Lista Classi-Anello
        """
        try:
            rings = []
            # rings=list(eval(f"{r}(config.{r})") for r in config._fields if r !="var" )
            for r in Json.get_value(self.config, "ingestion"):
                option_ = Json.get_value(self.config, "ingestion", r)
                try:
                    class_ = getattr(sys.modules[__name__], r)
                except AttributeError as e:
                    logger.info("Load custom ring : %s", e)
                    exec(f"from script import {r}")
                    class_ = getattr(sys.modules[__name__], r)

                c = class_(option_, self.id_transaction)
                rings.append(c)
            return rings
        except Exception as e:
            logger.exception("ManagerChain rings_list exception")
            traceback.print_exc()
            raise e

    def init_var(self):
        """
        Inizializzazione variabili del config.json
        """
        config = self.config

        self.db_log = DatabaseLog(Json.get_value(config, "db_log"))

        app_name = Json.get_value(config, "app", "name")
        # set start_time_env
        start_time_env = os.environ.get('START_TIME')
        if start_time_env is not None:
            logger.info("START_TIME == %s", start_time_env)
            self.start_time = datetime.strptime(start_time_env, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.utcnow().replace(microsecond=0)

        # set expiration_date
        validity_minutes = Json.get_value(config, "app", "validity_minutes")
        expiration_date = None
        if validity_minutes is not None:
            expiration_date = self.start_time + timedelta(minutes=validity_minutes)

        # init spark
        app_init_spark = Json.get_value(config, "app", "init_spark", defval=True)
        app_id = None
        if app_init_spark:
            spark_conf = Json.get_value(config, "spark_conf", defval={})
            spark = Spark(Json.get_value(config, "app", "name", defval="app"), spark_conf)
            app_id = spark.get_application_id()
            mapped_values[self.id_transaction]["spark"] = spark

        mapped_values[self.id_transaction]["db_log"] = self.db_log

        self.continue_if_except = Json.get_value(config, "app", "continue_if_except", defval=False)
        mapped_values[self.id_transaction]["continue_if_except"] = self.continue_if_except

        self.log = DatabaseLog.Log(run_id=self.id_transaction,
                                   script=app_name,
                                   start_time=self.start_time,
                                   expiration_date=expiration_date,
                                   app_id=app_id)

        self.db_log.insert_log(self.log)

    def end(self):
        # chiudere spark, update log
        """
        Funzione di fine-esecuzione per chiudere la spark-session e aggiornare la tabella log della corretta/non corretta esecuzione
        """
        print("End - ManagerChain")
        spark: Spark = mapped_values.get(self.id_transaction).get("spark")
        if spark is not None and spark.get_spark_context():
            spark.get_spark_context().stop()

        end_time = datetime.utcnow().replace(microsecond=0)

        setattr(self.log, 'execution_time', (end_time - self.start_time).total_seconds())
        self.log.end_time = datetime.utcnow().replace(microsecond=0)
        self.log.exit_code = self.exit_code

        self.db_log.update_session(self.log)
        #Copia file log da tmp a hdfs: config.folder_hdfs/{app_name}/YYYYMMDD/
        hdfs_location=Json.get_value(self.config,"log","hdfs_location")
        hdfs_location_complete=os.path.join(hdfs_location,
                                            Json.get_value(self.config, "app", "name"),
                                            datetime.now().strftime("%Y%m%d"))
        HdfsSubprocess.hdfsPut(hdfs_location_complete, env.get_log_path(self.id_transaction))

    def validate_config(self):
        """
        Validazione config.json

        :raise: config.json non valido
        """
        if "ingestion" not in self.config:
            raise PatValidationError("Error in config.json: ingestion key not present")

    def end_ko(self):
        pass
