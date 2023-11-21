import logging
import os
from typing import List
logger = logging.getLogger("PAT")


from pat.connector_stream import SparkConnectorStream, KafkaConnectorStream

try:
    import fsspec
except ImportError:
    logging.debug("No fsspec Library")

from pat.class_tool import ConnectorRead
from pat.db_utility import DbJaydebeapi as Db
from pat.utility import Common, SqlBuilder, PostgresSqlBuilder, ImpalaSqlBuilder, Json


try:
    from confluent_kafka import Consumer
except ImportError:
    logging.debug("No confluent_kafka Library")

'''
Classe per connettersi a diversi FileSystem
- HTTP/HTTPS (get/post) (DigestAuth/BasicAuth)
- SOAP
- Kafka
- FTP (File/Folder)
- SSH/SFTP (File/Folder)
- HDFS (File/Folder)
- WEBHDFS (File/Folder)
Protocolli DB si usa spark o jadebeapi:
    - DB Spark-Jdbc
    - DB Jdbc(JayDeBeeApi)
    - DB-HiveSpark
'''

'''
esempio conf.json
****
webhdfs:
****
{
    "id_source": "1",
    "url": "webhdfs__://10.206.227.251:9870/ingestion/json_example/pic_treni_out/2022/02/10/PICTreni2022020710411747470306b44-bf55-4611-bd74-e2954d958df7",
    "param": {
        "type" : "FILE"
    }
}
*****
jdbc:oracle:
*****
{
"id_source": "2",
"url": "jdbc:oracle://localhost:51521/geoanasprd",
"param": {
    "table" : "ASTRADET",
    "schema" : "CATASTO_PUBBLICO",
    "user": "CATASTO_PUBBLICO",
    "password": "Pubbl1co",
    "service": "geoanasprd",
    "driver": "oracle.jdbc.driver.OracleDriver",
    "jar_filepath": "ojdbc8.jar"
    }
}
'''


class ConnectorBase:
    def read(self, url, param, spark=None):
        pass

    def write(self, url, param, data, spark=None):
        pass

    def get_connection_param(self, url, param, protocol):
        properties = ""
        table = ""
        sql_builder = None
        url_param = url.split("//")[1].split(":")
        host = url_param[0]
        port = url_param[1].split("/")[0]
        database = url_param[1].split("/")[1]

        table = Json.get_value(param, "table", defval=None)
        schema = Json.get_value(param, "schema", defval=None)
        jar_filepath = Json.get_value(param, "jar_filepath", defval=None)
        driver = Json.get_value(param, "driver", defval=None)
        user = Json.get_value(param, "user", defval=None)
        password = Json.get_value(param, "password", defval=None)

        spark_jdbc_properties = {}
        if schema is not None: spark_jdbc_properties["schema"] = schema
        if jar_filepath is not None: spark_jdbc_properties["jar_filepath"] = jar_filepath
        if user is not None: spark_jdbc_properties["user"] = user
        if password is not None: spark_jdbc_properties["password"] = password
        if driver is not None: spark_jdbc_properties["driver"] = driver

        if "jdbc:impala" in protocol:
            url = "jdbc:impala://{}:{}/{};auth=noSasl".format(host, port, database)
            '''
            spark_jdbc_properties = {
                'schema': Json.get_value(param, "schema", defval=None),
                'jar_filepath': Json.get_value(param, "jar_filepath", defval=None),
                'driver': Json.get_value(param, "driver", defval=None)
            }
            '''
            sql_builder = ImpalaSqlBuilder()
        elif "jdbc:hive2" in protocol:
            url = "jdbc:hive2://{}:{}/{}".format(host, port, database)
            '''
            spark_jdbc_properties = {
                'user': Json.get_value(param, "user", defval=None)
            }
            '''
            sql_builder = SqlBuilder()
        elif "jdbc:postgresql" in protocol:
            url = "jdbc:postgresql://{}:{}/{}".format(host, port, database)
            spark_jdbc_properties["host"] = host
            spark_jdbc_properties["port"] = port
            spark_jdbc_properties["database"] = database

            '''
            spark_jdbc_properties = {
                'host': host,
                'port': port,
                'user': Json.get_value(param, "user", defval=None),
                'password': Json.get_value(param, "password", defval=None),
                'database': database,
                'schema': Json.get_value(param, "schema", defval=None),
                'driver': Json.get_value(param, "driver", defval=None)
            }
            '''
            sql_builder = PostgresSqlBuilder()
        elif "jdbc:oracle" in protocol:
            url = "jdbc:oracle:thin:@{}:{}/{}".format(host, port, database)
            spark_jdbc_properties["host"] = host
            spark_jdbc_properties["port"] = port
            '''
            spark_jdbc_properties = {
                'host': host,
                'port': port,
                'user': Json.get_value(param, "user", defval=None),
                'password': Json.get_value(param, "password", defval=None),
                'service': Json.get_value(param, "service", defval=None),
                'driver': Json.get_value(param, "driver", defval=None)
            }
            '''
            sql_builder = SqlBuilder()

        if schema is not None and table is not None:
            table = schema + "." + table

        db_client = None
        if "spark" not in protocol:
            db_client = Db(
                driver,
                url,
                user,
                password,
                jar_filepath,
                schema,
                sql_builder=sql_builder
            )

        return url, spark_jdbc_properties, table, db_client

    def get_connection_param_old(self, url, param, protocol):
        properties = ""
        table = ""
        url_param = url.split("//")[1].split(":")
        host = url_param[0]
        port = url_param[1].split("/")[0]
        database = url_param[1].split("/")[1]

        table = Json.get_value(param, "table", defval=None)
        schema = Json.get_value(param, "schema", defval=None)
        sql_builder = None

        if "jdbc:impala" in protocol:
            url = "jdbc:impala://{}:{}/{};auth=noSasl".format(host, port, database)
            properties = {
                'schema': Json.get_value(param, "schema", defval=None),
                'jar_filepath': Json.get_value(param, "jar_filepath", defval=None),
                'driver': Json.get_value(param, "driver", defval=None)
            }
            sql_builder = ImpalaSqlBuilder()
        elif "jdbc:postgresql" in protocol:
            url = "jdbc:postgresql://{}:{}/{}".format(host, port, database)
            properties = {
                'host': host,
                'port': port,
                'user': Json.get_value(param, "user", defval=None),
                'password': Json.get_value(param, "password", defval=None),
                'database': database,
                'schema': Json.get_value(param, "schema", defval=None),
                'driver': Json.get_value(param, "driver", defval=None)
            }
            sql_builder = PostgresSqlBuilder()
        elif "jdbc:oracle" in protocol:
            url = "jdbc:oracle:thin:@{}:{}/{}".format(host, port, database)
            properties = {
                'host': host,
                'port': port,
                'user': Json.get_value(param, "user", defval=None),
                'password': Json.get_value(param, "password", defval=None),
                'service': Json.get_value(param, "service", defval=None),
                'driver': Json.get_value(param, "driver", defval=None)
            }
            sql_builder = SqlBuilder()

        if schema is not None:
            table = schema + "." + table

        return url, properties, table, sql_builder


class JdbcConnector(ConnectorBase):
    def read(self, url, param, spark=None) -> List[tuple]:
        logger.info("JdbcConnector - read - START")
        protocol = Common.get_protocol(url)
        data = None
        db_url, db_properties, db_table, db_client = self.get_connection_param(url, param, protocol)
        sql = None
        if "query_file" in param:
            with open(Json.get_value(param, "query_file", defval=None)) as f:
                sql = f.read().replace('\n', '')
                sql = sql.split(";")
        elif "query" in param:
            sql = Json.get_value(param, "query", defval=None)
        else:
            sql = f"select * from {db_table}"  # cosi posso usare anche una query piu complessa (da testare)o cambiarla a seconda del db

        #data = db_client.execute_query(sql, isSelect=True)  # lista di tuple

        queries = Common.to_list(sql)
        data = db_client.execute_multiple_query(queries,  isSelect=True)  # lista di tuple)

        column = Json.get_value(param, "column", defval=None)
        if column is not None :
            columns = column.split(",")
            data.insert(0,columns)

        logger.info("JdbcConnector - read - END")
        cr = ConnectorRead()
        cr.data = data
        cr.src_name = db_table
        return cr

    def write(self, url, param, data, spark=None):
        logger.info("JdbcConnector - write - START")
        try:
            from pyspark.sql import DataFrame
        except:
            logging.exception("No Spark library")
        protocol = Common.get_protocol(url)
        db_url, db_properties, db_table, db_client = self.get_connection_param(url, param, protocol)
        mode = Json.get_value(param, "mode", defval=None)
        header = Json.get_value(param, "header", defval=True)
        mode = "insert"

        sql = Json.get_value(param, "query", defval=None)
        if sql is not None:
            logger.info(f"JdbcConnector - write - query == %s", sql)
            queries = Common.to_list(sql)
            db_client.execute_multiple_query(queries)
        elif isinstance(data, list):
            # columns = next(iter(data))
            if header:
                columns = data[0]
                values = data[1:]
            else:
                columns = []
                values = data
            queries = []

            if mode == "insert":
                db_client.insert(
                    Json.get_value(param, "schema", defval=None),
                    Json.get_value(param, "table", defval=None),
                    columns,
                    values
                )
            elif mode == "update":
                where_condition = Json.get_value(param, "where", defval=None)
                db_client.update(
                    Json.get_value(param, "schema", defval=None),
                    Json.get_value(param, "table", defval=None),
                    columns,
                    values,
                    where_condition
                )

            # riaggiungo le colonne perchè la pop lo elimina da data e i flussi successivi non la ritrovano
            # data.insert(0, columns)

            '''
            if mode == "insert":
                queries = db_sql_builder.to_insert(db_table, columns, values)
            elif mode == "update":
                where_condition = ""
                ignore_null = None
                queries = db_sql_builder.to_update(db_table, columns, values, where_condition, ignore_null)
            for q in queries:
                db_client.execute_query(q, commit=True)
            data.insert(0,
                        columns)  # riaggiungo le colonne perchè la pop lo elimina da data e i flussi successivi non la ritrovano
            '''
        elif isinstance(data, dict):
            columns = data["columns"]
            values = data["values"]
            # si potrebbero mettere pure nel dict di data
            where_condition = Json.get_value(param, "where_condition", defval=[])
            ignore_null = Json.get_value(param, "ignore_null", defval=None)
            queries = []
            if mode == "insert":
                db_client.insert(
                    Json.get_value(param, "schema", defval=None),
                    Json.get_value(param, "table", defval=None),
                    columns,
                    values
                )
            elif mode == "update":
                db_client.update(
                    Json.get_value(param, "schema", defval=None),
                    Json.get_value(param, "table", defval=None),
                    columns,
                    values,
                    where_condition
                )
            for q in queries:
                db_client.execute_query(q, commit=True)
        elif isinstance(data, DataFrame):
            # TODO
            values = []
            where_condition = ""
            df = data
            for i in df.collect():
                values.append(tuple(i))  # convert to tuple and append to list
            header = df.columns
            columns = ','.join(map(str, header))
            if mode == "insert":
                db_client.insert(
                    Json.get_value(param, "schema", defval=None),
                    Json.get_value(param, "table", defval=None),
                    columns,
                    values
                )
            elif mode == "update":
                db_client.update(
                    Json.get_value(param, "schema", defval=None),
                    Json.get_value(param, "table", defval=None),
                    columns,
                    values,
                    where_condition
                )
            # TODO
        logger.info("JdbcConnector - write - END")


'''
class PandasJdbcConnector(ConnectorBase):
    def read(self, url, param, spark=None):
        try :
            cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_12_2")
        except :
            pass
        protocol = Common.get_protocol(url)
        db_url, db_properties, db_table, db_client = self.get_connection_param(url, param, protocol)

        DIALECT = 'oracle'
        SQL_DRIVER = 'cx_oracle'
        USERNAME = Json.get_value(param, "schema", defval=None)
        PASSWORD = Json.get_value(param, "password", defval=None)
        HOST = '127.0.0.1'
        PORT = 1521
        SERVICE = 'xe'
        ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE
        TABLENAME = Json.get_value(param, "table", defval=None)

        conn = sqlalchemy.create_engine(ENGINE_PATH_WIN_AUTH)
        SQL = "SELECT * FROM {}".format(TABLENAME)
        data = pd.read_sql_query(SQL, con=conn)
        #data = pd.read_sql_table(TABLENAME, con=conn)

        #data = conn.execute(SQL)

        cr = ConnectorRead().data(data).filename(db_table)
        return cr

    def write(self, url, param, data, spark=None):
        try :
            cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_12_2")
        except :
            pass
        protocol = Common.get_protocol(url)
        db_url, db_properties, db_table, db_client = self.get_connection_param(url, param, protocol)

        DIALECT = 'oracle'
        SQL_DRIVER = 'cx_oracle'
        USERNAME = Json.get_value(param, "schema", defval=None)
        PASSWORD = Json.get_value(param, "password", defval=None)
        HOST = '127.0.0.1'
        PORT = 1521
        SERVICE = 'xe'
        ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE
        TABLENAME = Json.get_value(param, "table", defval=None)
        conn = sqlalchemy.create_engine(ENGINE_PATH_WIN_AUTH)
        data.to_sql(TABLENAME, conn, if_exists='append', index=False)
'''


class SparkJdbcConnector(ConnectorBase):
    def read(self, url, param, spark=None):
        protocol = Common.get_protocol(url)
        # db_url, db_properties, db_table, db_client = self.get_f.wconnection_param(url, param, protocol)
        # data = spark.read.jdbc(db_url, db_table, properties=db_properties)
        db_url, db_properties, db_table, db_sql_builder = self.get_connection_param(url, param, protocol)

        if "query_file" in param:
            with open(Json.get_value(param, "query_file", defval=None)) as f:
                q = f.read().replace('\n', '')
            data = spark.read.jdbc(db_url, q, properties=db_properties)
        elif "query" in param:
            # TODO regexp : ^\(.*\) [a-zA-Z ]$
            query_ = "(" + Json.get_value(param, "query", defval=None) + ") qry_"
            logger.debug(f"SparkHiveConnector - spark_query : {query_}")
            data = spark.read.jdbc(db_url, query_, properties=db_properties)
            # data = spark.read.jdbc(db_url, Json.get_value(param, "query", defval=None), properties=db_properties)

        else:
            data = spark.read.jdbc(db_url, db_table, properties=db_properties)

        '''
        if "table" in param and "query" not in param:
            data = spark.read.jdbc(db_url, db_table, properties=db_properties)
        elif "query" in param:  # usare query con alias esempio: query = "(select * from employees where emp_no < 10008) emp_alias: in oracle senza alias"
            data = spark.read.jdbc(db_url, Json.get_value(param, "query", defval=None), properties=db_properties)
        '''

        cr = ConnectorRead()
        cr.data = data
        cr.src_name = db_table

        return cr

    def write(self, url, param, data, spark=None):
        df = data
        protocol = Common.get_protocol(url)
        mode = Json.get_value(param, "mode", defval=None)
        db_url, db_properties, db_table, db_client = self.get_connection_param(url, param, protocol)

        # gestione truncate
        df_write = df.write
        if mode == "overwrite":
            df_write = df_write.option("truncate", "true")

        df_write.jdbc(db_url, db_table, properties=db_properties, mode=mode)


class SparkHiveConnector(ConnectorBase):
    def read(self, url, param, spark=None):
        logger.info("SparkHiveConnector - read - START")
        database = url.split("//")[1]
        protocol = Common.get_protocol(url)
        data = None
        if "query" in param:
            data = spark.sql(
                Json.get_value(param, "query", defval=None))
        elif "db_table" in param and "query" not in param:
            data = spark.table(
                f"{database}.{Json.get_value(param, 'db_table', defval=None)}")
        logger.info("SparkHiveConnector - read - END")
        return data

    def write(self, url, param, data, spark=None):
        logger.info("SparkHiveConnector - write - START")
        database = url.split("/,/")[1]
        protocol = Common.get_protocol(url)

        if "query_data" in param:
            spark.get_hive_context().sql(data)
        else:
            df = data
            mode = Json.get_value(param, "mode", defval=None)
            location = Json.get_value(param, "location", defval=None)
            table = Json.get_value(param, "table", defval=None)
            table = f"{database}.{table}"
            file_format = Json.get_value(param, "file_format", defval=None)

            df = df.write \
                .format(file_format) \
                .mode(mode) \
                .option("path", location)

            partition_fields = Json.get_value(param, "partition_fields", defval=None)
            if partition_fields is not None:
                # self.logger.info("writeDataToHiveImpl [{}] - set partition_fields".format(k))
                df = df.partitionBy(*partition_fields)
            df.saveAsTable(table)
        logger.info("SparkHiveConnector - write - END")


class KafkaConfluentConnector(ConnectorBase):
    def read(self, url, param, spark=None):
        logger.info("KafkaConfluentConnector - read - START")
        bootstrap_server = url.split("//")[1]
        protocol = Common.get_protocol(url)

        group_id = Json.get_value(param, "group_id", defval=None)
        auto_offset_reset = Json.get_value(param, "auto_offset_reset", defval="latest")
        topics = Json.get_value(param, "topics", defval=None)
        num_messages = Json.get_value(param, "num_messages", defval=1)
        timeout = Json.get_value(param, "timeout", defval=-1)
        conf = {
            'bootstrap.servers': bootstrap_server,
            'group.id': group_id,
            'enable.auto.commit': True,
            'auto.offset.reset': auto_offset_reset
        }
        data = []
        topic_src = []
        consumer = Consumer(conf)
        try:
            consumer.subscribe(topics.split(","))

            messages = consumer.consume(num_messages=num_messages, timeout=timeout)
            if messages is None:
                return None

            for msg in messages:
                if msg.error():
                    # todo gestire errore
                    logger.error(f"KafkaConfluentConnector - errore msg {msg}")
                else:
                    data.append(msg.value())
                    topic_src.append(msg.topic())
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()

        logger.info("KafkaConfluentConnector - read - END")

        cr = ConnectorRead()
        cr.data = data
        cr.src_name = topic_src
        return cr

    def write(self, url, param, data, spark=None):
        logger.info("KafkaConfluentConnector - write - START")
        #todo



class FileConnector(ConnectorBase):
    def get_list_file_name(self, protocol, url, param):
        with fsspec.open(url, **param, mode='rb') as fn:
            fs_ = fn.fs
        maxdepth = Json.get_value(param, "maxdepth")
        list_file_name = list(
            map(lambda x: f"{protocol}://{fs_.storage_options['host']}:{fs_.storage_options['port']}{x}",
                fs_.find(fn.path,maxdepth=maxdepth)))
        """
        list_directory = fs_.listdir(fn.path)
        list_file_name = []
        for d in list_directory:
            if d["type"] == 'file':
                file_name = f"{protocol}://{fs_.storage_options['host']}:{fs_.storage_options['port']}{d['name']}"
                logger.debug(f"add {file_name}")
                list_file_name.append(file_name)
            else:
                file_name_tmp = f"{protocol}://{fs_.storage_options['host']}:{fs_.storage_options['port']}{d['name']}/"
                logger.debug(f"read {file_name_tmp}")
                list_file_name_tmp = self.get_list_file_name(protocol, file_name_tmp, param)
                list_file_name.extend(list_file_name_tmp)
        """

        return list_file_name

    def get_list_file_name_(self, protocol, url, param):
        with fsspec.open(url, **param, mode='rb') as fn:
            fs_ = fn.fs

        # list_files = fsspec.open_files(url, **param)  ##!! lista oggetti folder
        list_files = fs_.ls(fn.path)
        list_directory = fs_.listdir(fn.path)
        list_file_name = []
        for f in list_files:
            if f.path.endswith("/"):
                file_name_tmp = f"{protocol}://{f.fs.storage_options['host']}:{f.fs.storage_options['port']}{f.path}*"
                logger.debug("read %s", file_name_tmp)
                list_file_name_tmp = self.get_list_file_name(protocol, file_name_tmp, param)
                list_file_name.extend(list_file_name_tmp)
            else:
                file_name = f"{protocol}://{f.fs.storage_options['host']}:{f.fs.storage_options['port']}{f.path}"
                logger.debug("add %s", file_name)
                list_file_name.append(file_name)
        return list_file_name

    def read(self, url, param, spark=None):
        protocol = Common.get_protocol(url)
        readList = []
        type_ = Json.get_value(param, "type", defval=None)

        if type_ == "FOLDER":
            '''
            list_files = fsspec.open_files(url, **param)  ##!! lista oggetti folder

            list_file_name = []
            for f in list_files :
                if f.path.endswith("/") :
                    logger.warn(f"NOT add {f.path}")
                else :
                    file_name = f"{protocol}://{f.fs.storage_options['host']}:{f.fs.storage_options['port']}{f.path}"
                    logger.debug(f"add {file_name}")
                    list_file_name.append(file_name)
            '''

            list_file_name = self.get_list_file_name(protocol, url, param)

            data_list = []
            filename_list = []

            if len(list_file_name) > 0:  # o parametro

                for f in list_file_name:
                    with fsspec.open(f, mode='rb',**param) as fs:
                        data_list.append(fs.read())
                        # filename_list.append(os.path.basename(f))
                        filename_list.append(f)
            obj_push = ConnectorRead()
            obj_push.data = data_list
            obj_push.src_name = filename_list
            readList.append(obj_push)
        else:
            data_list = []
            filename_list = []
            with fsspec.open(url, **param) as fs:
                data_list.append(fs.read())
                filename_list.append(os.path.basename(url))
                obj_push = ConnectorRead()
                obj_push.data = data_list
                obj_push.src_name = filename_list
                readList.append(obj_push)
        return readList

    def write(self, url, param, data, spark=None):
        protocol = Common.get_protocol(url)
        with fsspec.open(url, **param, mode='wb') as fn:
            fn.write(data)

    def delete(self, url, param, spark=None):
        logger.info("FileConnector - delete - url == %s", url)

        protocol = Common.get_protocol(url)
        with fsspec.open(url, **param, mode='wb') as fn:
            logger.info("FileConnector - delete - fn.path == %s", fn.path)
            # fn.fs.rm_file(fn.path)
            fs_ = fn.fs

        fs_.rm_file(fn.path)


class Connector:
    connector_map = {
        "spark:hive": SparkHiveConnector,
        # "spark:jdbc:oracle": PandasJdbcConnector,
        "spark:jdbc:oracle": SparkJdbcConnector,
        "spark:jdbc:postgresql": SparkJdbcConnector,
        "spark:jdbc:impala": SparkJdbcConnector,
        "jdbc:oracle": JdbcConnector,
        "jdbc:postgresql": JdbcConnector,
        "jdbc:impala": JdbcConnector,
        "jdbc:hive": JdbcConnector,
        "jdbc:hive2": JdbcConnector,
        "kafka_confluent": KafkaConfluentConnector,
        "stream:kafka": KafkaConnectorStream,
        "stream:spark": SparkConnectorStream,
        "stream:mqtt": ""
    }

    def read(self, url, param, spark=None):
        if param == None:
            param = {}
        protocol = Common.get_protocol(url)
        logging.info(f"Connector read - protocol == {protocol}")
        if protocol in Connector.connector_map.keys():
            connector = Connector.connector_map[protocol]()
        else:
            connector = FileConnector()
        return connector.read(url, param, spark=spark)

    def write(self, url, param, data, spark=None):
        if param == None:
            param = {}
        protocol = Common.get_protocol(url)
        logging.info(f"Connector write - protocol == {protocol}")
        if protocol in Connector.connector_map.keys():
            connector = Connector.connector_map[protocol]()
        else:
            connector = FileConnector()
        return connector.write(url, param, data, spark=spark)

    def delete(self, url, param, spark=None):
        if param == None:
            param = {}
        protocol = Common.get_protocol(url)
        if protocol in Connector.connector_map.keys():
            logger.error("Connector - UNIMPLEMENTED METHOD")
        else:
            connector = FileConnector()
        return connector.delete(url, param, spark=spark)

    def stream(self,url,param,spark=None, operation=None):
        if param == None:
            param = {}
        protocol = Common.get_protocol(url)
        if protocol in Connector.connector_map.keys():
            connector=Connector.connector_map[protocol]()
        else:
            logger.error("Connector Stream - UNIMPLEMENTED METHOD")

        return connector.read(url,param,spark=spark,operation=operation)


