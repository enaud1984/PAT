import os
import traceback
from datetime import datetime

import jaydebeapi

from common.utility import Common, SqlBuilder

class Db:
    class DatabaseError(Exception):
        def __init__(self, exc):
            super(Db.DatabaseError, self).__init__(exc)

    DB_TYPE_ORACLE = "ORACLE"
    DB_TYPE_POSTGRES = "POSTGRES"
    DB_TYPE_IMPALA = "IMPALA"

    DATA_TYPE_TIMESTAMP = "timestamp"
    DATA_TYPE_INTEGER = "int"
    DATA_TYPE_STRING = "string"
    DATA_TYPE_FLOAT = "float"
    DATA_TYPE_LIST = "list"
    LIST_SEPARATOR = "||"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    DICT_SCRIPT = "SCRIPT"
    DICT_KEY = "KEY"
    DICT_VALUE = "VALUE"
    DICT_TYPE = "TYPE_VALUE"

    def __init__(self, driver, url, user, password, jar_filepath, schema=None):
        self.url = url
        self.user = user
        self.password = password
        self.driver = driver
        self.connection = None
        self.jar_filepath = jar_filepath
        self.connected = False
        self.db_type = None
        self.db_schema = schema
        self.openConnection()

    def openConnection(self):
        try:
            # self.connection = jaydebeapi.connect("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@//{}".format(self.url),
            if "jdbc:oracle:" in self.url:
                self.db_type = self.DB_TYPE_ORACLE
                self.connection = jaydebeapi.connect(self.driver, self.url, {'user': self.user, 'password': self.password},
                                                     self.jar_filepath)
            elif "jdbc:postgresql:" in self.url:
                self.db_type = self.DB_TYPE_POSTGRES
                self.connection = jaydebeapi.connect(self.driver, self.url, [self.user, self.password], self.jar_filepath)
            elif "jdbc:impala:" in self.url:
                self.db_type = self.DB_TYPE_IMPALA
                self.connection = jaydebeapi.connect(self.driver, self.url, [], self.jar_filepath)

            self.connection.jconn.setAutoCommit(False)
            self.connected = True
        except Exception as e:
            raise self.DatabaseError(e)

    def getConnection(self):
        return self.connection

    def close(self):
        if self.connected:
            self.connection.commit()
            self.connection.close()
            self.connected = False

    def resetConnection(self):
        self.connection.close()
        self.openConnection()

    def checkTableExists(self, tablename):
        if "jdbc:postgresql:" in self.url:
            stmt=f"SELECT to_regclass('{tablename}')"
            stmt=f"SELECT '{tablename}'::regclass"
        else:
            return
        dbcur = self.connection.cursor()
        dbcur.execute( stmt )
        print("RETURN QUERY CHECKTABLES EXIST:", dbcur.fetchall())
        print("dbcur.fetchone() is not None: ", dbcur.fetchall() is not None)
        if dbcur.fetchall() is not None:
            print("TABELLA ESISTE")
            dbcur.close()
            return True
        print( "TABELLA non ESISTE" )
        dbcur.close()
        return False

    def executeMany(self, query, tuples, commit=False):

        print("executeMany - query == {}".format(query))
        print("executeMany - tuples == {}".format(tuples))

        if self.connected:
            cursor = self.connection.cursor()
            try:
                cursor.executemany(query, tuples)
                if commit:
                    self.connection.commit()
            except Exception as e:
                raise
            finally:
                cursor.close()
        else:
            pass

    def executeQuery(self, query, parameter=None, commit=False, isSelect=False):
        if self.connected:
            cursor = self.connection.cursor()
            try:
                # Jdbc doesn't support named parameters (:name, ...).
                cursor.execute(query, parameter) if parameter else cursor.execute(query)
                if commit:
                    self.connection.commit()
                if isSelect:
                    res = cursor.fetchall()
                    return res
            except Exception as e:
                raise
            finally:
                cursor.close()
        else:
            pass

    def truncateTable(self, table_name):
        query = "truncate table {}".format(table_name)
        self.executeQuery(query)

    def truncateWithDelete(self, table_name, where_clause="", commit=False):
        query = "DELETE FROM {}".format(table_name)
        if where_clause:
            query += " WHERE {}".format(where_clause)
        self.executeQuery(query, commit=commit)  # ????

    def renameTable(self, old_table_name, new_table_name):
        query = "rename table {} to {}".format(old_table_name, new_table_name)
        self.executeQuery(query)

    def dropTable(self, table_name):
        query = "drop table {}".format(table_name)
        self.executeQuery(query)

    # value_dict = dictionary contenente i valori da modificare e le chiavi sono i nomi delle colonne
    def updateTableWhere(self, table_name, value_dict, where_clause="", commit=False):
        query = "UPDATE {} SET ".format(table_name)
        query += ", ".join("{}='{}'".format(k, v) for k, v in value_dict.items())
        if where_clause:
            query += " where {}".format(where_clause)

        self.executeQuery(query, commit)

    # Da capire
    def insertListOfDictsIntoTable(self, table_name, l, dt_cols=[], commit=False):
        cols = sorted(list(l[0].keys()))

        if dt_cols:
            non_dt_cols = list(set(cols) - set(dt_cols))
        else:
            non_dt_cols = cols

        cols = non_dt_cols + dt_cols

        l = [[dic[col] for col in cols] for dic in l]

        print("insertListOfDictsIntoTable l = {}".format(l))
        self.insertListIntoTable(table_name, l, non_dt_cols, dt_cols)

        if commit:
            self.connection.commit()

    def insertListIntoTable(self, table_name, l, non_dt_cols, dt_cols=[]):
        '''
        The first values of each row should represent non-datetime fields.
        '''
        print("insertListIntoTable l = {}".format(l))
        print("insertListIntoTable dt_cols = {}".format(dt_cols))
        brackets_str = ", ".join(["{}"] * len(non_dt_cols))
        if dt_cols:
            brackets_str += ", "
            brackets_str += ", ".join(["to_timestamp({}, 'yyyy-MM-dd HH24:mi:ss')"] * len(dt_cols))
        n_cols = len(dt_cols) + len(non_dt_cols)
        # brackets_str = brackets_str.format( *[":" + str( i + 1 ) for i in range( 0, n_cols )] )
        brackets_str = brackets_str.format(*["?" for i in range(0, n_cols)])

        cols_str = ", ".join(non_dt_cols + dt_cols)

        query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, cols_str, brackets_str)
        print("insertListIntoTable query = {}".format(query))
        print("insertListIntoTable cols_str = {}".format(cols_str))
        print("insertListIntoTable brackets_str = {}".format(brackets_str))

        self.executeMany(query, l)

    def executeProcedure(self, procname, commit=False, parameters=None):
        if self.connected:
            procname = procname if ("." not in procname) else procname.split(".")[-1]
            final_procname = "{}.{}".format(self.db_schema, procname)

            proc_str = final_procname + "({})".format(",".join(parameters) if parameters else "")
            if self.db_type in (self.DB_TYPE_POSTGRES, self.DB_TYPE_ORACLE):
                procedure_query = "CALL {}".format(proc_str)

            cursor = self.connection.cursor()
            try:
                cursor.execute(procedure_query)
            except Exception as e:
                self.connection.rollback()
                raise
            else:
                self.connection.commit() if commit else None
            finally:
                cursor.close()
        else:
            pass

    def setKeyValue(self, script=None, key=None, value=None, type=None):
        table_name = "KEY_VALUE"
        if self.db_schema:
            table_name = "{}.{}".format(self.db_schema, table_name)

        try:
            if type == self.DATA_TYPE_TIMESTAMP:
                date_format = self.DATETIME_FORMAT
                value_str = value.strftime(date_format)
            elif type == self.DATA_TYPE_LIST:
                value_str = self.LIST_SEPARATOR.join(str(x) for x in value)
            else:
                value_str = str(value)

            if self.db_type == self.DB_TYPE_ORACLE:
                q = "MERGE INTO {table} d USING (select 1 FROM DUAL) \
                        ON (d.SCRIPT = '{script}' AND d.KEY = '{key}' AND d.TYPE_VALUE = '{type}') \
                        WHEN MATCHED THEN UPDATE SET d.VALUE = '{value}' \
                        WHEN NOT MATCHED THEN INSERT (SCRIPT, KEY, VALUE, TYPE_VALUE) VALUES ('{script}', '{key}', '{value}', '{type}')".format(
                    table=table_name, script=script, key=key, value=value_str, type=type)
            elif self.db_type == self.DB_TYPE_POSTGRES:
                q = "INSERT INTO {table} (SCRIPT, KEY, VALUE, TYPE_VALUE) " \
                    "VALUES('{script}', '{key}', '{value}', '{type}') " \
                    "ON CONFLICT (SCRIPT, KEY) DO UPDATE SET VALUE = excluded.VALUE".format(table=table_name,
                                                                                            script=script, key=key,
                                                                                            value=value_str, type=type)

            self.executeQuery(query=q, commit=True)

        except Exception as e:
            print("set_key_value - ERRORE")
            raise e

    def get_key_value(self, script, key, type=None, orderBy=None):
        table_name = "KEY_VALUE"
        if self.db_schema:
            table_name = "{}.{}".format(self.db_schema, table_name)

        try:
            q = "SELECT SCRIPT,KEY,VALUE,TYPE_VALUE FROM {table} WHERE SCRIPT = '{script}' AND KEY LIKE '{key}'".format(
                table=table_name, script=script, key=key)
            q += " ORDER BY {}".format(orderBy) if orderBy is not None else ""

            res = self.executeQuery(query=q, isSelect=True)
            if len(res) > 0:
                return_values = []

                for record in res:
                    rec_script = record[0]
                    rec_key = record[1]
                    rec_value = record[2]
                    rec_type = record[3]

                    if type == self.DATA_TYPE_TIMESTAMP:
                        date_format = "%Y-%m-%d %H:%M:%S"
                        rec_value = datetime.strptime(rec_value, date_format)

                    if type == self.DATA_TYPE_LIST:
                        rec_value = rec_value.split(self.LIST_SEPARATOR)
                    if type == self.DATA_TYPE_INTEGER:
                        rec_value = int(rec_value)
                    if type == self.DATA_TYPE_FLOAT:
                        rec_value = float(rec_value)

                    return_values.append(
                        {self.DICT_SCRIPT: rec_script, self.DICT_KEY: rec_key, self.DICT_VALUE: rec_value,
                         self.DICT_TYPE: rec_type})

                return return_values
            return None

        except Exception as e:
            print("get_key_value - ERRORE")
            print(e)
            traceback.print_exc()
            raise e

    def delKeyValue(self, script=None, key=None):
        table_name = "KEY_VALUE"
        if self.db_schema:
            table_name = "{}.{}".format(self.db_schema, table_name)

        try:
            param = (script, key)
            q = "DELETE FROM {table} WHERE SCRIPT = '{script}' AND KEY = '{key}'".format(table=table_name,
                                                                                         script=script, key=key)

            self.executeQuery(query=q, commit=True)

        except Exception as e:
            print("del_key_value - ERRORE")
            raise e


class DbSingleConnection(Db):
    def __init__(self, driver, url, user, password, jar_filepath, schema=None):
        # super().__init__(driver, url, user, password, jar_filepath, schema)
        self.url = url
        self.user = user
        self.password = password
        self.driver = driver
        self.jar_filepath = jar_filepath
        self.db_type = None
        self.db_schema = schema

    def runQuery(func):
        def wrapper(self, *args, **kwargs):
            if "jdbc:oracle:" in self.url:
                self.db_type = self.DB_TYPE_ORACLE
                argsDb = {'user': self.user, 'password': self.password}
            elif "jdbc:postgresql:" in self.url:
                self.db_type = self.DB_TYPE_POSTGRES
                argsDb = [self.user, self.password]
            elif "jdbc:impala:" in self.url:
                self.db_type = self.DB_TYPE_IMPALA
                argsDb = []
            with jaydebeapi.connect(self.driver, self.url, argsDb, self.jar_filepath) as conn:
                conn.jconn.setAutoCommit(False)
                with conn.cursor() as curs:
                    return func(self, conn, curs, *args, **kwargs)

        return wrapper

    @runQuery
    def checkTableExists(self, conn, curs, tablename):
        if "jdbc:postgresql:" in self.url:
            stmt = f"SELECT to_regclass('{tablename}')"
            stmt = f"SELECT '{tablename}'::regclass"
        else:
            return

        curs.execute(stmt)
        print("RETURN QUERY CHECKTABLES EXIST:", curs.fetchall())
        print("dbcur.fetchone() is not None: ", curs.fetchall() is not None)
        if curs.fetchall() is not None:
            print("TABELLA ESISTE")
            return True
        print("TABELLA non ESISTE")
        return False

    @runQuery
    def executeMany(self, conn, curs, query, tuples, commit=False):

        print("executeMany - query == {}".format(query))
        print("executeMany - tuples == {}".format(tuples))

        try:
            curs.executemany(query, tuples)
            if commit:
                conn.commit()
        except Exception as e:
            raise

    @runQuery
    def executeQuery(self, conn, curs, query, parameter=None, commit=False, isSelect=False):
        try:
            # Jdbc doesn't support named parameters (:name, ...).
            curs.execute(query, parameter) if parameter else curs.execute(query)
            if commit:
                conn.commit()
            if isSelect:
                res = curs.fetchall()
                return res
        except Exception as e:
            raise

    @runQuery
    def executeProcedure(self, conn, curs, procname, commit=False, parameters=None):

        procname = procname if ("." not in procname) else procname.split(".")[-1]
        final_procname = "{}.{}".format(self.db_schema, procname)

        proc_str = final_procname + "({})".format(",".join(parameters) if parameters else "")
        if self.db_type in (self.DB_TYPE_POSTGRES, self.DB_TYPE_ORACLE):
            procedure_query = "CALL {}".format(proc_str)

        try:
            curs.execute(procedure_query)
        except Exception as e:
            conn.rollback()
            raise
        else:
            conn.commit() if commit else None


class DbLog:
    sqlBuilder = SqlBuilder()

    def openConnection(self):
        try:
            # self.connection = jaydebeapi.connect("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@//{}".format(self.url),
            if "jdbc:oracle:" in self.url:
                #self.db_type = self.DB_TYPE_ORACLE
                connection = jaydebeapi.connect(self.driver, self.url,
                                                     {'user': self.user, 'password': self.password},
                                                     self.jar_filepath)
            elif "jdbc:postgresql:" in self.url:
                #self.db_type = self.DB_TYPE_POSTGRES
                connection = jaydebeapi.connect(self.driver, self.url, [self.user, self.password],
                                                     self.jar_filepath)
            elif "jdbc:impala:" in self.url:
                #self.db_type = self.DB_TYPE_IMPALA
                connection = jaydebeapi.connect(self.driver, self.url, [], self.jar_filepath)

            connection.jconn.setAutoCommit(False)
            return connection

        except Exception as e:
            print(e)
            raise self.DatabaseError(e)

    def __init__(self, cfg):
        self.common = Common()
        self.schema = self.common.getJsonValue(cfg, "schema", defval=None)
        self.log_table = "log"
        if self.schema is not None :
            self.log_table = self.schema + "." + self.log_table

        self.url = self.common.getJsonValue(cfg, "url", defval=None)
        self.driver = self.common.getJsonValue(cfg, "driver", defval=None)
        self.user = self.common.getJsonValue(cfg, "user", defval=None)
        self.password = self.common.getJsonValue(cfg, "password", defval=None)
        self.jar_filepath = self.common.getJsonValue(cfg, "jar_filepath", defval=None)

        START_TIME_ENV = os.environ.get('START_TIME')
        if START_TIME_ENV is not None:
            print('START_TIME = ' + START_TIME_ENV)
            self.start_time = datetime.strptime(START_TIME_ENV, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.utcnow().replace(microsecond=0)

    def insert(
            self,
            run_id=None,
            script=None,
            description=None,
            execution_time=None,
            success=1,
            start_time=None,
            end_time=None,
            expiration_date=None,
            exit_code=None,
            app_id=None,
            log_path=None
    ):

        if start_time is None :
            start_time = self.start_time

        columns = ["run_id", "script", "description", "execution_time", "success", "start_time", "end_time",
                   "expiration_date", "exit_code", "app_id", "log_path"]
        tuples = [(run_id, script, description, execution_time, success, start_time,
                   end_time, expiration_date,exit_code, app_id, log_path)]

        queries = self.sqlBuilder.to_insert(self.log_table,columns,tuples)

        with self.openConnection() as connection:
            cursor = connection.cursor()
            for q in queries :
                cursor.execute(q)
            connection.commit()

    def update(
            self,
            run_id=None,
            script=None,
            description=None,
            execution_time=None,
            success=1,
            start_time=None,
            end_time=None,
            expiration_date=None,
            exit_code=None,
            app_id=None,
            log_path=None
    ):

        columns = ["run_id","script", "description", "execution_time", "success", "start_time", "end_time",
                   "expiration_date", "exit_code", "app_id", "log_path"]
        tuples = [(run_id, script, description, execution_time, success, start_time,
                   end_time, expiration_date,exit_code, app_id, log_path)]
        where = ["run_id"]

        queries = self.sqlBuilder.to_update(self.log_table,columns,tuples,where)

        with self.openConnection() as connection:
            cursor = connection.cursor()
            for q in queries :
                cursor.execute(q)
            #cursor.executemany(queries, {})
            connection.commit()

    def truncateLogTable(self):
        try:
            query = "truncate table {}".format(self.log_table)
            self.db.executeQuery(query)
        except:
            pass

    def dropLogTable(self):
        try:
            query = "drop table {}".format(self.log_table)
            self.db.executeQuery(query)
        except:
            pass

    def createLogTable(self):
        query = "CREATE TABLE {} ".format(self.log_table)
        query += "(RUN_ID VARCHAR2(128 BYTE) NOT NULL, SCRIPT VARCHAR2(64 BYTE) NOT NULL, "
        query += "DESCRIPTION VARCHAR2(512 BYTE), EXECUTION_TIME FLOAT(126), "
        query += "EXIT_CODE NUMBER(*,0), "
        query += "START_TIME DATE, END_TIME DATE)"
        self.db.executeQuery(query)

    def createLogTableHdfs(self):
        # solo per postgres
        query = "CREATE TABLE {} ".format(self.log_table_hdfs)
        query += "(RUN_ID character varying(128) NOT NULL, FILE_HDFS character varying(300) NOT NULL)"
        print("CreateLogTableHdfs:", query)
        self.db.executeQuery(query)

