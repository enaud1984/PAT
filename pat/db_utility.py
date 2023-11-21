import base64
import os
from datetime import datetime, date
import logging

import traceback

try:
    import jaydebeapi
    import jpype
except ImportError:
    logging.debug("No jaydebeapi Library")

try:
    import cx_Oracle
except ImportError:
    logging.debug("No cx_Oracle Library")

from pat.utility import Common, SqlBuilder, Json


class Db:
    def __init__(self, driver, url, user, password, jar_filepath, schema=None, sql_builder=SqlBuilder(), service=None,
                 decode_password=False):
        # super().__init__(driver, url, user, password, jar_filepath, schema)
        self.sql_builder = sql_builder
        self.url = url
        self.user = user
        self.password = base64.b64decode(password).decode("utf-8") if decode_password else password
        self.driver = driver
        self.jar_filepath = jar_filepath
        self.schema = schema
        self.service = service

    def insert(
            self,
            schema=None,
            table=None,
            columns=None,
            tuples=None
    ):
        if schema is not None:
            table = schema + "." + table

        # queries = self.sql_builder.to_insert(table, columns, tuples)
        # self.execute_multiple_query(queries, commit=True)
        query = self.sql_builder.get_insert_string(table, columns)
        self.execute_many(query, tuples, commit=True)

    def update(
            self,
            schema=None,
            table=None,
            columns=None,
            tuples=None,
            where=None
    ):
        if schema is not None:
            table = schema + "." + table

        queries = self.sql_builder.to_update(table, columns, tuples, where)
        # self.execute_multiple_query(queries, commit=True)
        self.execute_many(queries, tuples, commit=True)

    def select(
            self,
            schema=None,
            table=None,
            columns=None,
            where=None
    ):
        if schema is not None:
            table = schema + "." + table

        query = self.sql_builder.to_select(table, columns, where, ignore_null=False)
        return self.execute_query(query, isSelect=True)


class DbJaydebeapi(Db):

    def insert(
            self,
            schema=None,
            table=None,
            columns=None,
            tuples=None
    ):
        if schema is not None:
            table = schema + "." + table

        if "jdbc:hive2:" in self.url:
            query = self.sql_builder.to_insert(table, columns, tuples, one_statement=True)
            self.execute_query(query)
        else:
            query = self.sql_builder.get_insert_string(table, columns)
            self.execute_many(query, tuples, commit=True)



    def run_query(func):
        def wrapper(self, *args, **kwargs):
            '''
            if "jdbc:oracle:" in self.url:
                argsDb = {'user': self.user, 'password': self.password}
            elif "jdbc:postgresql:" in self.url:
                argsDb = [self.user, self.password]
            elif "jdbc:impala:" in self.url:
                argsDb = []
            '''

            argsDb = {}
            if self.user is not None:
                argsDb["user"] = self.user
            if self.password is not None:
                argsDb["password"] = self.password
            if self.jar_filepath is not None:
                argsDb["jar_filepath"] = self.jar_filepath

            with jaydebeapi.connect(self.driver, self.url, argsDb, self.jar_filepath) as conn:
                conn.jconn.setAutoCommit(False)
                with conn.cursor() as curs:
                    return func(self, conn, curs, *args, **kwargs)

            '''
            to_ret = None
            try :
                conn = jaydebeapi.connect(self.driver, self.url, argsDb, self.jar_filepath)
                curs = conn.cursor()
                to_ret = func(self, conn, curs, *args, **kwargs)
            except Exception as e:
                traceback.print_exc()            
                print("Something went wrong", e)
            finally:
                curs.close()
                conn.close()

            return to_ret
            '''

        return wrapper

    @run_query
    def check_table_exists(self, conn, curs, tablename, schema=None):
        if "jdbc:postgresql:" in self.url:
            stmt = f"SELECT to_regclass('{tablename}')"
            stmt = f"SELECT '{tablename}'::regclass"
        elif "jdbc:oracle:" in self.url:
            stmt = f"select object_name from all_objects where object_type in ('TABLE','VIEW') and object_name='{tablename}'"
            if schema:
                stmt = f"{stmt} and owner={schema}"
        else:
            return None
        curs.execute(stmt)
        print("RETURN QUERY CHECKTABLES EXIST:", curs.fetchall())
        print("dbcur.fetchone() is not None: ", curs.fetchall() is not None)
        if curs.fetchall() is not None:
            print("TABELLA ESISTE")
            return True
        print("TABELLA non ESISTE")
        return False

    @run_query
    def execute_many(self, conn, curs, query, tuples, commit=False):
        print("executeMany - query == {}".format(query))
        # print("executeMany - tuples == {}".format(tuples))
        try:
            # cast Timestamp/Date
            tuples = self.castTimestampValues(tuples)
            if "jdbc:hive2:" in self.url:
                #in hive non è implementato il metodo addBatch
                for t in tuples :
                    #q = self.sql_builder.to_select(table, t, None, ignore_null=False)
                    curs.execute(query,t)

            else:
                curs.executemany(query, tuples)  # aggiungere tuples, query con i ? nei values, tuples coi valori [()]
            if commit:
                conn.commit()
        except Exception as e:
            raise

    @run_query
    def execute_query(self, conn, curs, query, parameter=None, commit=False, isSelect=False):
        try:
            # Jdbc doesn't support named parameters (:name, ...).
            curs.execute(query, parameter) if parameter else curs.execute(query)
            if commit:
                conn.commit()
                # return curs.lastrowid
            if isSelect:
                res = curs.fetchall()
                return res
        except Exception as e:
            raise

    @run_query
    def execute_multiple_query(self, conn, curs, queries, parameter=None, commit=False, isSelect=False):

        try:
            # Jdbc doesn't support named parameters (:name, ...).
            for query in queries:
                curs.execute(query, parameter) if parameter else curs.execute(query)
            if commit:
                conn.commit()
                # return curs.lastrowid
            if isSelect:
                res = curs.fetchall()
                return res
        except Exception as e:
            raise

    @run_query
    def execute_procedure(self, conn, curs, procname, commit=False, parameters=None):
        procname = procname if ("." not in procname) else procname.split(".")[-1]
        final_procname = "{}.{}".format(self.schema, procname)

        proc_str = final_procname + "({})".format(",".join(parameters) if parameters else "")
        '''
        if self.db_type in (self.DB_TYPE_POSTGRES, self.DB_TYPE_ORACLE):
            procedure_query = "CALL {}".format(proc_str)
        '''
        procedure_query = "CALL {}".format(proc_str)

        try:
            curs.execute(procedure_query)
        except Exception as e:
            conn.rollback()
            raise
        else:
            conn.commit() if commit else None

    def castTimestampValues(self, list_tuples):
        # self.initJVM(self.jar_filepath)
        fix_values = []
        # problema datetime eredita date----> usare type
        # date per campo Oracle Date: datetime.strptime("20-02-2022","%d-%m-%Y").date()
        for tu in list_tuples:
            fix_tuple = []
            for col in tu:
                if type(col) == date:
                    col = jpype.java.sql.Date @ col
                if type(col) == datetime:
                    col = jpype.java.sql.Timestamp @ col
                fix_tuple.append(col)
            fix_tuple = tuple(fix_tuple)
            fix_values.append(fix_tuple)
        return fix_values

    def initJVM(self, jar_filepath):
        if jpype.isJVMStarted():
            jpype.shutdownJVM()

        jvm_path = jpype.getDefaultJVMPath()
        args = f'-Djava.class.path={self.jar_filepath}'
        jpype.startJVM(jvm_path, args, convertStrings=True)

    def initJVM_old(self, jar_filepath):
        if jpype.isJVMStarted():
            jpype.shutdownJVM()

        jvm_path = jpype.getDefaultJVMPath()
        args = f'-Djava.class.path={self.jar_filepath}'
        jpype.startJVM(jvm_path, args, convertStrings=True)


class DbCxOracle(Db):

    def run_query(func):
        def wrapper(self, *args, **kwargs):
            string_connection = "{}/{}@{}:{}/{}".format(self.user, self.password, self.host, self.port, self.service)
            with cx_Oracle.connect(string_connection, encoding="UTF-8", nencoding="UTF-8") as conn:
                conn.autocommit = False
                with conn.cursor() as curs:
                    return func(self, conn, curs, *args, **kwargs)

        return wrapper

    @run_query
    def check_table_exists(self, conn, curs, tablename, schema=None):
        # TODO
        return False

    @run_query
    def execute_many(self, conn, curs, query, tuples, commit=False):
        '''
        values = []
        print("executeMany tuples 1 = {}".format(tuples))
        if tuples in ([], None):
            return
        if fields not in (None, "", []):
            values = []
            for r in tuples:
                value = {}
                for i in range( len( r ) ):
                    value[fields[i]] = r[i]
                values.append( value )
            tuples = values
        self.valori = tuples
        '''

        print("executeMany tuples 2 = {}".format(tuples))
        print("executeMany query = {}".format(query))
        try:
            curs.prepare(query)
            curs.executemany(None, tuples)
        except Exception as e:
            error, = e.args
            print('Error.code =', error.code)
            print('Error.message =', error.message)
            print('Error.offset =', error.offset)
            print("Row", curs.rowcount, "has error", error.message)
            self.conn.rollback()
            raise
        else:
            self.conn.commit() if commit else None

    @run_query
    def execute_query(self, conn, curs, query, parameter=None, commit=False, isSelect=False, isSelectColumns=False):
        try:
            curs.prepare(query)
            curs.execute(query, parameter) if parameter else curs.execute(query)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            conn.rollback()
            raise e
        else:
            conn.commit() if commit else None

        if isSelectColumns:
            res = curs.fetchall()
            columns = [i[0] for i in curs.description]
            return res, columns
        elif isSelect:
            res = curs.fetchall()
            return res

    @run_query
    def execute_multiple_query(self, conn, curs, queries, parameter=None, commit=False, isSelect=False):
        # TODO
        return None

    @run_query
    def execute_procedure(self, conn, curs, procname, commit=False, parameters=None):
        # TODO
        return None


class DbLog(DbJaydebeapi):
    """
    Non usata se presente SqlAlchemy
    """
    def __init__(self, cfg):
        self.common = Common()

        START_TIME_ENV = os.environ.get('START_TIME')
        if START_TIME_ENV is not None:
            print('START_TIME = ' + START_TIME_ENV)
            self.start_time = datetime.strptime(START_TIME_ENV, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.utcnow().replace(microsecond=0)

        super().__init__(
            driver=Json.get_value(cfg, "driver", defval=None),
            url=Json.get_value(cfg, "url", defval=None),
            user=Json.get_value(cfg, "user", defval=None),
            password=Json.get_value(cfg, "password", defval=None),
            decode_password=Json.get_value(cfg, "decode_password", defval=False),
            jar_filepath=Json.get_value(cfg, "jar_filepath", defval=None),
            schema=Json.get_value(cfg, "schema", defval=None)
        )

    def truncateLogTable(self):
        try:
            query = "truncate table {}".format(self.log_table)
            self.db.execute_query(query)
        except:
            pass

    def dropLogTable(self):
        try:
            query = "drop table {}".format(self.log_table)
            self.db.execute_query(query)
        except:
            pass

    def create_log_table(self):
        query = "CREATE TABLE {} ".format(self.log_table)
        query += "(RUN_ID VARCHAR2(128 BYTE) NOT NULL, SCRIPT VARCHAR2(64 BYTE) NOT NULL, "
        query += "DESCRIPTION VARCHAR2(512 BYTE), EXECUTION_TIME FLOAT(126), "
        query += "EXIT_CODE NUMBER(*,0), "
        query += "START_TIME DATE, END_TIME DATE)"
        self.db.execute_query(query)

    def createLogTableHdfs(self):
        # solo per postgres
        query = "CREATE TABLE {} ".format(self.log_table_hdfs)
        query += "(RUN_ID character varying(128) NOT NULL, FILE_HDFS character varying(300) NOT NULL)"
        print("CreateLogTableHdfs:", query)
        self.db.execute_query(query)

        # param contiene la lista dei parametri input (fields) e la lista dei valori (tuples)

    ########################################################################
    ####################              log               ####################
    ########################################################################

    def get_param(
            self,
            p1,
            p2,
            ignore_null=False
    ):
        for idx, val in reversed(list(enumerate(p1))):
            if ignore_null and p2[idx] is None:
                p1.pop(idx)
                p2.pop(idx)
            elif val == 'self':
                p1.pop(idx)
                p2.pop(idx)
        return [p1, p2]

    def log_insert(
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
            log_path=None,
            message=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print("log_insert param == ", param)
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        self.insert(self.schema, "log", param[0], [param[1]])

    def log_update(
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
            log_path=None,
            message=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()), ignore_null=True)
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        # where coincide con la chiave
        where = {
            "run_id": run_id
        }
        self.update(self.schema, "log", param[0], [param[1]], where)

    def log_select(
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
            log_path=None,
            message=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        where = {param[0][i]: param[1][i] for i in range(len(param[0]))}
        return self.select(self.schema, "log", param[0], where)

    ########################################################################
    ####################         log_operation          ####################
    ########################################################################

    def log_operation_insert(
            self,
            run_id=None,
            operation=None,
            start_time=None,
            end_time=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        self.insert(self.schema, "log_operation", param[0], [param[1]])

    def log_operation_update(
            self,
            run_id=None,
            operation=None,
            start_time=None,
            end_time=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()), ignore_null=True)
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        # where coincide con la chiave
        where = {
            "run_id": run_id,
            "operation": operation
        }
        self.update(self.schema, "log_operation", param[0], [param[1]], where)

    def log_operation_select(
            self,
            run_id=None,
            operation=None,
            start_time=None,
            end_time=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        where = {param[0][i]: param[1][i] for i in range(len(param[0]))}
        return self.select(self.schema, "log_operation", param[0], where)

    ########################################################################
    ####################         log_exit_code          ####################
    ########################################################################

    def log_exit_code_insert(
            self,
            type=None,
            module=None,
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        self.insert(self.schema, "log_exit_code", param[0], [param[1]])

    def log_exit_code_update(
            self,
            exit_code=None,
            type=None,
            module=None,
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()), ignore_null=True)
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        # where coincide con la chiave
        where = {
            "exit_code": exit_code
        }
        self.update(self.schema, "log_exit_code", param[0], [param[1]], where)

    def log_exit_code_select(
            self,
            exit_code=None,
            type=None,
            module=None,
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))


        where = {param[0][i]: param[1][i] for i in range(len(param[0]))}
        print("log_exit_code_select where:", where)

        return self.select(self.schema, "log_exit_code", param[0], where)

    ########################################################################
    ####################           log_file             ####################
    ########################################################################

    def log_file_insert(
            self,
            run_id=None,
            src_name=None,
            dst_name=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        self.insert(self.schema, "log_file", param[0], [param[1]])

    def log_file_update(
            self,
            run_id=None,
            src_name=None,
            dst_name=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()), ignore_null=True)
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        # where coincide con la chiave
        where = {
            "run_id": run_id,  # exit_code, #dovrebbe essere run_id
            "filename": src_name
        }
        self.update(self.schema, "log_file", param[0], [param[1]], where)

    def log_file_select(
            self,
            run_id=None,
            src_name=None,
            dst_name=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        # param = [list(locals().keys())[1:], list(locals().values())[1:]]  # rimuovo il primo (self) parametro input
        where = {param[0][i]: param[1][i] for i in range(len(param[0]))}
        return self.select(self.schema, "log_file", param[0], where)