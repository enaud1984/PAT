import logging
import re
import base64
from collections import namedtuple
from typing import Union
from pat.db_utility import DbJaydebeapi
from pat.utility import Json, SqlBuilder

'''
Implementazione del db_log utilizzando la libreria DbJaydebeapi
per abilitarla modificare il valore in env.py :
  db_log_library = "jaydebeapi"
'''
class DatabaseLog(DbJaydebeapi):
    logger = logging.getLogger(__name__)
    schema = "ingestion"

    table_name = {
        "Log": "log",
        "LogExitCode": "log_exit_code",
        "LogFile": "log_file",
        "LogOperation": "log_operation"
    }

    def __init__(self, cfg):
        super().__init__(
            driver=Json.get_value(cfg, "driver", defval=None),
            url=Json.get_value(cfg, "url", defval=None),
            user=Json.get_value(cfg, "user", defval=None),
            password=Json.get_value(cfg, "password", defval=None),
            decode_password=Json.get_value(cfg, "decode_password", defval=False),
            jar_filepath=Json.get_value(cfg, "jar_filepath", defval=None),
            schema=Json.get_value(cfg, "schema", defval=None))

        self.log_table = "log"
        self.log_exit_code_table = "log_exit_code"
        self.log_file_table = "log_file"
        self.log_operation_table = "log_operation"

        # self.create_tables()

    class LogBase(dict):
        default = {}

        def __init__(self, *args, **kw):
            z = {**self.default, **kw}
            for key, value in z.items():
                super(DatabaseLog.LogBase, self).__setitem__(key, value)

        def __setattr__(self, key, value):
            super(DatabaseLog.LogBase, self).__setitem__(key, value)

    class Log(LogBase):
        default = {
            "run_id": None,
            "script": None,
            "description": None,
            "execution_time": None,
            "success": None,
            "start_time": None,
            "end_time": None,
            "expiration_date": None,
            "exit_code": None,
            "app_id": None,
            "log_path": None,
            "message": None
        }

    class LogExitCode(LogBase):
        default = {
            "exit_code": None,
            "type": None,
            "module": None
        }

    class LogFile(LogBase):
        default = {
            "run_id": None,
            "src_name": None,
            "dst_name": None,
            "exit_code": None
        }

    class LogOperation(LogBase):
        default = {
            "run_id": None,
            "operation": None,
            "start_time": None,
            "end_time": None,
            "exit_code": None
        }

    def create_tables(self):
        print("ciao")
        query = f"""CREATE TABLE ingestion.{self.log_table} (
        run_id varchar(128) NULL,
        script varchar(64) NULL,
        description varchar(4000) NULL,
        execution_time float4 NULL,
        success int4 NULL,
        start_time timestamp(0) NULL,
        end_time timestamp(0) NULL,
        expiration_date timestamp(0) NULL,
        exit_code int4 NULL,
        app_id varchar(255) NULL,
        log_path varchar(500) NULL,
        message text NULL
        )"""
        self.execute_query(query)

        query = f"""CREATE TABLE ingestion.{self.log_exit_code_table} (
        exit_code serial4 NOT NULL,
        "type" varchar(200) NULL,
        "module" varchar(200) NULL
        )       
        """
        self.execute_query(query)

        query = f"""CREATE TABLE ingestion.{self.log_file_table} (
        run_id varchar(128) NULL,
        src_name varchar(200) NULL,
        dst_name varchar(1000) NULL,
        exit_code int4 NULL
        )"""
        self.execute_query(query)

        query = f"""CREATE TABLE ingestion.{self.log_operation_table} (
        run_id varchar(128) NOT NULL,
        operation varchar(64) NOT NULL,
        start_time timestamp NULL,
        end_time timestamp NULL,
        exit_code int4 NULL,
        CONSTRAINT log_operation_pkey PRIMARY KEY (run_id, operation)
        )"""
        self.execute_query(query)

    def insert_log(self, log_row):
        if isinstance(log_row, DatabaseLog.LogExitCode):
            if log_row["exit_code"] is None:
                query = "SELECT nextval('ingestion.log_exit_code_exit_code_seq')"
                x = self.execute_query(query, isSelect=True)[0][0]
                log_row["exit_code"] = x

        param = log_row
        self.logger.info("log_insert param == ", param)
        self.insert(
            schema=self.schema,
            table=self.table_name[type(param).__name__],
            columns=param.keys(),
            tuples=[param.values()]
        )

    def get_item(self, class_name, pk_value):
        where = {"run_id": pk_value}
        param = class_name._asdict()
        column = param.keys()
        res = self.select(self.table_name[type(param).__name__], column, where)
        res_dict = [dict(zip(column, row)) for row in res]

        for row in res_dict:
            return class_name(**row)

    def get_exit_code(self, type=None, module=None):
        where = {
            "type": type,
            "module": module
        }
        column = "exit_code"
        res = self.select(
            schema=self.schema,
            table="log_exit_code",
            columns=["exit_code"],
            where=where
        )

        if len(res) > 0:
            return res[0][0]
        else:
            return None

    def update_session(self, cls_instance=None):
        if cls_instance:
            param = cls_instance
            where = None
            if isinstance(cls_instance, DatabaseLog.Log):
                where = {
                    "run_id": cls_instance["run_id"]
                }
            elif isinstance(cls_instance, DatabaseLog.LogFile):
                where = {
                    "run_id": cls_instance["run_id"],
                    "filename": cls_instance["src_name"]
                }
            elif isinstance(cls_instance, DatabaseLog.LogExitCode):
                where = {
                    "exit_code": cls_instance["exit_code"]
                }
            elif isinstance(cls_instance, DatabaseLog.LogOperation):
                where = {
                    "run_id": cls_instance["run_id"],
                    "operation": cls_instance["operation"]
                }
            self.update(
                schema=self.schema,
                table=self.table_name[type(param).__name__],
                columns=param.keys(),
                tuples=[param.values()],
                where=where
            )

    def get_session(self):
        pass
