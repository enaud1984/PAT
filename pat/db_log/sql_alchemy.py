#import logging
import re
import base64
#from collections import namedtuple
from typing import Union
import sqlalchemy
from sqlalchemy import Column, Integer, String, REAL, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
#from sqlalchemy.engine.url import URL
#from sqlalchemy.orm.session import object_session

'''
Implementazione del db_log utilizzando la libreria sqlalchemy
per abilitarla modificare il valore in env.py :
  db_log_library = "sql_alchemy"
'''
class DatabaseLog:
    engine = None
    session = None
    Base = declarative_base()

    class Log(Base):
        __tablename__ = 'log'
        run_id = Column(String(128), primary_key=True)
        script = Column(String(64))
        description = Column(String(4000))
        execution_time = Column(REAL)
        success = Column(Integer)
        start_time = Column(DateTime)
        end_time = Column(DateTime)
        expiration_date = Column(DateTime)
        exit_code = Column(Integer)
        app_id = Column(String(255))
        log_path = Column(String(500))
        message = Column(String)

    class LogExitCode(Base):
        __tablename__ = 'log_exit_code'
        exit_code = Column(Integer, primary_key=True, autoincrement=True)
        type = Column(String(200))
        module = Column(String(200))

    class LogFile(Base):
        __tablename__ = 'log_file'
        run_id = Column(String(128), primary_key=True)
        src_name = Column(String(200))
        dst_name = Column(String(1000))
        exit_code = Column(Integer)

    class LogHDFS(Base):
        __tablename__ = 'log_hdfs'
        run_id = Column(String(128), primary_key=True)
        file_hdfs = Column(String(300))

    class LogOperation(Base):
        __tablename__ = 'log_operation'
        run_id = Column(String(128), primary_key=True)
        operation = Column(String(64), primary_key=True)
        start_time = Column(DateTime)
        end_time = Column(DateTime)
        exit_code = Column(Integer)

    def __init__(self, params):
        match = re.search(r"(.*):\/\/(.*):(.*)\/(.*)", params.get("url"))

        decode_password = params.get("decode_password", False)

        dict_params = {"username": params.get("user"),
                       "password": base64.b64decode(params.get("password")).decode(
                           "utf-8") if decode_password else params.get("password"),
                       "drivername": match.group(1),
                       "host": match.group(2),
                       "port": int(match.group(3)),
                       "database": match.group(4)
                       }
        db_connection_url = sqlalchemy.engine.url.URL(**dict_params)
        self.engine = create_engine(db_connection_url,
                                    connect_args={'options': '-csearch_path={}'.format(params.get('schema', ""))})
        Session = sessionmaker(self.engine, autocommit=False, expire_on_commit=False)
        self.session = Session()
        self.create_tables()

    def create_tables(self):
        self.Base.metadata.create_all(self.engine)

    def insert_log(self, log_row: Union[Log, LogExitCode, LogFile, LogHDFS, LogOperation]):
        session = self.session
        session.add(log_row)
        session.commit()

    def get_item(self, class_name, pk_value):
        return self.session.query(class_name).filter_by(run_id=pk_value).one()

    def get_exit_code(self, type=None, module=None):
        log_exit_code = self.session.query(DatabaseLog.LogExitCode).filter(
            DatabaseLog.LogExitCode.type == type).filter(
            DatabaseLog.LogExitCode.module == module).first()
        if log_exit_code is None :
            return None
        else :
            return log_exit_code.exit_code

    def update_session(self, cls_instance=None):
        session = self.session
        session.commit()

    def get_session(self):
        return self.session