from enum import Enum


class DbmsType(str, Enum):
    """
    Tipi DB gestiti
    """
    ORACLE = "ORACLE"
    POSTGRES = "POSTGRES"
    IMPALA = "IMPALA"


class Base:
    """
    classe Base
    """

    pass


class BaseResponse(Base):
    __slots__ = ("exit_code",)

    def __init__(self):
        self.exit_code = 0


class Source(Base):
    __slots__ = ("id_source", "data", "dst_name", "src_name")

    def __init__(self):
        self.id_source = None
        self.data = []
        self.dst_name = []
        self.src_name = []


class Storable(Base):
    __slots__ = ("id_source", "id_storable", "data")

    def __init__(self):
        self.id_source = None
        self.id_storable = None
        self.data = []


class ConnectorRead(Base):
    __slots__ = ("filename", "data")

    def __init__(self):
        self.data = []
        self.filename = []
