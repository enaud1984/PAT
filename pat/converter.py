import json
import copy
from datetime import datetime
import re
import time
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DecimalType

# lista dei tipi gestiti
data_types = [
    "array",
    "binary",
    "bool",
    "byte",
    "date",
    "decimal",
    "double",
    "float",
    "int",
    "long",
    "short",
    "text",
    "timestamp",
    "datetime"
]
data_type_default = "text"  # tipo di dfault se non inserito nello schema

J_NODE = "NODE"
J_LEAF = "LEAF"
J_NODE_ARRAY = "NODE_ARRAY"
J_LEAF_ARRAY = "LEAF_ARRAY"

CAST_ALL_STRING = "CAST_ALL_STRING"
CAST_DATA_TYPE = "CAST_DATA_TYPE"
CAST_CSV = "CAST_CSV"

J_SPECIAL_MANDATORY = "*"
J_SPECIAL_CONCAT_CHILDREN = " union"
J_KEY_DELIMITER = " using '"
J_SPECIAL_HIDDEN = " hidden"
J_SPECIAL_SEPARATOR = " as "
J_SPECIAL_PATH_SEPARATOR = "/"
COMMA_SEPARATOR = ","
J_UNION_DELIMITER = "|"


class JError(Exception):
    pass


# input key_ :
# chiave dello schema nel formato : *+JSON_NAME#NEW_NAME(type|info_type)
# * --> opzionale, implica che il campo e' obbligatorio
# + --> opzionale, unisce i contenuti degli array
# JSON_NAME --> obbligatorio, nome della campo json
# NEW_NAME --> opzionale, nome nuovo che viene inseriti nell' header , se non inserito viene utilizzato JSON_NAME
# type --> opzionale, tipo del dato (sono considerati valisi solo quelli contenuti in data_types)
# info_type --> da definire

# input json_type :
# L : list
# D : dict
# V : value
def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


class JKey:
    full_name: str = None
    real_name: str = None
    new_name: str = None
    key_name: str = None
    json_type = None
    data_type = None
    data_type_detail = None
    isMandatory: bool = False
    isUnion: bool = False
    isForceDefault: bool = False
    column: int = -1
    childrens = []
    delimiter = J_UNION_DELIMITER

    def __init__(self, key_name, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        if key_name is not None:
            self.decode(key_name)
        self.full_name = self.key_name

    def decode(self, key_name):
        key_split = key_name.split(J_SPECIAL_PATH_SEPARATOR)
        key = key_split[-1]
        i_open = key.find("(")  # get the position of delimiter [
        i_close = key.find(")")  # get the position of delimiter ]
        if key.find("::") > 1:
            special = key.split("::")
            d_type = special[1]
            if i_open > 0 and i_close > 0:
                d_type_detail = key[i_open + 1:i_close]
                self.data_type_detail = d_type_detail
                d_type = d_type.split("(")[0]
            self.data_type = d_type
            key = special[0]
        elif i_open > 0 and i_close > 0:
            d_type = key[i_open + 1:i_close]
            if d_type is not None:
                d_type_detail = None
                key = key.replace("(" + d_type + ")", '')

                d_type_split = d_type.split("|")
                if len(d_type_split) == 2:
                    d_type = d_type_split[0]
                    d_type_detail = d_type_split[1]

                if not d_type in data_types:
                    raise ValueError('field type is not valid : {}'.format(d_type))
                self.data_type = d_type
                self.data_type_detail = d_type_detail
        else:
            self.data_type = data_type_default
        if J_KEY_DELIMITER in key:
            special_split = key.split(J_KEY_DELIMITER)
            key = special_split[0]
            self.delimiter = special_split[1].split[","]
        if J_SPECIAL_CONCAT_CHILDREN in key:
            self.isUnion = True
            special_split = key.split(J_SPECIAL_CONCAT_CHILDREN)
            key = special_split[0]
            if self.data_type_detail is None:
                self.data_type_detail = special_split[1]
        if key.endswith(J_SPECIAL_MANDATORY):
            self.isMandatory = True
            key = key[:-1]
        elif key.endswith(J_SPECIAL_HIDDEN):
            self.isForceDefault = True
            key = key.split(J_SPECIAL_HIDDEN)[0]

        # remove
        itemSplit = key.split(J_SPECIAL_SEPARATOR)
        if len(itemSplit) == 1:
            self.real_name = itemSplit[0]
            self.new_name = camel_to_snake(itemSplit[0])
        elif len(itemSplit) == 2:
            self.real_name = itemSplit[0]
            self.new_name = itemSplit[1]

        key_split[-1] = self.real_name
        self.key_name = J_SPECIAL_PATH_SEPARATOR.join(key_split)

    def isValid(self):
        if self.isUnion and self.json_type != J_NODE_ARRAY and self.json_type != J_LEAF_ARRAY:
            raise JError("Use tag + only for list [] : {} is a {}".format(self.full_name, self.json_type))

    def __repr__(self):
        return "full_name:% s real_name:% s new_name:% s key_name:% s json_type:% s data_type:% s data_type_detail:% s isMandatory:% s isUnion:% s" % (
        self.full_name, self.real_name, self.new_name, self.key_name, self.json_type, self.data_type,
        self.data_type_detail, self.isMandatory, self.isUnion)

    def __str__(self):
        ret = f"column:{self.column}\tdata_type:{self.data_type}\treal_name:{self.real_name}\tnew_name:{self.new_name}\tkey_name:{self.key_name}\tjson_type:{self.json_type}\tfull_name:{self.full_name} "
        if self.data_type_detail:
            ret += f"\tdata_type_detail:{self.data_type_detail}\tisMandatory:{self.isMandatory}\tisUnion:{self.isUnion}"
        return ret


class JKeyNew(JKey):

    def decode_new(self, key_name):
        key_split = key_name.split(J_SPECIAL_PATH_SEPARATOR)
        key = key_split[-1]
        key_split_as = key.split(" as ")
        if len(key_split_as) > 1:
            real_name = key_split_as[0]
            new_name = key_split_as[1]
        else:
            new_name = key_name
            real_name = new_name
        key_split_Type = new_name.split(J_SPECIAL_SEPARATOR)
        if len(key_split_Type) > 1:
            new_name = key_split_Type[0]
            type_key = key_split_Type[1].split("(")
            d_type_detail = None
            if len(type_key) > 1:
                d_type = type_key[0]
                d_type_detail = type_key[1].split(")")[0]
            elif len(type_key) > 0:
                d_type = type_key[0]
            else:
                d_type = type_key
            new_name_split = new_name.split(" ")
            if len(new_name_split) > 1:
                new_name = new_name_split[0]
            if not d_type in data_types:
                raise ValueError('field type is not valid : {}'.format(d_type))
            self.data_type = d_type
            self.data_type_detail = d_type_detail
            real_name = key_split_Type[0]
        first_ = real_name[0:3]
        if J_SPECIAL_MANDATORY in first_:
            self.isMandatory = True
            real_name = real_name.replace(J_SPECIAL_MANDATORY, '')
        if J_SPECIAL_CONCAT_CHILDREN in first_:
            self.isUnion = True
            real_name = real_name.replace(J_SPECIAL_CONCAT_CHILDREN, '')
        elif J_SPECIAL_HIDDEN in first_:
            self.isForceDefault = True
            real_name = real_name.replace(J_SPECIAL_HIDDEN, '')
        if real_name.startswith(J_SPECIAL_PATH_SEPARATOR):
            real_name = real_name[1:]
        real_name_split = real_name.split(J_SPECIAL_PATH_SEPARATOR)
        if len(real_name_split) > 0:
            real_name = real_name_split[-1]
            if self.real_name == self.new_name:
                self.real_name = real_name
                self.new_name = camel_to_snake(real_name)
            else:
                self.real_name = real_name
                self.new_name = camel_to_snake(new_name)
        else:
            self.real_name = real_name
            self.new_name = camel_to_snake(new_name)
        self.key_name = f"{J_SPECIAL_PATH_SEPARATOR}{real_name}"


class JsonUtils:
    '''
    obj : oggetto da trasformare
    jKey : info sull'oggetto
    casttype :
        CAST_ALL_STRING : trasforma sempre l'oggetto in stringa
        CAST_DATA_TYPE : trasforma nell'oggetto definito nel jKey
        CAST_CSV : trasforma tutto in stringa tranne i numerici
    '''

    @staticmethod
    def castObj(obj, jKey, casttype=CAST_ALL_STRING):
        if obj is None:
            if casttype == CAST_ALL_STRING:
                return "NULL"
            return None

        if casttype == CAST_ALL_STRING:
            return str(obj)
        elif casttype == CAST_DATA_TYPE:
            if jKey.data_type in ["int", "long"]:
                return int(obj)
            elif jKey.data_type in ["double", "float"]:
                return float(obj)
            elif jKey.data_type == "bytes":
                return bytes(obj)
            elif jKey.data_type == "bool":
                return bool(obj)
            elif jKey.data_type == "timestamp":
                if jKey.data_type_detail is None:
                    return datetime.strptime(obj, '%Y-%m-%dT%H:%M:%S+01:00')
                else:
                    return datetime.strptime(obj, jKey.data_type_detail)
            else:
                return str(obj)
        elif casttype == CAST_CSV:
            if jKey.data_type == "int":
                return int(obj)
            elif jKey.data_type == "long":
                return int(obj)
            elif jKey.data_type == "double":
                return float(obj)
            elif jKey.data_type == "float":
                return float(obj)
            else:
                return str(obj)

    @staticmethod
    def loads(content):
        if not isinstance(content, dict):
            return json.loads(content)
        return content

    @staticmethod
    def getCsvObj(obj):
        if obj is None:
            obj = "NULL"
        elif isinstance(obj, str):
            obj = obj.replace('"', '""')
            obj = '"' + obj + '"'
        else:
            obj = str(obj)
        return obj

    ################################################
    #######      Static Public Methods       #######
    ################################################
    @staticmethod
    def dataToStr(csvData, separator=',', quoteFields="smart", lineSeparator='\r\n'):
        if not isinstance(csvData, list) or not isinstance(csvData[0], list):
            raise ValueError(
                'csvData is not a list-of-lists. dataToStr is meant to convert the return of "extractData" method to csv data.')

        lines = [separator.join([JsonUtils.getCsvObj(item) for item in items]) for items in csvData]
        # lines = [separator.join(['"%s"' % (str(item).replace('"', '""'),) for item in items]) for items in csvData]

        return lineSeparator.join(lines)


class JsonConverter:
    schema = None
    shema_detail = {}
    header_detail = {}
    real_field_names = {}
    new_field_names = {}
    sorted_fields = []
    fields_of_union = {}
    fields_ignored = {}

    last_position = 0
    J_NULL_VALUE = None  # il valore che assume il campo Stringa null
    # J_NULL_VALUE = 'NULL'
    old_mode: bool = True
    raise_on_error: bool = True

    def __init__(self, schema, *args, **kwargs):
        # Schema iniziale
        self.init_schema = schema
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.shema_detail = self.get_schema_detail(copy.deepcopy(schema))
        # self.setSchemaDetail(copy.deepcopy(schema))
        self.schema = self.get_schema(copy.deepcopy(schema))
        self.header_detail = self.get_header_detail(copy.deepcopy(self.schema))

        print("self.init_schema == {}".format(self.init_schema))
        print("self.schema == {}".format(self.schema))
        print("self.header_detail == {}".format(self.header_detail))
        print("self.shema_detail == {}".format(self.shema_detail))
        check, field_names = self.check_schema()
        if len(check) > 0:
            print(f"Warning columns duplicated {check} field_names:{field_names}")

    root_name: str = None

    def get_schema_detail(self, json_schema, key=None):
        shema_detail_local = {}
        if self.old_mode:
            keyClass = JKey
        else:
            keyClass = JKeyNew
        jKey = keyClass(key)
        if key is not None:
            shema_detail_local[jKey.key_name] = jKey
            key = jKey.key_name
            if self.root_name is None:
                self.root_name = key.replace(J_SPECIAL_PATH_SEPARATOR, "")
        else:
            jKey.key_name = ""
            key = ""

        if type(json_schema) is dict:

            jKey.json_type = J_NODE
            shema_detail_local[jKey.key_name] = jKey
            for name, v in sorted(json_schema.items()):
                shema_detail_in = self.get_schema_detail(json_schema[name], key + J_SPECIAL_PATH_SEPARATOR + name)
                shema_detail_local.update(shema_detail_in)
        elif type(json_schema) is list:
            jKey.json_type = J_NODE_ARRAY
            shema_detail_local[jKey.key_name] = jKey
            for name in json_schema:
                shema_detail_in = self.get_schema_detail(json_schema[0], key + J_SPECIAL_PATH_SEPARATOR + "@")
                shema_detail_local.update(shema_detail_in)
                if name is None:
                    jKey.json_type = J_LEAF_ARRAY
                    shema_detail_local[jKey.key_name] = jKey
        else:
            jKey.json_type = J_LEAF
            shema_detail_local[jKey.key_name] = jKey

        jKey.isValid()
        return shema_detail_local

    '''
    def setSchemaDetail(self,json_schema,key=None):
        if key is not None :
            jKey = JKey(key)
            self.shema_detail[jKey.key_name]=jKey
            key = jKey.key_name
        else :
            jKey = JKey(None)
            jKey.key_name = ""
            key = ""

        if type(json_schema) is dict:
            jKey.json_type = J_NODE
            self.shema_detail[jKey.key_name]=jKey
            for name, v in sorted(json_schema.items()):
                self.setSchemaDetail(json_schema[name],key + "/" + name)
        elif type(json_schema) is list:
            jKey.json_type = J_NODE_ARRAY
            self.shema_detail[jKey.key_name]=jKey
            for name in json_schema:
                self.setSchemaDetail(json_schema[0],key + "/" + "@")
                if name is None :
                    jKey.json_type = J_LEAF_ARRAY
                    self.shema_detail[jKey.key_name]=jKey
        else :
            jKey.json_type = J_LEAF
            self.shema_detail[jKey.key_name] = jKey

        jKey.isValid()
    '''

    '''
    restituisce una riga con l'header estratto dallo schema
    '''

    def get_header_detail(self, json_schema, key=None):
        if key is not None:
            jKey = self.shema_detail[key]
            # jKey = JKey(key)
            key = jKey.key_name
        else:
            key = ""
            jKey = JKey(None)

        current_line = []

        if jKey.isUnion:
            current_line = [jKey]
        elif type(json_schema) is dict:
            # for name in json_schema:
            for name, v in sorted(json_schema.items()):
                inner_line = self.get_header_detail(json_schema[name], key + J_SPECIAL_PATH_SEPARATOR + name)
                current_line = current_line + inner_line
        elif type(json_schema) is list:
            i = 0
            # for name in sorted(json_schema) :
            for name in json_schema:
                inner_line = self.get_header_detail(json_schema[0], key + J_SPECIAL_PATH_SEPARATOR + "@")
                if name is None:
                    inner_line = [jKey]

                current_line = inner_line
        else:
            current_line = [jKey]
        return current_line

    '''
    Restituisce una lista con gli header
    '''

    def get_header(self):
        # header = [hd.new_name for hd in self.header_detail if hd.real_name not in self.fields_of_union]
        header = [hd.new_name for hd in self.header_detail]
        return header

    def get_header_types(self):
        ret = {}
        for hd in self.header_detail:
            if hd.real_name in self.fields_of_union:
                continue
            name = hd.new_name.replace(".", '_').replace("-", '_')
            if hd.data_type == 'text':
                value_type = str
            elif hd.data_type == 'timestamp':
                value_type = datetime
            else:
                value_type = eval(hd.data_type)
            ret[name] = value_type
        return ret

    '''
    Crea un nuovo schema con le chiavi pulite dai tag di detail
    '''

    def get_schema(self, iterable):
        if type(iterable) is dict:
            new_ = {}
            for key in iterable.keys():
                jKey = JKey(key)
                new_name = jKey.real_name
                if type(iterable[key]) is dict or type(iterable[key]) is list:
                    new_[new_name] = self.get_schema(iterable[key])
                else:
                    new_[new_name] = iterable[key]
        elif type(iterable) is list:
            new_ = []
            for item in iterable:
                new_.append(self.get_schema(item))
        else:
            new_ = iterable
        return new_

    '''
    Crea un nuovo schema con le chiavi pulite dai tag di detail
    '''

    def get_schema_clean(self, iterable):
        if type(iterable) is dict:
            for key in iterable.keys():
                jKey = JKey(key)
                # new_name = key.lower()
                new_name = jKey.real_name
                iterable[new_name] = iterable.pop(key)
                if type(iterable[new_name]) is dict or type(iterable[new_name]) is list:
                    iterable[new_name] = self.get_schema(iterable[new_name])
        elif type(iterable) is list:
            for i in range(len(iterable)):
                item = self.get_schema(iterable[i])
                iterable[i] = item
        return iterable

    def add_field(self, jKey):
        name = str(jKey.new_name)
        if (jKey.json_type not in [J_NODE_ARRAY, J_NODE] or (
                jKey.json_type == J_NODE_ARRAY and jKey.isUnion)) and name not in self.new_field_names:
            if name != "@":
                if jKey.childrens and len(jKey.childrens) > 0:
                    fields_of_union = [k for k in jKey.childrens if k not in self.fields_of_union]
                    map_fields_of_union = {k: self.new_field_names.get(k) for k in fields_of_union}
                    if len(map_fields_of_union) > 0:
                        self.fields_of_union.update(map_fields_of_union)
                    if self.last_position > 0:
                        self.last_position -= len(jKey.childrens)
                jKey.column = self.last_position
                self.new_field_names[name] = jKey
                self.real_field_names[jKey.real_name] = jKey
                self.last_position += 1

    # collassa un oggetto in una singola stringa
    # TODO gestire oggeti complessi
    # funziona solo con liste di stringhe
    def __list_to_str(self, jKey, obj, level=0):
        union_str = ""
        obj_str = ""
        if type(obj) is list:
            # for x in obj:
            for idx, x in enumerate(obj):
                if idx != 0:
                    delim = jKey.delimiter
                else:
                    delim = ""

                obj_str = obj_str + delim + self.__list_to_str(jKey, x, level + 1)


        else:
            obj_str = obj_str + str(obj)

        return obj_str

    # cast_obj = True.  Lista disomogenea : nella lista degli oggetti restituiti ogni oggetto avra' il tipo identificato nello schema (int) (double) ecc
    # cast_obj = False. Lista omogenea : nella lista degli oggetti restituiti ogni oggetto sara' di tipo String
    def __getData(self, key, json_obj, json_schema, casttype, level: int = 0):
        data_list = []
        if key == "":
            self.new_field_names = {}
            self.last_position = 0
            self.map_values = {}
        if type(json_schema) is dict:

            # for name in json_schema:
            for name, v in sorted(json_schema.items()):
                jKey = self.shema_detail[key + "/" + name]
                if (name not in json_obj) or (json_obj[name] is None):
                    if jKey.isMandatory:
                        raise JError("Error_1 : {} is mandatory".format(jKey.key_name))
                    else:
                        # valore di default inserito nel json_schema
                        json_obj[name] = json_schema[name]
                        self.add_field(jKey)
                if jKey.isForceDefault:
                    # valore di default inserito nel json_schema
                    json_obj[name] = json_schema[name]
                    self.add_field(jKey)

                inner_list = self.__getData(key + J_SPECIAL_PATH_SEPARATOR + name, json_obj[name], json_schema[name],
                                            casttype, level + 1)
                if len(data_list) == 0:
                    data_list = inner_list
                else:
                    lines_new = []
                    for data_list_obj in data_list:
                        for inner_list_obj in inner_list:
                            if type(inner_list_obj) != bool:
                                lines_new.append(data_list_obj + inner_list_obj)
                    data_list = lines_new
        elif type(json_schema) is list:
            i = 0
            jKey = self.shema_detail[key]
            for obj in json_obj:
                jKey.childrens = json_schema[0]
                inner_list = self.__getData(key + f"{J_SPECIAL_PATH_SEPARATOR}@", obj, json_schema[0], casttype,
                                            level + 1)

                if len(data_list) == 0:
                    # se isUnion = True agrego tutti i risultati in uno solo
                    if jKey.isUnion:
                        union_str = self.__list_to_str(jKey, inner_list)
                        data_list = [[union_str]]
                    else:
                        data_list = inner_list
                else:
                    for inner_list_obj in inner_list:
                        if jKey.isUnion:
                            # data_list[0] = [str(data_list[0][0]) + J_UNION_DELIMITER +  str(inner_list_obj[0])]
                            union_str = str(data_list[0][0]) + "#" + self.__list_to_str(jKey, inner_list)
                            data_list = [[union_str]]
                            self.add_field(jKey)
                        else:
                            data_list.append(inner_list_obj)
                            if jKey.json_type == J_LEAF:
                                self.add_field(jKey)
        else:
            jKey = self.shema_detail[key]
            if json_obj is None and jKey.isMandatory:
                raise JError("Error_2 : {} is mandatory".format(jKey.key_name))
            else:
                obj = JsonUtils.castObj(json_obj, jKey, casttype)
                data_list.append([obj])
                self.map_values[jKey.new_name] = obj
                self.add_field(jKey)
        return data_list

    def __getDataJson(self, key, json_obj, json_schema, casttype):
        data_json = {}
        if type(json_schema) is dict:
            # for name in json_schema:
            for name, v in sorted(json_schema.items()):
                jKey = self.shema_detail[key + J_SPECIAL_PATH_SEPARATOR + name]
                if (name not in json_obj) or (json_obj[name] is None):
                    if jKey.isMandatory:
                        raise JError("Error_1 : {} is mandatory".format(jKey.key_name))
                    else:
                        # valore di default inserito nel json_schema
                        json_obj[name] = json_schema[name]

                if jKey.isForceDefault:
                    # valore di default inserito nel json_schema
                    data_json[name] = json_schema[name]
                else:
                    data_json[name] = self.__getDataJson(key + J_SPECIAL_PATH_SEPARATOR + name, json_obj[name],
                                                         json_schema[name], casttype)

        elif type(json_schema) is list:
            i = 0
            data_json = []
            for a in json_obj:
                data_json_inner = self.__getDataJson(key + f"{J_SPECIAL_PATH_SEPARATOR}@", a, json_schema[0], casttype)
                data_json.append(data_json_inner)
        else:
            jKey = self.shema_detail[key]
            if json_obj is None and jKey.isMandatory:
                raise JError("Error_2 : {} is mandatory".format(jKey.key_name))
            else:
                obj = JsonUtils.castObj(json_obj, jKey, casttype)
                data_json = obj

        return data_json

    def get_spark_schema(self, json_schema=None, key=None):
        # print(f"key == {key} - json_schema = {json_schema}")
        """
        restituisce una riga con l'header estratto dallo schema
        """
        if key is not None:
            jKey = self.shema_detail[key]
            key = jKey.key_name
        else:
            json_schema = self.schema
            key = ""
            jKey = JKey(None)

        if type(json_schema) is dict:
            fields = []
            for name, v in sorted(json_schema.items()):
                spark_ = self.getSparkSchema(json_schema[name], key + "/" + name)
                if type(spark_) is StructType:
                    for i in range(len(spark_)):
                        fields.append(spark_[i])
                else:
                    fields.append(spark_)
            return StructType(fields)
        elif type(json_schema) is list:
            # TODO
            return self.getSparkSchema(json_schema[0], key + "/" + "@")
        else:
            if jKey.data_type == 'int':
                return StructField(jKey.new_name, IntegerType(), True)
            elif jKey.data_type == 'double':
                return StructField(jKey.new_name, DoubleType(), True)
            elif jKey.data_type == 'timestamp':
                return StructField(jKey.new_name, TimestampType(), True)
            elif jKey.data_type == 'decimal':
                return StructField(jKey.new_name, DecimalType(), True)
            else:
                return StructField(jKey.new_name, StringType(), True)

        return current_line

    def to_json(self, data, header=True):
        obj = JsonUtils.loads(data)
        return self.__getDataJson("", obj, self.schema, CAST_DATA_TYPE)

    def to_list(self, data, header=True):
        obj = JsonUtils.loads(data)

        ret = self.__getData("", obj, self.schema, CAST_DATA_TYPE)
        if header:
            ret.insert(0, self.get_header())
        return ret

    def check_schema(self):
        diff = []
        map_count = {}
        field_names = self.get_header()
        header = self.get_header_types()
        new_list = []
        for field_name in field_names:
            if field_name not in header:
                diff.append(field_name)
            if field_name not in map_count:
                map_count[field_name] = 0
                new_list.append(field_name)

            map_count[field_name] += 1
        dupl = [k for k, v in map_count.items() if v > 1]
        return dupl + diff, new_list

    """
        method to load datas
        CAST_DATA_TYPE = default
        CAST_ALL_STRING = for pyspark
        CAST_CSV = for csv output
    """

    def load_data(self, data, type_of_load=CAST_DATA_TYPE):
        obj = JsonUtils.loads(data)
        self.data = self.__getData("", obj, self.schema, type_of_load)
        list_index = {jKey.column: jKey for k, jKey in self.real_field_names.items()}
        i = 0
        self.sorted_fields = [list_index[k] for k in sorted(list_index)]
        for k in sorted(list_index):
            v = list_index[k]
            # if k != i:
            # v.column = i
            i += 1
        diff = [jKey.column for k, jKey in self.real_field_names.items() if
                jKey.column not in range(len(self.real_field_names.items()))]
        if len(diff) > 0:
            print("manca qualcosa")

    def to_object(self):
        data = self.data
        field_names = self.get_header_types()
        class_name = f"{self.root_name[0].upper()}{self.root_name[1:]}"
        class_obj = namedtuple(class_name, field_names=field_names)
        ret = []
        for l in data:
            values = {}
            for f in field_names:
                if f in self.new_field_names and self.new_field_names[f].column < len(l):
                    values[f] = l[self.new_field_names[f].column]
                elif f in self.new_field_names:
                    self.fields_ignored[f] = "Ignored"
                else:
                    self.fields_ignored[f] = "Error"
                    # print(f"Errore field non trovato {f}")
            if len(values) < len(field_names):
                for f in field_names:
                    if f not in values:
                        values[f] = None
            ret.append(class_obj(**values))
        return ret

    def to_csv(self, quoteFields="smart", lineSeparator='\r\n', separator=',', header=True):
        data = self.data
        if header:
            data.insert(0, self.get_header())
        return JsonUtils.dataToStr(data, separator=separator, quoteFields=quoteFields, lineSeparator=lineSeparator)

    def to_dataframe(self, sc):
        from pyspark import SparkConf, SparkContext, Row
        from pyspark.sql import HiveContext, DataFrame, SparkSession, SQLContext
        data = self.data
        data.insert(0, self.get_header())

        '''
        f_list = []
        for hd in self.header_detail:
            f_list.append(hd.new_name)
        R = Row(*f_list)
        R = Row(*self.getHeader())
        '''

        R = Row(*data.pop(0))
        df = sc.parallelize([R(*r) for r in data]).toDF()

        for hd in self.header_detail:
            # if hd.data_type is not 'string' :
            if type(hd.data_type) is not str:
                df = df.withColumn(hd.new_name, df[hd.new_name].cast(hd.data_type))

        return df

    def to_dataframe_from_list(self, data, sc):
        from pyspark import SparkConf, SparkContext, Row
        from pyspark.sql import HiveContext, DataFrame, SparkSession, SQLContext
        data.insert(0, self.get_header())

        print("list_ == {}".format(data))

        R = Row(*data.pop(0))
        df = sc.parallelize([R(*r) for r in data]).toDF(sampleRatio=0.2)

        for hd in self.header_detail:
            # if hd.data_type is not 'string' :
            if type(hd.data_type) is not str:
                df = df.withColumn(hd.new_name, df[hd.new_name].cast(hd.data_type))
        return df


#########################################
##########         CSV         ##########
#########################################

import csv
from io import StringIO


class CsvConverter:

    def __init__(self, schema):
        self.init_schema = schema

    def toList(self, data_, header=True, csv_header=True):
        header_list = []
        for key in self.init_schema:
            header_list.append(key)

        data = StringIO(data_)
        if csv_header:
            csv_reader = csv.DictReader(data, delimiter=',')
        else:
            csv_reader = csv.DictReader(data, delimiter=',', fieldnames=header_list)

        line_count = 0
        csv_content = []
        for row in csv_reader:
            csv_line = []
            for key in self.init_schema:
                if key in row:
                    csv_line.append(row[key])
                else:
                    csv_line.append(None)
            csv_content.append(csv_line)

        if header:
            csv_content.insert(0, header_list)

        return csv_content
