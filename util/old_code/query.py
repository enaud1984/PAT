from datetime import datetime
import numbers

class SqlBuilder:
    @staticmethod
    def toInsert (table, columns, tuples):
        fields = ','.join(map(str, columns))
        question = ','.join(['?'] * len(columns))
        query = f"insert into {table} ({fields}) values ({question})"
        queries = []

        for t in tuples :
            q = query
            for v in t :
                q = q.replace("?",SqlBuilder.toValue(v),1)
            print(q)
            queries.append(q)
        return queries


    @staticmethod
    def toUpdate (table, columns, tuples, where=[], ignore_null = False):
        for t in tuples :
            update_set = []
            update_where = []
            for index in range(len(t)):
                if columns[index] in where:
                    update_where.append(SqlBuilder.toValue2(columns[index],t[index]))
                elif t[index] is not None or ignore_null :
                    update_set.append(SqlBuilder.toValue2(columns[index],t[index]))

            upd = ",".join(update_set)
            whr = " and ".join(update_where)
            query = f"update {table} set {upd} where {whr}"
            print(query)
            #q = q.replace("?",SqlBuilder.toValue(v),1)
            #print(q)
            #queries.append(q)
        return "queries"


    @staticmethod
    def toValue2 (k, v) :
        if v is None :
            return f"{k}=null"
        if isinstance(v, numbers.Number) :
            return f"{k}={v}"
        if isinstance(v, datetime) :
            dt_ = datetime.strftime(v, format="%Y-%m-%d %H:%M:%S")
            return f"{k}=to_timestamp('{dt_}', 'yyyy-MM-dd HH24:mi:ss')"
        return f"{k}='{v}'"


    @staticmethod
    def toValue (v) :
        if v is None :
            return "null"
        if isinstance(v, numbers.Number) :
            return str(v)
        if isinstance(v, datetime) :
            dt_ = datetime.strftime(v, format="%Y-%m-%d %H:%M:%S")
            return "to_timestamp('{}', 'yyyy-MM-dd HH24:mi:ss')".format(dt_)
        return f"'{v}'"

data = [
    (60, "Parent 60", None, datetime.now()),
    (70, "Parent 70", 22, datetime.now()),
    (80, None, 22, datetime.now()),
    (90, "Parent 90", 22, datetime.now()),
    (100, "Parent 100", 22, datetime.now())
]

table = "log"
columns = ["a","b","c","d"]

SqlBuilder.toUpdate(table,columns,data,["a","b"])



