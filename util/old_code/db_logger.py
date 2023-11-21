import json
#from jpype import java
from common.db_utility import DbLog
from collections import namedtuple



with open('../conf.json') as json_file:
    config = json.load(json_file, object_hook=lambda d: namedtuple('Config', d.keys())(*d.values()))
l = DbLog(config.db_log)


run_id_ = "12345678"
l.insert(
    run_id=run_id_,
    script="ppp"

)
l.update(
    run_id=run_id_,
    script="ppp5555"
)
