import os
import tempfile


#db_log_library = "jaydebeapi"
db_log_library = "sql_alchemy"

def get_log_path(log_id):
    if not os.getenv("local_log"):
        return os.path.join(tempfile.gettempdir(), f"{log_id}.log")
    else:
        return os.getenv("local_log")


def get_log_handle_exception():
    if not os.getenv("log_handle"):
        return os.path.join(tempfile.gettempdir(), f"handle_exception.log")
    else:
        return os.getenv("log_handle")
