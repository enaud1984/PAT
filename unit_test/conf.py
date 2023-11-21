######### DATA ACQUISITION READ #########
conf_data_acquisition_read = {
    "option": {
        "id_source": "hdfs",
        "url": "file://file/data_acquisition_read.json",
        "param": {
            "type": "FILE"
        }
    }
}

conf_data_acquisition_write = {
    "id_source": "hdfs",
    "url": "file://hdfs/",
    "param": {
        "source_delete": "N",
        "type": "FOLDER"
    }
}
