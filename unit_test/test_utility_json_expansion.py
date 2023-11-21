import unittest
import json
from pat.utility import Json
from freezegun import freeze_time


class UtilityTest(unittest.TestCase):

    @freeze_time("2022-05-04 12:00:00")
    def test_json_template_simple(self):
        with open("file/json_expansion_1.json") as json_file:
            loaded_dict = json.load(json_file)
        ret = Json.expand_templates(loaded_dict)
        assert ret.get("url_expand") == "test_2022_05_04_11_00_00"

    @freeze_time("2022-05-04 12:00:00")
    def test_json_template_list(self):
        with open("file/json_expansion_2.json") as json_file:
            loaded_dict = json.load(json_file)
        ret = Json.expand_templates(loaded_dict)
        assert ret.get("value_list")[1] == "test_2022_05_03_12_00_00"
