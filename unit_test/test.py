import os
import unittest

import mockfs

from pat.chain_of_responsability import BaseRing, DataAcquisitionRead, MemoryMap, DataAcquisitionWrite
from pat.class_tool import Source
from unit_test.common import DbLog
from unit_test.conf import *
import json


class TestDataAcquisition(unittest.TestCase):
    responseRead = None

    @classmethod
    def setUpClass(cls):
        print("setUp - INIT")
        cls.db_log = DbLog()
        cls.id_transaction = "001"
        cls.option = conf_data_acquisition_read["option"]
        print("setUp - END")
        MemoryMap.hset(cls.id_transaction, "db_log", cls.db_log)

    def test_data_acquisition_read(self):
        print("test_data_acquisition_read - INIT")
        data_acquisition_read = DataAcquisitionRead(self.option, self.id_transaction)

        response = data_acquisition_read.run(None)
        self.assertIsNotNone(response[0].data())
        self.assertIsInstance(response[0].data()[0], bytes)
        self.assertEqual(json.loads(response[0].data()[0].decode('utf-8'))["op"], "assignment")
        self.__class__.responseRead = response
        print("test_data_acquisition_read - END")

    # @unittest.skipIf(self.responseRead is None, "TestDataAcquisitionRead response is None")
    def test_data_acquisition_write(self):
        self.option = conf_data_acquisition_write
        data_acquisition_write = DataAcquisitionWrite(self.option, self.id_transaction)

        response = data_acquisition_write.run(self.__class__.responseRead)

        self.assertEqual(response, self.__class__.responseRead)
        self.assertEqual(True, all(isinstance(item, Source) for item in response), "Tutti elementi di tipo Source")


'''
def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDataAcquisitionRead('test_acquisition_read'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner
    runner.run()
'''
