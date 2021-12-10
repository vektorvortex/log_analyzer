#!/usr/bin/env python3

from collections import namedtuple, defaultdict
import datetime
import json
import log_analyzer as la
import os
import unittest


class Namespace:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class LogAnalyzerTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.default_config = la.process_config(la.process_args())
        LogFileEntry = namedtuple('LogFileEntry',
                                  {'path': '',
                                   'name': '',
                                   'ext': '',
                                   'date': ''
                                   })
        self.testEntity = LogFileEntry(
            path='./tmp/nginx-access-ui.log-20211207',
            name='nginx-access-ui.log-20211207',
            ext=None,
            date=datetime.datetime.strptime('20211207', '%Y%m%d').date()
        )

        self.test_raw_data = defaultdict(list)
        self.test_raw_data['/test'].append(0.01)
        self.test_raw_data['/test'].append(0.02)
        self.test_raw_data['/test'].append(0.03)

        self.test_dir = './tmp/'
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)

    @classmethod
    def tearDownClass(self):

        for file in os.listdir(self.test_dir):
            os.remove(self.test_dir + file)
        os.rmdir(self.test_dir)

    def test_process_config(self):
        self.assertEqual(la.process_config(
            la.process_args()), self.default_config)

    def test_process_config_when_config_missing(self):

        args = la.process_args()
        args.__setattr__('config', 'file.json')

        with self.assertRaises(FileNotFoundError):
            la.process_config(args)

    def test_load_config(self):
        test_fname = self.test_dir + 'tmp.json'
        with open(test_fname, 'w') as f:
            f.write(json.dumps(self.default_config, indent=2))

        self.assertEqual(la.load_config(test_fname), self.default_config)

    def test_load_config_when_config_broken(self):
        test_fname = self.test_dir + 'incorrect.json'
        open(test_fname, 'w').close()

        self.assertEqual(la.load_config(test_fname), None)

    def test_find_latest_log(self):

        open(self.test_dir + 'nginx-access-ui.log-20211205.gz', 'w').close()
        open(self.test_dir + 'nginx-access-ui.log-20211206', 'w').close()
        open(self.test_dir + 'nginx-access-ui.log-20211207', 'w').close()
        open(self.test_dir + 'nginx-access-ui.log-20211207.bz2', 'w').close()

        self.assertEqual(la.find_latest_log(self.test_dir), self.testEntity)

    def test_find_latest_log_when_no_logs(self):

        test_dir = './empty_test_folder'
        os.mkdir(test_dir)
        self.assertEqual(la.find_latest_log(test_dir), None)
        os.rmdir(test_dir)

    def test_is_report_exsist(self):

        test_report_name = 'report-2021.12.05.html'

        open(self.test_dir + test_report_name, 'w').close()
        testdate = datetime.date(2021, 12, 5)

        self.assertEqual(la.is_report_exsist(self.test_dir, testdate), True)
        os.remove(self.test_dir + test_report_name)
        self.assertEqual(la.is_report_exsist(self.test_dir, testdate), False)

    def test_process_log(self):

        test_data = [
            '192.168.1.1 -  - [30/Nov/2021:05:41:23 +0300] '
            '"GET /test HTTP/1.1" 200 9134 "-" "-" "-" "-" "-" 0.01',
            '192.168.1.1 -  - [30/Nov/2021:05:41:23 +0300] '
            '"GET /test HTTP/1.1" 200 9134 "-" "-" "-" "-" "-" 0.02',
            '192.168.1.1 -  - [30/Nov/2021:05:41:23 +0300] '
            '"GET /test HTTP/1.1" 200 9134 "-" "-" "-" "-" "-" 0.03']

        with open(self.test_dir + 'nginx-access-ui.log-20211207', 'w') as f:
            for item in test_data:
                f.write(item + '\n')

        self.assertEqual(la.process_log(self.testEntity, 1),
                         self.test_raw_data)

    def test_process_log_when_max_errors_exceeded(self):

        test_data = [
            '192.168.1.1 -  - [30/Nov/2021:05:41:23 +0300] '
            '"GET __ HTTP/1.1" 200 9134 "-" "-" "-" "-" "-" ___',
            '192.168.1.1 -  - [30/Nov/2021:05:41:23 +0300] '
            '"GET /test HTTP/1.1" 200 9134 "-" "-" "-" "-" "-" 0.02',
            '192.168.1.1 -  - [30/Nov/2021:05:41:23 +0300] '
            '"GET /test HTTP/1.1" 200 9134 "-" "-" "-" "-" "-" 0.03']

        with open(self.test_dir + 'nginx-access-ui.log-20211207', 'w') as f:
            for item in test_data:
                f.write(item + '\n')

        self.assertEqual(la.process_log(self.testEntity, 1),
                         None)

    def test_process_raw(self):

        excpected_output = [{'count': 3,
                            'count_perc': 100.0,
                             'time_avg': 0.02,
                             'time_max': 0.03,
                             'time_med': 0.02,
                             'time_perc': 100.0,
                             'time_sum': 0.06,
                             'url': '/test'}]

        self.assertEqual(la.process_raw(self.test_raw_data), excpected_output)


if __name__ == '__main__':
    unittest.main(verbosity=2)
