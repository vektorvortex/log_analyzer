#!/usr/bin/env python3

import argparse
from collections import namedtuple, defaultdict
import datetime
import json
import gzip
import logging
import os
import re
from statistics import median
from string import Template
import sys

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}

DEFAULT_CONFIG = './config.json'
REPORT_TEMPLATE = './reports/report.html'


def process_args():
    '''Process command line arguments'''

    parser = argparse.ArgumentParser(description='Parser for nginx logs')
    parser.add_argument("--config",
                        dest="config",
                        default=DEFAULT_CONFIG,
                        help='path to config file')
    args = parser.parse_args()
    return args


def process_config(args, default=config):
    '''Join default & custom config'''

    if not os.path.exists(args.config):
        raise FileNotFoundError('Config file is not exsist!')
    parsed_config = load_config(args.config)
    if not parsed_config:
        raise IOError("Error parsing config file!")

    return {**default, **parsed_config}


def load_config(path_to_config):
    '''Loading config from file'''

    try:
        with open(path_to_config) as f:
            config_file = json.load(f)
        return config_file
    except (ValueError, TypeError):
        return None


def find_latest_log(log_dir):
    ''' Find latest log file '''

    LogFileEntry = namedtuple('LogFileEntry',
                              {'path': '',
                               'name': '',
                               'ext': '',
                               'date': ''
                               })

    logging.info('Searching latest log file in "{}"'.format(log_dir))
    pattern = re.compile(r'^nginx-access-ui\.log-(\d{8})(\.gz)?$')
    min_date = datetime.datetime.min.date()
    found_log = None

    for file in os.listdir(log_dir):
        match = pattern.search(file)
        if match:
            f_date = datetime.datetime.strptime(
                match.group(1), '%Y%m%d').date()
            f_ext = match.group(2)
            if f_date > min_date:
                min_date = f_date
                found_log = LogFileEntry(
                    path=os.path.join(log_dir, file),
                    name=file,
                    ext=f_ext,
                    date=f_date)
    return found_log


def log_reader(log_filename, log_extension):
    '''Generator-based reader for log files'''

    log = (gzip.open(log_filename, 'rt')
           if log_extension == ".gz" else open(log_filename))
    for line in log:
        if line:
            yield line
    log.close()


def is_report_exsist(report_dir, lastdate):
    '''Check if report already exsist'''

    report_name = 'report-{}.html'.format(lastdate.strftime('%Y.%m.%d'))
    report_path = os.path.join(report_dir, report_name)
    if os.path.exists(report_path):
        return True
    else:
        return False


def process_log(log_file_entry, max_errors_percent):
    '''Parse log file and return raw data for calculations'''

    time_regexp = r'\d+\.\d+$'
    url_regexp = r'\B(?:/(?:[\w?=_&-]+))+'

    raw_data = defaultdict(list)

    lines_count = 0
    lines_parsed = 0

    for line in log_reader(log_file_entry.path, log_file_entry.ext):
        url = (re.findall(url_regexp, line)[0]
               if re.findall(url_regexp, line) else None)
        time = (re.findall(time_regexp, line)[0]
                if re.findall(time_regexp, line) else None)
        if(time and url):
            lines_parsed += 1
            raw_data[url].append(float(time))

        lines_count += 1

    success_percent = 100*lines_parsed/lines_count
    errors_percent = 100 - success_percent

    logging.info('{} lines from {} parsed ({:.3f} %)'.format(
        lines_parsed,
        lines_count,
        success_percent)
    )

    if errors_percent > max_errors_percent:
        logging.error('Max errors percent exceeded! '
                      'Max={} Current={:.3f} Aborting'.format(
                          max_errors_percent,
                          errors_percent)
                      )
        return None

    return raw_data


def process_raw(raw_data):
    '''Process raw data and return stats table'''

    sum_time = sum([sum(i) for i in raw_data.values()])
    sum_lines = sum([len(i) for i in raw_data.values()])

    clean_data = []
    processed = 0
    for url in raw_data:
        times = raw_data[url]
        count = len(raw_data[url])
        line = {
            "url": url,
            "count": count,
            "count_perc": 100*count/sum_lines,
            "time_avg": sum(times)/count,
            "time_max": max(times),
            "time_med": median(times),
            "time_perc": 100*sum(times)/sum_time,
            "time_sum": sum(times)
        }
        clean_data.append(line)
        processed += 1

    return clean_data


def create_report(clean_data, report_size, report_dir, report_date):
    '''Create report from html template'''

    report_fname = '{}/report-{}.html'.format(report_dir,
                                              report_date.strftime('%Y.%m.%d'))

    report_data = json.dumps(sorted(clean_data,
                                    key=lambda k: k['time_sum'],
                                    reverse=True)[:report_size])

    with open(REPORT_TEMPLATE, 'r') as template:
        template_data = Template(template.read())
    report_data = template_data.safe_substitute(table_json=report_data)

    with open(report_fname, 'w') as report:
        report.write(report_data)

    logging.info('Report created: "{}"'.format(report_fname))


def main():
    args = process_args()
    config = process_config(args)

    logging.basicConfig(level=logging.INFO,
                        format="[%(asctime)s] %(levelname).1s %(message)s",
                        datefmt="%Y.%m.%d %H:%M:%S",
                        filename=(config['LOG_FILE']
                                  if 'LOG_FILE' in config else None))

    logging.info('log_analyzer start! Using config: \n{}'.format(
        json.dumps(config, indent=2)
    ))

    latest_log = find_latest_log(config['LOG_DIR'])
    if not latest_log:
        logging.info("No log files to process! Exit")
        sys.exit(0)
    if is_report_exsist(config['REPORT_DIR'], latest_log.date):
        logging.info('Report already exsist! Exit')
        sys.exit(0)

    logging.info('Processing "{}"...'.format(latest_log.path))
    raw_data = process_log(latest_log, config['MAX_ERRORS_PERCENT'])
    if not raw_data:
        sys.exit(0)
    clean_data = process_raw(raw_data)
    create_report(clean_data,
                  config['REPORT_SIZE'],
                  config['REPORT_DIR'],
                  latest_log.date)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(str(e))
