# -*- coding: utf-8 -*-
"""
    Author  ：   Ethan
    Time    ：   2020/8/10 2:43 下午
    Site    :   
    Suggestion  ：
    Description :
    File    :   e_util.py
    Software    :   PyCharm
"""
import os
import sys
import logging
from abc import ABC, abstractmethod
# import tensorflow as tf
from absl import app as absl_app
from absl import flags
from datetime import date, timedelta, datetime
import shutil

# >>> import my module
# This is your Project Root
PROJECT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)


class Logger(logging.Logger, object):

    def __init__(self, name="default", level=logging.INFO, log_file=None, stdout=True):

        super(Logger, self).__init__(name, level)

        # Gets or creates a logger
        # self._logger = logging.getLogger(logger_name)

        # set log level
        # self._logger.setLevel((level))

        # set formatter
        formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')

        if log_file is not None:
            if not os.path.exists(log_file):
                os.makedirs(log_file)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.addHandler(file_handler)

        if stdout:
            # define stream handler
            stream_handler = logging.StreamHandler(sys.stdout)
            # set handler
            stream_handler.setFormatter(formatter)
            # add file handler to logger
            self.addHandler(stream_handler)


logger = Logger(__file__)


class DataReader(object):
    def __init__(self, file_path, report_interval=10):
        super(DataReader, self).__init__()
        self.file_path = file_path
        self.report_interval = report_interval
        self.file_total_lines = sum([1 for line in open(self.file_path)])
        self.has_read_lines = 0
        self.progress_rate = 0

    @abstractmethod
    def parse_line(self, line):
        pass

    def read_line(self):
        for line in open(self.file_path, "r"):
            self.has_read_lines += 1
            self.progress_rate = self.has_read_lines / self.file_total_lines
            if self.has_read_lines % self.report_interval == 0:
                logger.info("[DataReader] has_read_lines: {}, file_total_lines: {}, progress_rate: {} ......".format(
                    self.has_read_lines, self.file_total_lines, round(self.progress_rate, 5)))
            yield self.parse_line(line)


def define_flags():
    flags.DEFINE_enum(name="mode", default="train", enum_values=["train", "predict"],
                      help="")


def remove_file(file_path):
    if os.path.exists(file_path):
        logger.info("remove file: {}".format(file_path))
        os.remove(file_path)


def remove_dir(dir):
    if os.path.exists(dir):
        cmd = "rm -rf {}".format(dir)
        logger.info(cmd)
        os.system(cmd)


def ensure_dir(dir):
    if not os.path.exists(dir):
        logger.info("mkdir: {}".format(dir))
        os.makedirs(dir)


def refresh_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
    else:
        cmd = "rm -rf {}/*".format(dir)
        logger.info(cmd)
        os.system(cmd)


def hdfs2local(hdfs_path, local_path, reload=False):
    if reload is False and os.path.exists(local_path):
        logger.info("local_path already exists: {}".format(local_path))
        return 1
    else:
        remove_file(local_path)
        ensure_dir(os.path.dirname(local_path))
        logger.info("downloading from HDFS: {} --> Local: {}".format(hdfs_path, local_path))
        cmd = "hdfs dfs -get {} {}".format(hdfs_path, local_path)
        status = os.system(cmd)
        if int(status) != 0:
            logger.fatal("failed: {}".format(cmd))
            return 1
        logger.info("download finish: {} -> {}".format(hdfs_path, local_path))
        return 0


def save_list_to_file(data_dict, file_path):
    if len(data_dict) < 1:
        logger.warning("[save_list_to_file], len(data_dict): {} < 1 ".format(len(data_dict)))
        return
    logger.info("[save_list_to_file], len(data_dict): {} -> file path: {}".format(len(data_dict), file_path))
    ensure_dir(os.path.dirname(file_path))
    remove_file(file_path)
    with open(file_path, "w") as f:
        for key in data_dict:
            line = ",".join([str(i) for i in data_dict[key]])
            f.write("{} {}\n".format(key, line))


def str2datetime(date_string, format_str="%Y-%m-%d-%H"):
    return datetime.strptime(date_string, format_str)


def datetime2str(dt: datetime, format_str="%Y-%m-%d-%H"):
    return dt.strftime(format_str)


def get_date_list_by_timedelta(start_dt: date, delta: timedelta):
    days = []
    for i in range(delta.days + 1):
        day = start_dt + timedelta(days=i)
        days.append(day)
    return days


def get_datetime_list_by_start_end(start_dt: date, end_dt: date):
    """
    s = str2datetime("20200901", "%Y%m%d")
    e = str2datetime("20200911", "%Y%m%d")
    days = get_datetime_list_by_start_end(s, e)
    days = [datetime2str(i, "%Y%m%d") for i in days]
    print(days)
    :param start_dt:
    :param end_dt:
    :return:
    """
    delta = end_dt - start_dt  # as timedelta
    return get_date_list_by_timedelta(start_dt, delta)


def run_loop(flags_obj):

    return


def main(_):
    run_loop(flags.FLAGS)


if __name__ == '__main__':
    define_flags()
    absl_app.run(main)
