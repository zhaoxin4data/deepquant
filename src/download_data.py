# -*- coding: utf-8 -*-
"""
    Author  ：   Ethan
    Time    ：   2020/9/15 5:53 下午
    Site    :   
    Suggestion  ：
    Description :
    File    :   data.py
    Software    :   PyCharm
"""
import os
import sys
import logging
from abc import ABC, abstractmethod
# import tensorflow as tf
from absl import app as absl_app
from absl import flags
import shutil
import e_util
import tushare as ts
import pandas as pd

ts.set_token("604f55679ce08683bbd407622ebee92238c9ac1efc8633ba4e0acd50")

logger = e_util.Logger(__file__)


def define_flags():
    flags.DEFINE_enum(name="mode", default="train", enum_values=["train", "predict"],
                      help="")


def download_daily(ts_code, start_date, end_date, csv_file):
    """
    https://waditu.com/document/2?doc_id=27
    :param ts_code: 600837.SH, ...
    :param start_date: '20201020'
    :param end_date: '20201026'
    :return:
    """
    pro = ts.pro_api()

    df = pro.query("daily", ts_code=ts_code, start_date=start_date, end_date=end_date)
    df.to_csv(csv_file)


def run_loop(flags_obj):
    logger.info("PROJECT_DIR: {}".format(e_util.PROJECT_DIR))
    ts_code = "600837.SH"
    start_date = "20200101"
    end_date = "20201027"
    csv_file = "{}/data/{}_{}_{}.csv".format(e_util.PROJECT_DIR, ts_code, start_date, end_date)
    download_daily(ts_code, start_date, end_date, csv_file)

    logger.info("end")

    return


def main(_):
    run_loop(flags.FLAGS)


if __name__ == '__main__':
    define_flags()
    absl_app.run(main)