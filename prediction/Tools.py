import pandas as pd
import datetime


def Columns_Get(columns_info):
    new_columns = list()
    for column in columns_info:
        new_columns.append(column[0])

    return new_columns


def Listize(tuple):
    new_list = list()
    for tu in tuple:
        new_list.append(list(tu))
    return new_list


def datetime_to_string(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def string_to_datetime(st):
    return datetime.datetime.strptime(st, "%Y-%m-%d %H:%M:%S")


def cal_predict_finish_time(strtime, cost):
    h = cost // 3600
    m = cost % 3600 // 60
    s = cost % 60
    time = string_to_datetime(strtime)
    time = time + datetime.timedelta(hours=h, minutes=m, seconds=s)
    return datetime_to_string(time)


# 计算某物料、某车型、某仓库的剩余计划量
def cal_residual_plan(plan_day, plan_finish, key):
    a = plan_day[key]
    b = plan_finish[key]
    return a - b

"""
实际上线
def cal_residual_plan(df, kind_name, truck_kind, warehouse_code):
    plan_num = int(sum(
        df[(df['KIND_NAME'] == kind_name) & (df['TRUCK_KIND'] == truck_kind) & (df['WAREHOUSE_CODE'] == warehouse_code)][
            'PLAN_WEIGHT'].values)) + int(sum(
        df[(df['KIND_NAME'] == kind_name) & (df['TRUCK_KIND'] == truck_kind) & (df['WAREHOUSE_CODE'] == warehouse_code)][
            'ADD_WEIGHT'].values))
    finish_num = int(sum(
        df[(df['KIND_CODE'] == kind_code) & (df['TRUCK_KIND'] == truck_kind) & (df['WAREHOUSE_CODE'] == warehouse_code)][
            'ACT_WEIGHT'].values))
    return plan_num - finish_num

"""


