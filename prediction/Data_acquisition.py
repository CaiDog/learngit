import SQLConnect as connect
import pandas as pd
import Tools
import datetime


def read_factory_car_time(time):
    sql = "SELECT * FROM dispatch_data_analyze.t_disp_entry_queue where FINISH_TIME > '{0}' and ENTRY_TIME < '{1}' and " \
          "WAREHOUSE_CODE IS NOT NULL and TIMESTAMPDIFF(MINUTE,ENTRY_TIME,FINISH_TIME) < 2880".format(time, time)
    connect.cursor_dispatch.execute(sql)
    time_data = connect.cursor_dispatch.fetchall()
    time_data = Listize(time_data)
    time_columns = connect.cursor_dispatch.description
    time_columns = Columns_Get(time_columns)
    time_pd = pd.DataFrame(time_data, columns=time_columns)
    return time_pd


def read_queue_car_time(time):
    sql = "SELECT * FROM dispatch_data_analyze.t_disp_entry_queue where ENTRY_NOTICE_TIME >= '{0}' " \
          "and QUEUE_START_TIME < '{1}' and WAREHOUSE_CODE IS NOT NULL and TIMESTAMPDIFF(MINUTE," \
          "ENTRY_TIME,FINISH_TIME) < 4320 and TIMESTAMPDIFF(MINUTE,QUEUE_START_TIME,ENTRY_TIME)<4320".format(time, time)
    connect.cursor_dispatch.execute(sql)
    time_data = connect.cursor_dispatch.fetchall()
    time_data = Listize(time_data)
    time_columns = connect.cursor_dispatch.description
    time_columns = Columns_Get(time_columns)
    time_pd = pd.DataFrame(time_data, columns=time_columns)
    return time_pd


def read_notice_car_time(time):
    sql = "SELECT * FROM dispatch_data_analyze.t_disp_entry_queue where ENTRY_NOTICE_TIME <= '{0}' " \
          "and ENTRY_TIME >= '{1}'".format(time, time)
    connect.cursor_dispatch.execute(sql)
    time_data = connect.cursor_dispatch.fetchall()
    time_data = Listize(time_data)
    time_columns = connect.cursor_dispatch.description
    time_columns = Columns_Get(time_columns)
    time_pd = pd.DataFrame(time_data, columns=time_columns)
    return time_pd

def read_test_car_time(time1, time2):
    sql = "SELECT * FROM dispatch_data_analyze.t_disp_entry_queue where QUEUE_START_TIME > '{0}' and QUEUE_START_TIME < '{1}'" \
          "order by QUEUE_START_TIME".format(time1, time2)
    connect.cursor_dispatch.execute(sql)
    time_data = connect.cursor_dispatch.fetchall()
    time_data = Listize(time_data)
    time_columns = connect.cursor_dispatch.description
    time_columns = Columns_Get(time_columns)
    time_pd = pd.DataFrame(time_data, columns=time_columns)
    return time_pd

def in_plant_cars(time):
    sql = '''
        select *
        from t_disp_entry_queue
        where entry_time is not null and finish_time is not null and WAREHOUSE_NAME IS NOT NULL and 
        TIMESTAMPDIFF(second, entry_time, "%s")>0 and TIMESTAMPDIFF(second, finish_time, "%s")<0
        ''' % (time, time)
    connect.cursor_dispatch.execute(sql)
    data = connect.cursor_dispatch.fetchall()
    data = Listize(data)
    columns = connect.cursor_dispatch.description
    columns = Columns_Get(columns)
    df = pd.DataFrame(data, columns=columns)
    return df


def in_warehouse_cars(time):
    sql = '''
            select TASK_ID,warehouse_code
            from t_disp_entry_queue
            where entry_time is not null and finish_time is not null and WAREHOUSE_CODE IS NOT NULL and 
            TIMESTAMPDIFF(second, entry_time, "%s")>0 and TIMESTAMPDIFF(second, finish_time, "%s")<0
            ''' % (time, time)
    connect.cursor_dispatch.execute(sql)
    data = connect.cursor_dispatch.fetchall()
    data = Listize(data)
    columns = connect.cursor_dispatch.description
    columns = Columns_Get(columns)
    df = pd.DataFrame(data, columns=columns)
    return df


def in_plant_kind_cars(time):
    sql = '''
            select TASK_ID,mat_code
            from t_disp_entry_queue
            where entry_time is not null and finish_time is not null and WAREHOUSE_CODE IS NOT NULL and 
            TIMESTAMPDIFF(second, entry_time, "%s")>0 and TIMESTAMPDIFF(second, finish_time, "%s")<0
            ''' % (time, time)
    connect.cursor_dispatch.execute(sql)
    data = connect.cursor_dispatch.fetchall()
    data = Listize(data)
    columns = connect.cursor_dispatch.description
    columns = Columns_Get(columns)
    df = pd.DataFrame(data, columns=columns)
    return df


def read_plan_day(time):
    sql = 'SELECT KIND_CODE, WAREHOUSE_CODE, TRUCK_KIND, PLAN_WEIGHT, ADD_WEIGHT ' \
          'FROM dispatch_data_analyze.t_disp_entry_plan_day where PLAN_DATE = "%s" and ' \
          '(PLAN_WEIGHT !=0 OR ADD_WEIGHT!=0) order by KIND_CODE,WAREHOUSE_CODE,TRUCK_KIND' % (time)
    connect.cursor_dispatch.execute(sql)
    data = connect.cursor_dispatch.fetchall()
    data = Listize(data)
    columns = connect.cursor_dispatch.description
    columns = Columns_Get(columns)
    df = pd.DataFrame(data, columns=columns)
    result = dict()
    flag = 0
    for i in range(len(df)):
        if i == len(df) - 1:
            if flag == 1:
                result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])] = int(
                    result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])]
                    + df.iloc[i + 1, 3] + df.iloc[i + 1, 4])
            else:
                result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])] = int(df.iloc[i, 3] + df.iloc[i, 4])
        else:
            if df.iloc[i, 0] == df.iloc[i+1, 0] and df.iloc[i, 1] == df.iloc[i+1, 1] and df.iloc[i, 2] == df.iloc[i+1, 2]:
                flag = 1
                if (df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2]) not in result.keys():
                    result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])] = int(df.iloc[i, 3] + df.iloc[i, 4])
                else:
                    result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])] = int(result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])]
                    + df.iloc[i+1, 3] + df.iloc[i+1, 4])
            elif flag == 1:
                result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])] = int(result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])]
                    + df.iloc[i + 1, 3] + df.iloc[i + 1, 4])
                flag = 0
            else:
                result[(df.iloc[i, 0] + df.iloc[i, 1] + df.iloc[i, 2])] = int(df.iloc[i, 3] + df.iloc[i, 4])
    return result

'''
实际上线
def read_plan_day(time):
    sql = '
            select *
            from t_disp_entry_plan_day
            where PLAN_DATE = "%s"
            ' % (time)
    connect.cursor_dispatch.execute(sql)
    data = connect.cursor_dispatch.fetchall()
    data = Listize(data)
    columns = connect.cursor_dispatch.description
    columns = Columns_Get(columns)
    df = pd.DataFrame(data, columns=columns)
    return df
'''


def str_transform(str):
    str = str.split('-')
    str = str[0] + str[1]
    return str


def is_digit(x):
    if x:
        if x.isdigit():
            return True
        else:
            return False
    else:
        return False


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

# def To_Date(x):
#     if not pd.isnull(x):
#         date = pd.datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8]), hour=int(x[8:10]), minute=int(x[10:12]), second=int(x[12:14]))
#     # try:
#     #     date = pd.datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8]), hour=int(x[8:10]), minute=int(x[10:12]), second=int(x[12:14]))
#     #
#     # except:
#     #     date = pd.datetime(year=1971, month=1, day=1)
#
#         return date

def To_Date(x):
    if not pd.isnull(x):
        date = pd.datetime(year=int(x[0:4]), month=int(x[5:7]), day=int(x[8:10]), hour=int(x[11:13]), minute=int(x[14:16]), second=int(x[17:19]))
    # try:
    #     date = pd.datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8]), hour=int(x[8:10]), minute=int(x[10:12]), second=int(x[12:14]))
    #
    # except:
    #     date = pd.datetime(year=1971, month=1, day=1)

        return date

def To_Date_Entering_Info(x):
    try:
        date = pd.datetime(year=int(x[0:4]), month=int(x[5:7]), day=int(x[8:10]), hour=int(x[11:13]), minute=int(x[14:16]), second=int(x[17:19]))

    except:
        date = pd.datetime(year=1971, month=1, day=1)

    return date

if __name__ == '__main__':
    plan = Stacking_Info_Detail()