import SQLConnect as connect
import pandas as pd
import Tools as tool
import datetime
import Data_acquisition as da

def Read_queue_time():
    sql = "SELECT ENTRY_NOTICE_TIME,FINISH_TIME,NET_WEIGHT,TRUCK_KIND " \
          "FROM dispatch_data_analyze.t_disp_entry_queue WHERE FINISH_TIME IS NOT NULL AND " \
          "WAREHOUSE_CODE IS NOT NULL AND QUEUE_START_TIME BETWEEN '2019-07-01' AND '2019-08-10' " \
          "AND FINISH_TIME BETWEEN '2019-07-01' AND '2019-08-10' AND TIMESTAMPDIFF(MINUTE,ENTRY_TIME,FINISH_TIME)<4320 " \
          "AND TIMESTAMPDIFF(MINUTE,QUEUE_START_TIME,ENTRY_TIME)<4320 ORDER BY ENTRY_TIME"
    connect.cursor_dispatch.execute(sql)
    time_data = connect.cursor_dispatch.fetchall()
    time_data = da.Listize(time_data)
    time_columns = connect.cursor_dispatch.description
    time_columns = da.Columns_Get(time_columns)
    time_pd = pd.DataFrame(time_data, columns=time_columns)
    return time_pd

data = Read_queue_time()

data['wait_time'] = 0

for i in range(len(data)):
    data.iloc[i, 4] = (tool.string_to_datetime(data.iloc[i, 2]) - tool.string_to_datetime(data.iloc[i, 1])).total_seconds()
a = data[da]

data.to_excel('data.xlsx')