import datetime


# 排队车辆信息：排队号码、装车清单、前方等待车辆、预计等待时间
class Car:
    __unload_time_mean__ = {'1': 6076, '2': 400, '4': 250}
    __kind_max_count__ = {'10201': 10, '11002': 600, '10302000050000': 300, '10701': 100, '10301000010000': 30, '10301': 150}

    def __init__(self, business_list):
        self.business_list = business_list  # 业务清单
        self.task_id = business_list[1]     # 任务号（唯一确定一辆车）
        self.kind_code = self.cal_kind_code(business_list)     # 物料代码
        self.mat_code = business_list[7]   # 原物料代码
        self.kind_name = business_list[8]   # 物料名字
        self.truck_kind = business_list[10] # 车型
        self.weight = business_list[14]     # 净重
        self.queue_num = business_list[19]  # 大队列
        self.kind_queue = business_list[20] # 小队列
        self.queue_start_time = business_list[21]  # 十公里签到时间
        self.warehouse_code = business_list[25]  # 仓库代码
        self.entry_notice_time = business_list[34]  # 进厂提醒时间
        self.entry_time = business_list[37] # 进厂时间
        self.finish_time = business_list[38]# 出厂时间
        self.cost = self.cal_cost()         # 装货时间
        self.wait_time = str()              # 预计等待时间

    def cal_cost(self):
        if self.truck_kind == '1':
            return self.__unload_time_mean__['1']
        elif self.truck_kind == '2':
            return self.__unload_time_mean__['2'] * int(self.weight)
        else:
            return self.__unload_time_mean__['4'] * int(self.weight)

    def cal_kind_code(self, business_list):
        for i in self.__kind_max_count__.keys():
            if business_list[7].startswith(i):
                return i
        return 'other'


