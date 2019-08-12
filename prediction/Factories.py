import Data_acquisition as da
import Tools as tool
import datetime
from Cars import Car


class Factory:
    # 每种车型的在厂时间
    __unload_time_mean__ = {'1': 5576, '2': 287, '4': 240}
    # 厂内最大容量
    __factory_max_count__ = 1100
    # 每个品种最大容量
    __kind_max_count__ = {'10201': 10, '11002': 600, '10302000050000': 300, '10701': 100, '10301000010000': 30,
                          '10301': 150}
    # 每个仓库最大容量
    __warehouse_max_count__ = {'R01W1Y01A00': 5, 'R01W2Y03B00': 1, 'R01WAY01A00': 75, 'R01WAY01B00': 600,
                                'R02W0Y06B00': 10, 'R02W0Y06C00': 300, 'R02W0Y06D00': 100, 'R02W0Y06E00': 50,
                                'R02W0Y06F00': 50, 'R02W0Y11A00': 10, 'R02W0Y11B00': 15, 'R02W0Y12I00': 5,
                                'R02W0Y12N00': 10, 'R02W0Y13000': 50, 'R02W0Y14D00': 26, 'R02W2Y01A01': 5,
                                'R02W2Y01B01': 5, 'R02W2Y01B02': 5, 'R02W2Y01C01': 20, 'R02W2Y01D01': 25,
                                'R02W3Y01F03': 5, 'R02W3Y03A00': 100, 'R02W3Y03C00': 100, 'R02W4Y01A00': 3,
                                'R02W4Y01B00': 5, 'R02W5Y01A00': 5, 'R02W5Y01B00': 10, 'R07W1Y01A00': 100,
                                'R09W1Y01000': 100, 'R09W1Y05000': 1}

    def __init__(self, plan_day, warehouse_car, kind_car, car_predict_finish_time, factory_car):
        """
        程序运行时的厂内车辆初始状态
        :param plan_day: 需要从数据库中读取当天的各品种计划车辆数
        :param warehouse_car: 之前在每个仓库内的车辆
        :param kind_car: 之前在厂每个品种的车辆
        :param car_predict_finish_time: 之前在厂的每个车辆的预计结束时间
        """
        # 每天的计划车辆数（从数据库中读取）dataframe
        self.plan_day = plan_day
        # 每个仓库的车辆（实时）{仓库名:[TASK_ID]}
        self.warehouse_car = warehouse_car
        # 每个品种的车辆（实时）{品种:[TASK_ID]}
        self.kind_car = kind_car
        # 每辆车的预计结束时间（实时，也就是在厂车辆）{TASK_ID:时间}
        self.car_predict_finish_time = car_predict_finish_time
        # 更新数据的时间点
        self.update_time = tool.datetime_to_string(datetime.datetime.now())
        # 仓库中的车辆字典  {TASK_ID:car}
        self.factory_car = factory_car

    def monitor_in_factory_car(self):
        """
        实时监控厂内车辆：1.有车辆进厂  2.有车辆出厂
        :param:time：监控的时间点 string类型 '%Y-%m-%d %H:%M:%S'
        :return:厂内车辆的任务号 list类型
        """
        # 更新car_predict_finish_time = 入厂时间 + 卸货时间
        car_predict_finish_time = dict()
        for i in self.factory_car.keys():
            car_predict_finish_time[i] = tool.cal_predict_finish_time(self.factory_car[i].entry_time, self.factory_car[i].cost)
        self.car_predict_finish_time = car_predict_finish_time

    def monitor_in_warehouse_car(self):
        """
        实时监控仓库内车辆
        :param time: time 监控的时间点 string类型 '%Y-%m-%d %H:%M:%S'
        :return: 每个仓库内的车辆  dict类型
        """
        warehouse_car = dict()
        for i in self.factory_car.keys():
            if self.factory_car[i].warehouse_code not in warehouse_car.keys():
                warehouse_car[self.factory_car[i].warehouse_code] = [i]
            else:
                warehouse_car[self.factory_car[i].warehouse_code].append(i)
        self.warehouse_car = warehouse_car

    def monitor_kind_car(self):
        """
        实习监控厂内各品种车辆
        :param time: time 监控的时间点 string类型 '%Y-%m-%d %H:%M:%S'
        :return: 每个品种的车辆 dict类型
        """
        kind_car = dict()
        for i in self.factory_car.keys():
            if self.factory_car[i].kind_code not in kind_car.keys():
                kind_car[self.factory_car[i].kind_code] = [i]
            else:
                kind_car[self.factory_car[i].kind_code].append(i)
        self.kind_car = kind_car

    def update(self, time):
        """
        更新厂内车辆情况
        :param time: 更新时间
        :return:
        """
        df = da.read_factory_car_time(time)
        for i in range(len(df)):
            self.factory_car[df.iloc[i, 1]] = Car(df.ix[i].tolist())
        self.monitor_in_warehouse_car()
        self.monitor_kind_car()
        self.monitor_in_factory_car()
        self.update_time = time



