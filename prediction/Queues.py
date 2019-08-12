import datetime
import Tools as tool
import Data_acquisition as da
from Cars import Car
class Queue:

    def __init__(self, car_queue, factory, warehouse_car, kind_car, car_notice, car_predict_finish_time):
        """

        :param car_queue: 之前在厂外排队的车辆
        :param factory: 之前厂内情况
        :param warehouse_car: 厂内+排队 各仓库的车辆 {‘仓库代码’:[TASK_ID]}
        :param kind_car: 厂内+排队 各品种的车辆 {‘物料代码’:[TASK_ID]}
        """
        self.car_queue = car_queue # {TAKS_ID:car} # 排队未叫号的
        self.car_notice = car_notice # {TASK_ID:car} # 叫号未进厂的
        self.factory = factory
        self.warehouse_car = warehouse_car
        self.kind_car = kind_car
        self.car_predict_finish_time = car_predict_finish_time
        self.plan_finish = self.init_plan()

    def init_plan(self):
        plan_finish = dict()
        for i in self.factory.plan_day.keys():
            # if i == '10302000050000R02W0Y14D002':
            #     plan_finish[i] = 300
            # else:
            plan_finish[i] = 0
        return plan_finish

    def insert(self, car, insert_time):
        """
        :param:car 插入一个car实例
        :return:
        """


        # 判断是否已经达到了今日的计划量
        if self.judge_plan(car):
            # 已经达到计划量，返回到第二天0点的时间差
            car.wait_time = str(tool.string_to_datetime(insert_time[0:11] + '00:00:00') + datetime.timedelta(days=1) -
                                tool.string_to_datetime(insert_time))
            self.car_predict_finish_time[car.task_id] = tool.cal_predict_finish_time\
                (str(tool.string_to_datetime(insert_time[0:11] + '00:00:00') + datetime.timedelta(days=1)), car.cost)

        # 没有达到计划量，代表今日可以入厂，判断是否可以直接入厂
        elif len(self.factory.car_predict_finish_time) < self.factory.__factory_max_count__ \
                and self.judge_factory_warehouse(car) and self.judge_factory_kind(car):
            # 此时均未达到厂内最大车辆、该车仓库最大量、该车物料最大量，代表可以直接入厂，等待时间为0
            car.wait_time = '0'
            self.car_predict_finish_time[car.task_id] = tool.cal_predict_finish_time(insert_time, car.cost)
            self.plan_finish[car.mat_code + car.warehouse_code + car.truck_kind] = self.plan_finish[car.mat_code + car.warehouse_code + car.truck_kind] + 1

        # 不可以直接入厂，需要计算等待时间，厂内+厂外考虑
        else:
            # 有相同品种且有相同仓库的，等待时间=仓库最早出来的和品种车辆的最早出来的偏晚的值
            if self.find_warehouse_early_finish_car(car) != '9999-99-99 99:99:99' and \
                    self.find_kind_early_finish_car(car) != '9999-99-99 99:99:99':
                car.wait_time = str(tool.string_to_datetime(max(self.find_kind_early_finish_car(car),
                                    self.find_warehouse_early_finish_car(car))) - tool.string_to_datetime(insert_time))
                self.car_predict_finish_time[car.task_id] = tool.cal_predict_finish_time(max(self.find_kind_early_finish_car(car), self.find_warehouse_early_finish_car(car)), car.cost)
            # 只有相同仓库的
            elif self.find_warehouse_early_finish_car(car) != '9999-99-99 99:99:99':
                car.wait_time = str(
                    tool.string_to_datetime(self.find_warehouse_early_finish_car(car)) - tool.string_to_datetime(insert_time))
                self.car_predict_finish_time[car.task_id] = tool.cal_predict_finish_time(tool.string_to_datetime
                                                                                         (self.find_warehouse_early_finish_car(car)), car.cost)
            # 只有相同品种的
            else:
                car.wait_time = str(
                    tool.string_to_datetime(self.find_kind_early_finish_car(car)) - tool.string_to_datetime(insert_time))
                self.car_predict_finish_time[car.task_id] = tool.cal_predict_finish_time(tool.string_to_datetime
                                                                                         (self.find_kind_early_finish_car(
                                                                                                 car)), car.cost)
            self.plan_finish[car.mat_code + car.warehouse_code + car.truck_kind] = self.plan_finish[car.mat_code + car.warehouse_code + car.truck_kind] + 1

        # 将该车插入队列
        self.car_queue[car.task_id] = car
        if car.warehouse_code not in self.warehouse_car.keys():
            self.warehouse_car[car.warehouse_code] = [car.task_id]
        else:
            self.warehouse_car[car.warehouse_code].append(car.task_id)
        if car.kind_code not in self.kind_car.keys():
            self.kind_car[car.kind_code] = [car.task_id]
        else:
            self.kind_car[car.kind_code].append(car.task_id)

    def judge_plan(self, car):
        """
        根据车型、物料名、仓库判断是否小于当日计划量
        :param car: 当前来的车辆
        :return: True: 计划量满了   False: 计划量未满
        """
        if tool.cal_residual_plan(self.factory.plan_day, self.plan_finish, car.mat_code + car.warehouse_code + car.truck_kind) == 0:
            return True
        else:
            return False

    def judge_factory_kind(self, car):
        """
        根据物料代码判断该车辆是否能进厂
        :param car: 当前来的车辆
        :return: True: 1.该物料代码有做规则，且小于厂内该物料的最大量
                       2.该物料代码没有做规则
                False: 该物料代码有做规则，且已经达到厂内该物料的最大量
        """
        if car.kind_code == 'other':
            return True
        elif len(self.factory.kind_car[car.kind_code]) < self.factory.__kind_max_count__[car.kind_code]:
            return True
        else:
            return False

    def judge_factory_warehouse(self, car):
        """
        根据仓库代码判断该车辆是否能进厂
        :param car: 当前来的车辆
        :return: True: 1.该物料代码有做规则，且小于厂内该物料的最大量
                       2.该物料代码没有做规则
                False: 该物料代码有做规则，且已经达到厂内该物料的最大量
        """
        if len(self.factory.warehouse_car[car.warehouse_code]) < self.factory.__warehouse_max_count__[car.warehouse_code]:
            return True
        else:
            return False

    def judge_queue_warehouse(self, car):
        """
        判断队列中是否有与该车辆去同一个仓库的车辆
        :param car: 当前来的车辆
        :return: True: 有去同一个仓库的车辆
                Flase: 没有去同一个仓库的车辆
        """
        for i in self.car_queue.keys():
            if self.car_queue[i].warehouse_code == car.warehouse_code:
                return True
        return False

    def judge_queue_kind(self, car):
        """
        判断队列中是否有与该车辆同一个品种的车辆
        :param car: 当前来的车辆
        :return:
        """
        for i in self.car_queue.keys():
            if self.car_queue[i].kind_code == car.kind_code:
                return True
        return False

    def find_warehouse_early_finish_car(self, car):
        """
        找出厂内+厂外该车辆仓库最早结束车辆的预计结束时间
        :param car: 当前来的车辆
        :return: early_time
        """
        early_time = '9999-99-99 99:99:99'
        for i in self.warehouse_car[car.warehouse_code]:
            if self.car_predict_finish_time[i] < early_time:
                early_time = self.car_predict_finish_time[i]
        return early_time

    def find_kind_early_finish_car(self, car):
        """
        找出厂内+厂外该车辆品种最早结束车辆的预计结束时间
        :param car: 当前来的车辆
        :return:
        """
        early_time = '9999-99-99 99:99:99'
        if car.kind_code == 'other':
            return early_time
        else:
            for i in self.kind_car[car.kind_code]:
                if self.car_predict_finish_time[i] < early_time:
                    early_time = self.car_predict_finish_time[i]
            return early_time

    def update(self, insert_time):
        data = da.read_queue_car_time(insert_time)
        data_n = da.read_notice_car_time(insert_time)
        for i in range(len(data)):
            self.car_queue[data.iloc[i, 1]] = Car(data.ix[i].tolist())
        for i in range(len(data_n)):
            self.car_notice[data.iloc[i, 1]] = Car(data_n.ix[i].tolist())


