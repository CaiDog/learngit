from Factories import Factory
from Cars import Car
from Queues import Queue
import Tools as tool
import Data_acquisition as da
import datetime
import copy


class Main:
    __init_time__ = '2019-07-21 00:00:00'
    __init_day__ = '2019-07-21'

    def __init__(self):
        self.factory = self.factory_data_init(self)
        self.queue = self.queue_data_init()

    @staticmethod
    def factory_data_init(self):
        # 厂内车辆 {TASK_ID:car}
        factory_car = dict()
        # 厂内各仓库的车辆 {仓库代码:[TASK_ID]}
        warehouse_car = dict()
        # 厂内各品种车辆 {品种代码:[TASK_ID]}
        kind_car = dict()
        # 厂内各车的预计结束 {TASK_ID:时间}
        car_predict_finish_time = dict()
        # 读取数据
        data = da.read_factory_car_time(self.__init_time__)
        # 初始化factory_car
        for i in range(len(data)):
            factory_car[data.iloc[i, 1]] = Car(data.ix[i].tolist())
        for i in factory_car.keys():
            # 初始化warehouse_car
            if factory_car[i].warehouse_code not in warehouse_car.keys():
                warehouse_car[factory_car[i].warehouse_code] = [factory_car[i].task_id]
            else:
                warehouse_car[factory_car[i].warehouse_code].append(factory_car[i].task_id)
            # 初始化kind_car
            if factory_car[i].kind_code not in kind_car.keys():
                kind_car[factory_car[i].kind_code] = [factory_car[i].task_id]
            else:
                kind_car[factory_car[i].kind_code].append(factory_car[i].task_id)
            # 初始化car_predict_finish_time = 进厂时间 + 花费时间
            car_predict_finish_time[factory_car[i].task_id] = tool.cal_predict_finish_time(factory_car[i].entry_time, factory_car[i].cost)
        return Factory(da.read_plan_day(self.__init_day__), warehouse_car, kind_car, car_predict_finish_time, factory_car)

    def queue_data_init(self):
        # 厂外等待的车辆队列 {TASK_ID:car} 排队但未叫号
        car_queue = dict()
        # 已经叫号但还未入厂的车辆
        car_notice = dict()
        # 厂内情况
        factory = self.factory
        # 厂内 + 厂外所有车辆的预计结束时间 {TASK_ID:时间}
        car_predict_end_time = copy.deepcopy(self.factory.car_predict_finish_time)
        # 厂内 + 厂外各仓库的车辆 {仓库代码:[TASK_ID]}
        warehouse_car = copy.deepcopy(self.factory.warehouse_car)
        # 厂内 + 厂外各品种车辆 {品种代码:[TASK_ID]}
        kind_car = copy.deepcopy(self.factory.kind_car)
        # 读取数据
        data = da.read_queue_car_time(self.__init_time__)
        data_n = da.read_notice_car_time(self.__init_time__)
        # 初始化car_queue
        for i in range(len(data)):
            car_queue[data.iloc[i, 1]] = Car(data.ix[i].tolist())
        for i in range(len(data_n)):
            car_notice[data_n.iloc[i, 1]] = Car(data_n.ix[i].tolist())


        sort_notice = sorted(car_notice.values(), key=lambda x: int(x.queue_num))
        for car in sort_notice:
            # # 判断是否已经达到了今日的计划量
            # if self.judge_plan(i):
            #     # 已经达到计划量，预计结束时间为第二天零点+cost
            #     '''
            #     实际上线
            #     car_predict_end_time[i.task_id] = tool.cal_predict_finish_time(str(tool.string_to_datetime(
            #         datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")) + datetime.timedelta(days=1)), i.cost)
            #     '''
            #     car_predict_end_time[i.task_id] = tool.cal_predict_finish_time(str(tool.string_to_datetime(self.__init_time__) + datetime.timedelta(days=1)), i.cost)
            # # 没有达到计划量，代表今日可以入厂，判断是否可以直接入厂
            # el
            if len(self.factory.car_predict_finish_time) < self.factory.__factory_max_count__ \
                    and self.judge_factory_warehouse(car) and self.judge_factory_kind(car):
                # 此时均未达到厂内最大车辆、该车仓库最大量、该车物料最大量，代表可以直接入厂，等待时间为0
                '''
                实际上线
                car_predict_end_time[i.task_id] = tool.cal_predict_finish_time(tool.datetime_to_string(datetime.datetime.now()), i.cost)
                '''
                car_predict_end_time[car.task_id] = tool.cal_predict_finish_time(self.__init_time__, car.cost)

            # 不可以直接入厂，需要计算等待时间，厂内+厂外考虑
            else:
                # 有相同品种且有相同仓库的，等待时间=仓库最早出来的和品种车辆的最早出来的偏晚的值
                if self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time) != '9999-99-99 99:99:99'\
                        and self.find_kind_early_finish_car(car, kind_car, car_predict_end_time) != '9999-99-99 99:99:99':
                    car_predict_end_time[car.task_id] = tool.cal_predict_finish_time\
                        (max(self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time),
                             self.find_kind_early_finish_car(car, kind_car, car_predict_end_time)), car.cost)
                # 只有相同仓库的
                elif self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time) != '9999-99-99 99:99:99':
                    car_predict_end_time[car.task_id] = tool.cal_predict_finish_time\
                        ((self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time)), car.cost)
                # 只有相同品种的
                else:
                    car_predict_end_time[car.task_id] = tool.cal_predict_finish_time\
                        ((self.find_kind_early_finish_car(car, kind_car, car_predict_end_time)), car.cost)
            if car.warehouse_code not in warehouse_car.keys():
                warehouse_car[car.warehouse_code] = [car.task_id]
            else:
                warehouse_car[car.warehouse_code].append(car.task_id)
            if car.kind_code not in kind_car.keys():
                kind_car[car.kind_code] = [car.task_id]
            else:
                kind_car[car.kind_code].append(car.task_id)

        # 让厂外排队车辆根据排队号码排序 sort_queue=[car]
        sort_queue = sorted(car_queue.values(), key=lambda x: int(x.queue_num))
        # 初始化厂外排队车辆的预计结束时间
        for car in sort_queue:
            #
            # # 判断是否已经达到了今日的计划量
            # if self.judge_plan(i):
            #     # 已经达到计划量，预计结束时间为第二天零点+cost
            #     '''
            #     实际上线
            #     car_predict_end_time[i.task_id] = tool.cal_predict_finish_time(str(tool.string_to_datetime(
            #         datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")) + datetime.timedelta(days=1)), i.cost)
            #     '''
            #     car_predict_end_time[i.task_id] = tool.cal_predict_finish_time(str(tool.string_to_datetime(self.__init_time__) + datetime.timedelta(days=1)), i.cost)
            # # 没有达到计划量，代表今日可以入厂，判断是否可以直接入厂
            # el
            #
            if len(self.factory.car_predict_finish_time) < self.factory.__factory_max_count__ and self.judge_factory_warehouse(car) and self.judge_factory_kind(car):
                # 此时均未达到厂内最大车辆、该车仓库最大量、该车物料最大量，代表可以直接入厂，等待时间为0
                '''
                实际上线
                car_predict_end_time[i.task_id] = tool.cal_predict_finish_time(tool.datetime_to_string(datetime.datetime.now()), i.cost)
                '''
                car_predict_end_time[car.task_id] = tool.cal_predict_finish_time(self.__init_time__, car.cost)
            # 不可以直接入厂，需要计算等待时间，厂内+厂外考虑
            else:
                # 有相同品种且有相同仓库的，等待时间=仓库最早出来的和品种车辆的最早出来的偏晚的值
                if self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time) != '9999-99-99 99:99:99'\
                        and self.find_kind_early_finish_car(car, kind_car, car_predict_end_time) != '9999-99-99 99:99:99':
                    car_predict_end_time[car.task_id] = tool.cal_predict_finish_time\
                        (max(self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time),
                             self.find_kind_early_finish_car(car, kind_car, car_predict_end_time)), car.cost)
                # 只有相同仓库的
                elif self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time) != '9999-99-99 99:99:99':
                    car_predict_end_time[car.task_id] = tool.cal_predict_finish_time \
                        ((self.find_warehouse_early_finish_car(car, warehouse_car, car_predict_end_time)), car.cost)
                # 只有相同品种的
                else:
                    car_predict_end_time[car.task_id] = tool.cal_predict_finish_time \
                        ((self.find_kind_early_finish_car(car, kind_car, car_predict_end_time)), car.cost)
            if car.warehouse_code not in warehouse_car.keys():
                warehouse_car[car.warehouse_code] = [car.task_id]
            else:
                warehouse_car[car.warehouse_code].append(car.task_id)
            # 初始化kind_car
            if car.kind_code not in kind_car.keys():
                kind_car[car.kind_code] = [car.task_id]
            else:
                kind_car[car.kind_code].append(car.task_id)

        return Queue(car_queue, factory, warehouse_car, kind_car, car_notice, car_predict_end_time)

    # def judge_plan(self, car):
    #     """
    #     根据车型、物料名、仓库判断是否小于当日计划量
    #     :param car: 当前来的车辆
    #     :return: True: 计划量满了   False: 计划量未满
    #     """
    #     if tool.cal_residual_plan(self.factory.plan_day, self.queue.plan_finish, car.mat_code) == 0:
    #         return True
    #     else:
    #         return False

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

    def find_warehouse_early_finish_car(self, car, warehouse_car, car_predict_finish_time):
        """
        找出厂内+厂外该车辆仓库最早结束车辆的预计结束时间
        :param car: 当前来的车辆
        :return: early_time
        """
        early_time = '9999-99-99 99:99:99'
        for i in warehouse_car[car.warehouse_code]:
            if car_predict_finish_time[i] < early_time:
                early_time = car_predict_finish_time[i]
        return early_time

    def find_kind_early_finish_car(self, car, kind_car, car_predict_finish_time):
        """
        找出厂内+厂外该车辆品种最早结束车辆的预计结束时间
        :param car: 当前来的车辆
        :return:
        """
        early_time = '9999-99-99 99:99:99'
        if car.kind_code == 'other':
            return early_time
        else:
            for i in kind_car[car.kind_code]:
                if car_predict_finish_time[i] < early_time:
                    early_time = car_predict_finish_time[i]
            return early_time

m = Main()



a = da.read_test_car_time('2019-07-21 00:00:00', '2019-07-21 01:00:00')

for i in range(len(a)):
    car = Car(a.ix[i].tolist())
    # 厂外情况更新
    m.queue.update(car.queue_start_time)
    # 厂内情况更新，得到一个实时的厂内数据
    m.factory.update(car.queue_start_time)
    m.queue.insert(car, car.queue_start_time)
    print(car.task_id, car.wait_time)

