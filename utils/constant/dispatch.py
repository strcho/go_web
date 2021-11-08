import math

from . import MbEnum

# 取消原因
class WorkmanCancelType(MbEnum):
    RIDING_CANCAL_REASON_TYPE = 100  # 用户骑车时,派单取消原因为100
    SYS_EARLY_FINISH_TYPE = 200  # 发现工单被其他的巡视人员完成后,及时取消自动派单任务


# 原始派单分类
class OriginType(MbEnum):
    ALARM = 1  # 报警单
    FIX = 2  # 检修单
    AUTO_MOVE = 3  # 自动挪车单
    COMPLAINT = 4  # 报修


# 实际派单分类
class ActualType(MbEnum):
    CHANGE_BATTERY = 1  # 换电
    FIX = 2  # 维修
    MOVE_CAR = 3  # 挪车
    INSPECT = 4  # 巡检


# 派单排序
class OrderTYPE(MbEnum):
    DEFAULT = 0  # 系统默认
    HIGHER_ACHIEVE = 1  # 预测绩效
    SHORTER_TIME = 2  # 任务时长
    ENDTIME = 3  # 截止时间

# 工作进度
class DispatchWorkProgress(MbEnum):
    Init = 0  # 工作初始状态
    Processing = 1  # 进行中
    Finish = 2  # 已完成


# 订单状态
class DispatchTaskType(MbEnum):
    Init = 0  # 未接单
    Refuse = 1  # 经理拒绝单
    Process = 2  # 工人已经接单
    Canceling = 3  # 接单取消中
    Canceled = 4  # 接单已经取消
    CancelRefuse = 5  # 接单拒绝取消
    Finish = 6  # 单已经完成
    Expired = 7  # 单已经过期
    ExpiredFinish = 8  # 单过期后完成
    NoFoundMan = 9  # 找不到工人的废弃单

    @staticmethod
    def workman_process_list():
        """
        工人正在进行中的任务列表
        """
        return [2, 3, 5]

    @staticmethod
    def workman_finish_list():
        """
        工人正在完成中的任务列表
        """
        return [4, 6, 7, 8]

    @staticmethod
    def manager_to_audit_list():
        """
        城市经理待审批的任务列表
        """
        return [3]

    @staticmethod
    def manager_to_dispatch_list():
        """
        城市经理已处理(派单)的任务列表
        :return:
        """
        return [1, 2]

    @staticmethod
    def manager_to_approve_list():
        """
        城市经理已处理(审批)的任务列表
        :return:
        """
        return [4, 5]


def distance_two_points(b_lat, b_lng, e_lat, e_lng):
    """
    b_lat：第一个点的纬度
    b_lng：第一个点的经度
    e_lat：第二个点的纬度
    e_lng：第一个点的经度
    """
    # 将十进制度数转化为弧度
    b_lng, b_lat, e_lng, e_lat = map(math.radians, [b_lng, b_lat, e_lng, e_lat])
    # haversine公式
    dlng = e_lng - b_lng
    dlat = e_lat - b_lat
    a = math.sin(dlat / 2) ** 2 + math.cos(b_lat) * math.cos(e_lat) * math.sin(dlng / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    return c * r * 1000  # 单位为米

# print(distance_two_points(30.5025, 114.429167, 30.579167, 114.257223))
