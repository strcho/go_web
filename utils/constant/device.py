from mbutils.constant import MbEnum


class DeviceState(MbEnum):
    READY = 1  # 可使用
    RIDING = 2  # 骑行中
    BROKEN = 3  # 设备报修
    BOOKING = 4  # 设备被预约
    OFFLINE = 5  # 设备离线
    CHANGING_BATTERY = 6  # 处于工人换电池阶段  【虚拟状态】
    DRAG_BACK = 7  # 设备无法现场维修，置为拖回状态
    LOW_BATTERY = 8  # 新增低电量类型车辆，和拖回状态分开
    MOVE_CAR = 9  # 挪车 【虚拟状态】
    UNSHELEVES = 10  # 下架状态[跟车辆号有绑定关系，不记录redis，只做统计使用]
    # 空。设备login但未上架


class PropertyType(MbEnum):
    OP_AREA = 1  # 运营区
    AGENT = 2  # 代理商
    SHOP = 3  # 门店


class OneMoveType(MbEnum):
    START_BUTTON = 1  # 启动按钮
    PUSH_CAR_BUTTON = 2  # 推车按钮


class MoveType(MbEnum):
    ONE = 1  # 单台
    BATCH = 2  # 多台
    INSTATION = 3  # 站内


class GfenceType(MbEnum):
    AT_SERVICE = "1",  # 服务区
    FOR_PARK = "2",  # 租还区
    DENIED = "3",  # 禁行区
    NO_PARKING = "4",  # 禁停区
    TBEACON_PARKING = "5",  # 道钉停车区
    CHANGE_BATTERY_PARKING = "6",  # 换电片区
    MOVE_CAR_PARKING = "7",  # 挪车片区
    OPERATION_PARKING = "8",  # 运维站点
    NO_CRAWL_PARKING = "9",  # 无围栏站点
