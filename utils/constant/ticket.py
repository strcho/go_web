from . import MbEnum

"""
流水类型, 来自XCEBikeAccount.js的SERIAL_TYPE
"""

# 从前端拷贝来的,后端不维护,仅供参考
toFixTypes = [
    {"type": "0", "name": "龙头"},
    {"type": "1", "name": "车灯"},
    {"type": "2", "name": "线路"},
    {"type": "3", "name": "车篮"},
    {"type": "4", "name": "车轮"},
    {"type": "5", "name": "车撑"},
    {"type": "6", "name": "刹车"},
    {"type": "7", "name": "车座"},
    {"type": "8", "name": "二维码"},
    {"type": "9", "name": "电池"},
    {"type": "10", "name": "挡泥板"},
    {"type": "11", "name": "脚蹬"},
    {"type": "12", "name": "无法还车"},
    {"type": "13", "name": "加私锁"},
    {"type": "14", "name": "锁"},
    {"type": "15", "name": "油门"}
];

alarmTypes = [
    {"type": "0", "name": "电池电量低"},
    {"type": "1", "name": "电压高"},
    {"type": "2", "name": "移动报警"},
    {"type": "3", "name": "出服务区"},
    {"type": "4", "name": "巡检到期"},
    {"type": "5", "name": "电池切断"},
    {"type": "6", "name": "异常离线"},
    {"type": "7", "name": "有单无程"},
    {"type": "8", "name": "超一日无单"},
    {"type": "9", "name": "丢失"},
    {"type": "10", "name": "订单超长"}
];


class FixState(MbEnum):
    TO_FIX = 0  # 报修
    FIXING = 1  # 接单维修中
    FIXED = 2  # 完成工单
    DRAG_BACK = 3  # 无法现场解决，拖回

    @staticmethod
    def unfixed_list():
        return [FixState.TO_FIX.value, FixState.FIXING.value]


class AlarmType(MbEnum):
    LOW_BATTERY = 0  # 低电压
    HIGHT_BATTERY = 1  # 高电压
    MOVE_ALARM = 2  # 移动报警：设防时
    OUT_GFENCE = 3  # 出服务区
    TIME_2_CHECK = 4  # 超过N天未巡检
    POWER_CUT = 5  # 断电瓶
    OFFLINE = 6  # 异常离线
    ORDER_WITHOUT_GPS = 7  # 有单无程
    ONE_DAY_WITHOUT_ORDER = 8  # 超一日无单
    LOST = 9  # 丢失
    ORDER_LAST_ONE_DAY = 10  # 订单超出一天
    SHORT_ORDER = 11  # 短时订单
    UNLOCK_ABNORMAL = 12  # 异常开锁


class ToolOrPF(MbEnum):
    PLATFORM = 'PLATFORM'
    TOOL = 'TOOL'

    INT_PLATFORM = 0
    INT_TOOL = 1

