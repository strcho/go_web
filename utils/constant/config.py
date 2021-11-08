from . import MbEnum

THREAD_NUM = 40
REDIS_MAX_COLLECITONS = 40
SQLALCHEMY_POOL_SZIE = 20


class ConfigName(MbEnum):
    HELP = 'helpSet'  # 帮助页配置
    FIX = 'eFixReason'  # 保修项配置
    DEPOSIT = 'depositSet'  # 押金配置
    TEL = 'telephoneSet'  # 客服电话配置
    COST = 'costSet'  # 计费配置
    FIXV2 = 'eFixReasonV2'  # 保修项配置,升级版本，增加挪车失败的报修项
    RIDINGCARD = 'ridingCard'  # 骑行卡
    ZERODEPOSIT = 'zeroDeposit'  # 免押金
    FREEORDER = 'freeOrder'  # 免费订单配置
    MOTIONLESS = 'motionless'  # 自动还车、临停配置
    ALARM = 'alarm'  # 异常报警配置
    WALLETCHARGE = 'walletCharge'  # 充值配置
    CHARGESCOPE = 'chargeScope'  # 充值范围
    CHARGESWITCH = 'chargeSwitch'  # 先充值才可用车配置
    COSTEXPLAIN = 'costExplain'  # 计费配置说明
    SHOPPINGMALL = 'shoppingMall'  # 商城配置
    HELPACTICESET = 'helpActiceSet'  # 帮助活动配置
    SHOPPINGMALLSET = 'shoppingMallSet'  # 商城服务配置
    SHOPPINGMALLACTIVESET = 'shoppingMallActiveSet'  # 商城活动配置
    SHOPPINGMALLACTIVESTATESET = 'shoppingMallActiveStateSet'  # 商城活动开关
    BLE = 'ble'  # 强制打开蓝牙
    OFFLINETIME = 'offlineTime'  # 自定义设备超时时长
    SYSTEMMSG = 'systemMsg'  # 系统消息配置
    APPMSG = 'appMsg'  # APP消息配置
    SMSMSG = 'smsMsg'  # 短信消息配置
    OPERATIME = 'operatTime'  # 运营时间端配置
    OPENDEPOSIT = 'openDeposit'  # 开启押金配置
    FREEDEPOSITCARD = 'freeDepositCard'  # 新用户免押金卡活动配置
    FREEDEPOSITCARDONOFF = 'freeDepositCardOnOff'  # 新用户免押金卡活动开关
    STUDENTAUTH = 'studentAuth'  # 学生认证配置
    OUTOFSERVICE = 'outOfService'  # 出服务区配置
    WXLITEREMPLATEID = 'wxliteTemplateId'  # 小程序消息推送
    UNLOCKWITHLESSBALANCE = 'unlockWithLessBanlance'  # 开锁须最低余额配置
    LOCKCONFIG = 'lockConfig'  # 锁车须余额充足配置+自动还车
    AUTOCHANGEBATTREY = 'AutoChangeBattrey'  # 自动换电审核
    AUTOMOVEVEHICLESINGLE = 'AutoMoveVehicleSingle'  # 自动单台挪车审核
    AUTOMOVEVEHICLELIST = 'AutoMoveVehiList'  # 自动批量挪车
    FAVORABLECARDCOST = 'favorableCardCost'  # 优惠卡的计费规则
    SUPERRIDINGCARD = 'super_riding_card'  # 骑行卡的说明规则
    MARKETINGRULES = 'marketingrules'  # 营销活动规则
    FAVORABLECARDRULES = 'favorableCardRules'  # 优惠卡活动规则
    CUSTOMERSERVICEENABLE = 'CustomerServiceEnable'  # 客服开关
    BATTERYCAPACITY = 'batteryCapacity'  # 换电阈值配置


class MsgMethod(MbEnum):
    SYS = 0  # 系统
    APP = 1  # APP
    SMS = 2  # 短信


class MsgType(MbEnum):
    FREEZE = 0  # 冻结
    BLACK = 1  # 拉黑
    ALARM = 2  # 警告
    LOCK = 3  # 临时停车或自动还车
    STUDENT = 4  # 学生认证
    ACTIVITY = 5  # 营销活动奖励 包括固定活动 自定义活动 指定用户活动
    AUTHCHANGE = 6  # 申请换绑
    TASK = 7  # 任务派发
    REPORTPENALTY = 8  # 举报罚金
    DISSENTTICKET = 9  # 异议工单


class MsgTemplate(MbEnum):
    FREEZE_SYS = 0  # 0冻结押金系统通知
    FREEZE_APP = 1  # 1冻结押金APP推送
    FREEZE_SMS = 2  # 2冻结押金短信
    BLACK_SYS = 3  # 3拉黑名单系统通知
    BLACK_APP = 4  # 4拉黑名单APP推送
    BLACK_SMS = 5  # 5拉黑名单短信
    ALARM_SYS = 6  # 6警告系统通知
    ALARM_APP = 7  # 7警告APP推送
    ALARM_SMS = 8  # 8警告短信

    LOCK_SYS = 9  # 9临停或自动还车系统通知
    LOCK_APP = 10  # 10临停或自动还车APP推送
    LOCK_SMS = 11  # 11临停或自动还车警告短信
    ACTIVITY_SYS = 12  # 12学生认证系统通知
    ACTIVITY_APP = 13  # 13学生认证APP推送
    ACTIVITY_SMS = 14  # 14学生认证告警短信
    STUDENT_SYS = 15  # 15营销活动奖励系统通知
    STUDENT_APP = 16  # 16营销活动奖励APP推送
    STUDENT_SMS = 17  # 17营销活动奖励短信通知

    AUTHCHANGE_SYS = 18  # 换绑系统通知
    AUTHCHANGE_APP = 19  # 换绑app推送
    AUTHCHANGE_SMS = 20  # 换绑短信
    TASK_SYS = 21  # 21任务派发系统通知
    TASK_APP = 22  # 22任务派发APP推送
    TASK_SMS = 23  # 23任务派发短信通知
    REPORTPENALTY_SYS = 24  # 举报罚金系统通知
    REPORTPENALTY_APP = 25  # 举报罚金app推送
    REPORTPENALTY_SMS = 26  # 举报罚金短信

    DISSENTTICKET_SYS = 27  # 异议工单系统通知
    DISSENTTICKET_APP = 28  # 异议工单app推送
    DISSENTTICKET_SMS = 29  # 异议工单短信

    def get_msg_type(self):
        msg_type = self.name.split('_')[0]
        return MsgType.__members__[msg_type]

    def get_msg_method(self):
        msg_method = self.name.split('_')[1]
        return MsgMethod.__members__[msg_method]

    @staticmethod
    def initialize(mt: MsgType, mm: MsgMethod):
        """
        子属性生成枚举
        MsgTemplate.initialize(MsgType.ACTIVITY, MsgMethod.APP)
        """
        return MsgTemplate.__members__["_".join([mt.name, mm.name])]


class BigScreenType(MbEnum):
    historical_arrears_pay_back = 1  # 历史欠款补交订单(historical_arrears_pay_back)D
    new_actual_payment = 2  # 新增实收订单(new_actual_payment)E
    new_arrears = 3  # 新增欠款订单(new_arrears)C
    amount_should_accept = 4  # 应收订单总额(amount_should_accept_total)A
    actual_payment = 5  # 实收订单总额(actual_payment)

    order_water = 6  # 订单流水(orderWater)
    order_num = 7  # 订单量(orderNum)
    order_penalty = 8  # 总罚金(orderPenalty)
    charging_order_water = 9  # 充值金额结算 + 赠送金额结算（已支付订单流水）(chargingOrderWater)

    reality_income = 10  # 总收益

    @staticmethod
    def arrears_type_list():
        return (BigScreenType.historical_arrears_pay_back.value, BigScreenType.new_actual_payment.value,
                BigScreenType.new_arrears.value, BigScreenType.amount_should_accept.value,
                BigScreenType.actual_payment.value,)


class AlarmType(MbEnum):
    """异常工单报警类型完善"""
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


class DeviceState(MbEnum):
    """车辆状态, 为空时。设备login但未上架"""

    READY = 1  # 可使用
    RIDING = 2  # 骑行中
    BROKEN = 3  # 设备报修
    BOOKING = 4  # 设备被预约
    OFFLINE = 5  # 设备离线
    CHANGING_BATTERY = 6  # 处于工人换电池阶段，换电中
    DRAG_BACK = 7  # 设备无法现场维修，置为拖回状态
    LOW_BATTERY = 8  # 新增低电量类型车辆，和拖回状态分开

    @staticmethod
    def all_value():
        return [1, 2, 3, 4, 5, 6, 7, 8]

# class UserState(MbEnum):
#     """用户状态表"""
#     SIGN_UP = 1
#     AUTHED = 2
#     READY = 3
#     BOOKING = 4
#     LEAVING = 5  # 离开状态 （临时停车）
#     RIDING = 6
#     TO_PAY = 7
