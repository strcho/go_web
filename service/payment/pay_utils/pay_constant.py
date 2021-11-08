from mbutils import cfg
from mbutils.constant import MbEnum

RidingCardName = {
    6: '月卡',
    7: '半年卡',
    8: '年卡',
    10: '日卡',
    11: '周卡',
    12: '买断卡',
    13: '季卡'
}

RidingTypes = ['6', '7', '8', '10', '11', '12', '13']


class PAY_CONFIG():
    OLDWEPAY_CFG = cfg.get("oldWepay", {})
    WEPAY_CFG = cfg.get("wepay", {})
    WXLITE_CFG = cfg.get("wx_lite", {})
    ALI_CFG = cfg.get("alipay", {})
    UNIONPAY_CONFIG = cfg.get('UnionpayConfig', {})


class PAYMENT_CONFIG_CHANNEL(MbEnum):
    DEPOSITE = "deposite"
    DEPOSITECARD = "depositeCard"
    WALLET = "wallet"
    RIDINGCARD = "ridingCard"
    FAVORABLECARD = "favorableCard"


RECHARGE_ACTIVETYPE = {
    "noActive": 0,  # 默认值,无活动
    "giveAmount": 1,  # 赠送金额
    "giveRidingCard": 2  # 赠送骑行卡
}

WX_SUCCESS_XML = """<xml>
    <return_code><![CDATA[SUCCESS]]></return_code>
    <return_msg><![CDATA[OK]]></return_msg>
</xml>"""

WX_FAILED_XML = """<xml>
    <return_code><![CDATA[FAIL]]></return_code>
    <return_msg><![CDATA[INVALID]]></return_msg>
</xml>"""

WX_ERROR_XML = """<xml>
    <return_code><![CDATA[FAIL]]></return_code>
    <return_msg><![CDATA[ERROR]]></return_msg>
</xml>"""

ALI_SUCCESS_RESP = "success"
ALI_FAILED_RESP = "fail"

UNION_SUCCESS_RESP = "ok"
UNION_FAILED_RESP = "fail"  # not ok

CONFIG_NAME = {
    "HELP": 'helpSet',  # 帮助页配置
    "FIX": 'eFixReason',  # 保修项配置
    "DEPOSIT": 'depositSet',  # 押金配置
    "TEL": 'telephoneSet',  # 客服电话配置
    "COST": 'costSet',  # 计费配置
    "FIXV2": 'eFixReasonV2',  # 保修项配置,  升级版本，增加挪车失败的报修项
    "RIDINGCARD": 'ridingCard',  # 骑行卡
    "ZERODEPOSIT": 'zeroDeposit',  # 免押金
    "FREEORDER": 'freeOrder',  # 免费订单配置
    "MOTIONLESS": 'motionless',  # 自动还车、临停配置
    "ALARM": 'alarm',  # 异常报警配置
    "WALLETCHARGE": 'walletCharge',  # 充值配置
    "CHARGESCOPE": 'chargeScope',  # 充值范围
    "CHARGESWITCH": 'chargeSwitch',  # 先充值才可用车配置
    "COSTEXPLAIN": 'costExplain',  # 计费配置说明
    "SHOPPINGMALL": 'shoppingMall',  # 商城配置
    "HELPACTICESET": 'helpActiceSet',  # 帮助活动配置
    "SHOPPINGMALLSET": 'shoppingMallSet',  # 商城服务配置
    "SHOPPINGMALLACTIVESET": 'shoppingMallActiveSet',  # 商城活动配置
    "SHOPPINGMALLACTIVESTATESET": 'shoppingMallActiveStateSet',  # 商城活动开关
    "BLE": 'ble',  # 强制打开蓝牙
    "OFFLINETIME": 'offlineTime',  # 自定义设备超时时长
    "SYSTEMMSG": 'systemMsg',  # 系统消息配置
    "APPMSG": 'appMsg',  # APP消息配置
    "SMSMSG": 'smsMsg',  # 短信消息配置
    "OPERATIME": 'operatTime',  # 运营时间端配置
    "OPENDEPOSIT": 'openDeposit',  # 开启押金配置
    "FREEDEPOSITCARD": 'freeDepositCard',  # 新用户免押金卡活动配置
    "FREEDEPOSITCARDONOFF": 'freeDepositCardOnOff',  # 新用户免押金卡活动开关
    "STUDENTAUTH": 'studentAuth',  # 学生认证配置
    "OUTOFSERVICE": 'outOfService',  # 出服务区配置
    "WXLITEREMPLATEID": 'wxliteTemplateId',  # 小程序消息推送
    "UNLOCKWITHLESSBALANCE": 'unlockWithLessBanlance',  # 开锁须最低余额配置
    "LOCKCONFIG": 'lockConfig',  # 锁车须余额充足配置+自动还车
    "AUTOCHANGEBATTREY": 'AutoChangeBattrey',  # 自动换电审核
    "AUTOMOVEVEHICLESINGLE": 'AutoMoveVehicleSingle',  # 自动单台挪车审核
    "AUTOMOVEVEHICLELIST": 'AutoMoveVehiList',  # 自动批量挪车
    "TBEACONCONFIG": 'tBeacon',  # 道钉控制（精准停车）
    "CUSTOMSERVICE": 'customService',  # 客服中心内容配置
    "WXLITEREMPLATESW": 'wxliteTemplateSW',  # 小程序消息推送开关
    "BATTERYCAPACITY": 'batteryCapacity',  # 换电阈值配置
    "PILE4RETURN": 'pile4return',  # 满桩还车配置
    "FAVORABLECARDCOST": 'favorableCardCost',  # 优惠卡的计费规则
    "BANNERSWITCH": 'bannerSwitcher',  # 广告开关位
    "BANNERINFO": 'bannerInfo',  # 广告配置信息
    "RETURN_BIKE": 'returnBikeConfig',  # 还车设置
    "USERVERIFYFACE": 'userVerifyFace',  # 人脸识别配置
    "MARGINCOVERAUTOPAY": 'marginCoverAutoPay',  # 骑行订单补差额自动支付
}
