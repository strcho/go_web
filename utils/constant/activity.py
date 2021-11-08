from . import MbEnum


#  ====================  充值活动  ==================
#  营销活动类型
class MarketingActiveType(MbEnum):
    RECHARGE_ACTIVE = 1  # 充值赠送活动


# 充值赠送活动
class RechargeActiveType(MbEnum):
    NO_ACTIVE = 0  # 默认值，没有活动
    GIVE_AMOUNT = 1  # 赠送金额
    GIVE_RIDING_CARD = 2  # 赠送骑行卡


# 充值赠送活动, 活动状态枚举
class MarketingActiveState(MbEnum):
    CLOSE = 0  # 关闭
    OPEN = 1  # 开启
    DELETE = 2  # 删除

# ==================================================


# 活动状态
class ActivityStatus(MbEnum):
    INIT = 0  # 时间未到
    PROCESSING = 1  # 启用中
    EXPIRE = 2  # 过期

    DISABLE = 3  # 禁用
    DELETE = 4  # 删除


# 奖励类型
class RewardType(MbEnum):
    FREE = 0  # 免单
    RIDING_CARD = 1  # 骑行卡
    BALANCE = 2  # 余额
    DISCOUNT = 3  # 折扣
    DEPOSIT_CARD = 4  # 押金卡


# 活动详情跳转
class BannerRedirect(MbEnum):
    URL = 0  # h5链接
    RICH_TEXT = 1  # 富文本


class FinishType(MbEnum):
    DEPOSIT_CARD = 0  # 完成条件，购买押金卡
    RIDING_CARD = 1  # 购买骑行卡
    RIDING_COUNT = 2  # 骑行次数


# 获得奖励条件类型
class RewardWinType(MbEnum):
    UN_LIMIT = 0
    RANK = 1
    RANDOM = 2


# 活动用户的状态
class ActivityUserStatus(MbEnum):
    JOIN = 0  # 参与中
    FINISH = 1  # 已经完成
    WIN_NOT_GET = 2  # 待领取
    WIN_GET = 3  # 已领取
    FINISH_NOT_WIN = 4  # 未获奖,包括未完成用户


# 完成固定活动的类型
class ActivityFixType(MbEnum):
    ASSIGN_USER_NAME = 0  # 指定用户奖励
    RIGISTER_NAME = 1  # 注册奖励
    AUTH_NAME = 2  # 实名认证奖励
    RECHARGE_NAME = 3  # 完成充值余额
    DEPOSTE_NAME = 4  # 完成购买押金
    DEPOSTE_CARD_NAME = 5  # 完成购买免押金会员卡
    RIDING_CARD_NAME = 6  # 完成购买骑行卡
    EFFECTIVE_NAME = 7  # 完成有效骑行
    ADVERTISING_NAME = 8  # 看广告获奖励
