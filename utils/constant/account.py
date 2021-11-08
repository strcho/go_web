from . import MbEnum

"""
流水类型, 来自XCEBikeAccount.js的SERIAL_TYPE
"""


class SERIAL_TYPE(MbEnum):
    DEPOSIT = 1  # 支付押金
    CHARGE = 2  # 充值钱包
    UNDEPOSIT = 3  # 退还押金
    UNCHARGE = 4  # 退还钱包余额
    MANUAL = 5  # 手动修改      该处属于平台操作用户钱包
    MONTH_CARD = 6  # 购买月卡
    HALF_YEAR_CARD = 7  # 购买半年卡
    YEAR_CARD = 8  # 购买年卡
    DEPOSIT_CARD = 9  # 购买押金卡
    DAY_CARD = 10  # 购买日卡
    WEEK_CARD = 11  # 购买周卡
    BYOUT_CARD = 12  # 购买买断卡
    THREEMTH_CARD = 13  # 购买季卡
    BALANCE_REFUND = 14  # 钱包余额退款
    PLATFORMGIVEWALLET = 15  # 充值赠送钱包
    PLARFORMGIVECARD = 16  # 充值赠送骑行卡
    NEWUSERACTIVITYFREEDEPOSITCARD = 17  # 新用户赠送免押金卡
    REGULAR_ACTIVITY_NO_PAY = 18  # 完成营销活动固定活动送免单
    REGULAR_ACTIVITY_ADD_RINGDINGCARD = 19  # 完成营销活动固定活动送骑行卡
    REGULAR_ACTIVITY_ADD_MONEY = 20  # 完成营销活动固定活动送余额
    REGULAR_ACTIVITY_ADD_DISCOUNT = 21  # 完成营销活动固定活动送折扣
    REGULAR_ACTIVITY_ADD_DEPOSIT = 22  # 完成营销活动固定活动送免押金卡
    TARGET_USER_NO_PAY = 23  # 指定用户奖励送免单
    TARGET_USER_ADD_RINGDINGCARD = 24  # 指定用户奖励送骑行卡
    TARGET_USER_ADD_MONEY = 25  # 指定用户奖励送余额
    TARGET_USER_ADD_DISCOUNT = 26  # 指定用户奖励送折扣
    TARGET_USER_ADD_DEPOSIT = 27  # 指定用户奖励送免押金卡
    CUSTOM_ACTIVITY_NO_PAY = 28  # 完成营销活动自定义活动送免单
    CUSTOM_ACTIVITY_ADD_RINGDINGCARD = 29  # 完成营销活动自定义活动送骑行卡
    CUSTOM_ACTIVITY_ADD_MONEY = 30  # 完成营销活动自定义活动送余额
    CUSTOM_ACTIVITY_ADD_DISCOUNT = 31  # 完成营销活动自定义活动送折扣
    CUSTOM_ACTIVITY_ADD_DEPOSIT = 32  # 完成营销活动自定义活动送免押金卡
    REPORT_PENALTY = 33  # 举报罚金
    DEPOSIT_CARD_REFUND = 200  # 押金卡退款
    RIDING_CARD_REFUND = 201  # 骑行卡退款
    VOUCHER_ACTIVITY_NO_PAY = 300  # 兑换券送免单
    VOUCHER_ACTIVITY_ADD_RINGDINGCARD = 301  # 兑换券送骑行卡
    VOUCHER_ACTIVITY_ADD_MONEY = 302  # 兑换券送余额
    VOUCHER_ACTIVITY_ADD_DISCOUNT = 303  # 兑换券送折扣
    VOUCHER_ACTIVITY_ADD_DEPOSIT = 304  # 兑换券送押金卡
    FAVORABLE_CARD_ADD_PAY = 401  # 购买优惠卡
    FAVORABLE_CARD_REFUND = 402  # 退款优惠卡
    RIDING_DAY_CARD = 310  # 新骑行自定义日卡
    RIDING_COUNT_CARD = 311  # 骑行次卡·
    SUPER_RIDING_CARD_COUNT = 312  # 新次卡·
    SUPER_RIDING_CARD_TIME = 313  # 时长卡·
    SUPER_RIDING_CARD_DISTANCE = 314  # 里程卡·
    SUPER_RIDING_CARD_MONEY = 315  # 减免卡·
    INVITE_ACTIVITY_ADD_RINGDINGCARD = 500  # 邀请有礼送骑行卡
    INVITE_ACTIVITY_ADD_MONEY = 510  # 邀请有礼送余额
    IMPUNITY_PENALTY = 510  # 还车申请免罚罚金

    @staticmethod
    def merchants_wallet_buy():
        return (
            SERIAL_TYPE.CHARGE.value,  # 2
        )

    @staticmethod
    def merchants_riding_card_buy():
        return (
            SERIAL_TYPE.MONTH_CARD.value,  # 6
            SERIAL_TYPE.HALF_YEAR_CARD.value,  # 7
            SERIAL_TYPE.YEAR_CARD.value,  # 8
            SERIAL_TYPE.DAY_CARD.value,  # 10
            SERIAL_TYPE.WEEK_CARD.value,  # 11
            SERIAL_TYPE.BYOUT_CARD.value,  # 12
            SERIAL_TYPE.THREEMTH_CARD.value,  # 13
            SERIAL_TYPE.RIDING_DAY_CARD.value,  # 310
            SERIAL_TYPE.RIDING_COUNT_CARD.value,  # 311
            SERIAL_TYPE.SUPER_RIDING_CARD_COUNT.value,
            SERIAL_TYPE.SUPER_RIDING_CARD_TIME.value,
            SERIAL_TYPE.SUPER_RIDING_CARD_DISTANCE.value,
            SERIAL_TYPE.SUPER_RIDING_CARD_MONEY.value
        )

    @staticmethod
    def merchants_favorable_card_buy():
        return (
            SERIAL_TYPE.FAVORABLE_CARD_ADD_PAY.value,  # 401
        )

    @staticmethod
    def merchants_deposit_buy():
        return (
            SERIAL_TYPE.DEPOSIT.value,  # 1
        )

    @staticmethod
    def merchants_deposit_card_buy():
        return (
            SERIAL_TYPE.DEPOSIT_CARD.value,  # 9
        )

    @staticmethod
    def merchants_wallet_refund():
        return (
            SERIAL_TYPE.BALANCE_REFUND.value,  # 14
        )

    @staticmethod
    def merchants_deposit_card_refund():
        return (
            SERIAL_TYPE.DEPOSIT_CARD_REFUND.value,  # 200
        )

    @staticmethod
    def merchants_riding_card_refund():
        return (
            SERIAL_TYPE.RIDING_CARD_REFUND.value,  # 201
        )

    @staticmethod
    def merchants_favorable_card_refund():
        return (
            SERIAL_TYPE.FAVORABLE_CARD_REFUND.value,  # 402
        )

    @staticmethod
    def merchants_deposit_refund():
        return (
            SERIAL_TYPE.UNDEPOSIT.value,  # 3
        )

    @staticmethod
    def merchants_old_riding_card_buy():
        return (
            SERIAL_TYPE.MONTH_CARD.value,  # 6
            SERIAL_TYPE.HALF_YEAR_CARD.value,  # 7
            SERIAL_TYPE.YEAR_CARD.value,  # 8
            SERIAL_TYPE.DAY_CARD.value,  # 10
            SERIAL_TYPE.WEEK_CARD.value,  # 11
            SERIAL_TYPE.BYOUT_CARD.value,  # 12
            SERIAL_TYPE.THREEMTH_CARD.value  # 13
        )

    @staticmethod
    def merchants_new_riding_card_buy():
        return (
            SERIAL_TYPE.RIDING_DAY_CARD.value,  # 310
            SERIAL_TYPE.RIDING_COUNT_CARD.value,  # 311
            SERIAL_TYPE.SUPER_RIDING_CARD_COUNT.value,
            SERIAL_TYPE.SUPER_RIDING_CARD_TIME.value,
            SERIAL_TYPE.SUPER_RIDING_CARD_DISTANCE.value,
            SERIAL_TYPE.SUPER_RIDING_CARD_MONEY.value
        )

    @staticmethod
    def riding_card_type_days():
        return {
            "6": 30,
            "7": 180,
            "8": 365,
            "10": 1,
            "11": 7,
            "12": 1460,
            "13": 90,
        }

    # 骑行卡赠送
    @staticmethod
    def riding_card_activity():
        return (
            SERIAL_TYPE.PLARFORMGIVECARD.value,  # 16
            SERIAL_TYPE.REGULAR_ACTIVITY_ADD_RINGDINGCARD.value,  # 19
            SERIAL_TYPE.TARGET_USER_ADD_RINGDINGCARD.value,  # 24
            SERIAL_TYPE.CUSTOM_ACTIVITY_ADD_RINGDINGCARD.value,  # 29
            SERIAL_TYPE.VOUCHER_ACTIVITY_ADD_RINGDINGCARD.value,  # 301
            SERIAL_TYPE.INVITE_ACTIVITY_ADD_RINGDINGCARD.value  # 500
        )

    # 会员卡（押金卡）赠送
    @staticmethod
    def deposit_card_activity():
        return (
            SERIAL_TYPE.NEWUSERACTIVITYFREEDEPOSITCARD.value,  # 17
            SERIAL_TYPE.REGULAR_ACTIVITY_ADD_RINGDINGCARD.value,  # 19
            SERIAL_TYPE.TARGET_USER_ADD_RINGDINGCARD.value,  # 24
            SERIAL_TYPE.CUSTOM_ACTIVITY_ADD_DEPOSIT.value,  # 32
            SERIAL_TYPE.VOUCHER_ACTIVITY_ADD_DEPOSIT.value  # 304
        )


"""
骑行卡，钱包的支付渠道，来自XCEBikeAccount.js的CHANNEL_TYPE
"""


class RIDING_CHANNEL_TYPE(MbEnum):
    PLATFORM = 0  # 平台
    ALIPAY = 1  # 支付宝APP支付
    WEPAY = 2  # 微信APP支付
    WXLITE = 3  # 微信小程序支付
    WEPAY_H5 = 4  # 微信H5支付
    UNIONPAY_WXLITE = 5  # 云闪付=>微信小程序支付
    UNIONPAY_CODE = 6  # 云闪付-二维码支付
    UNIONPAY_APP = 7  # 云闪付-APP支付
    PLATFORMGIVE = 8  # 平台赠送
    NEWUSERACTIVITY = 9  # 新用户活动赠送和平台赠送区分开
    REGULAR_ACTIVITY = 10  # 营销活动固定活动
    TARGET_USER = 11  # 指定用户奖励
    CUSTOM_ACTIVITY = 12  # 营销活动自定义活动

    RIDING_CARD_REGULAR_ACTIVITY = 20
    RIDING_CARD_TARGET_USER = 21
    RIDING_CARD_CUSTOM_ACTIVITY = 22
    WX_SCORE = 30  # 微信信用分支付
    ALIPAYLITE = 40  # 支付宝小程序支付
    USER_ADDVOUCHER = 300  # 兑换券活动
    INVITE_ACTIVITY = 500  # 邀请有礼

    @staticmethod
    def from_str(ch: str):
        translate_dict = {"wepay": RIDING_CHANNEL_TYPE.WEPAY,
                          "alipay": RIDING_CHANNEL_TYPE.ALIPAY,
                          "wxlite": RIDING_CHANNEL_TYPE.WXLITE,
                          "alipaylite": RIDING_CHANNEL_TYPE.ALIPAYLITE,
                          "unionpayOfApp": RIDING_CHANNEL_TYPE.UNIONPAY_APP,
                          "unionpay": RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE,
                          "unionpayOfCode": RIDING_CHANNEL_TYPE.UNIONPAY_CODE
                          }
        return translate_dict[ch]

    @staticmethod
    def riding_card_fake_income_type():
        return [RIDING_CHANNEL_TYPE.PLATFORM.value,  # 0
                RIDING_CHANNEL_TYPE.PLATFORMGIVE.value,  # 8
                RIDING_CHANNEL_TYPE.NEWUSERACTIVITY.value,  # 9
                RIDING_CHANNEL_TYPE.REGULAR_ACTIVITY.value,  # 10
                RIDING_CHANNEL_TYPE.TARGET_USER.value,  # 11
                RIDING_CHANNEL_TYPE.CUSTOM_ACTIVITY.value,  # 12
                RIDING_CHANNEL_TYPE.RIDING_CARD_REGULAR_ACTIVITY.value,  # 20
                RIDING_CHANNEL_TYPE.RIDING_CARD_TARGET_USER.value,  # 21
                RIDING_CHANNEL_TYPE.RIDING_CARD_CUSTOM_ACTIVITY.value,  # 22
                RIDING_CHANNEL_TYPE.USER_ADDVOUCHER.value,  # 300
                RIDING_CHANNEL_TYPE.INVITE_ACTIVITY.value  # 500
                ]

    @staticmethod
    def get_weixin_channel():
        """微信支付渠道（2，3，4，30）"""
        return (RIDING_CHANNEL_TYPE.WEPAY.value,
                RIDING_CHANNEL_TYPE.WXLITE.value,
                RIDING_CHANNEL_TYPE.WEPAY_H5.value,
                RIDING_CHANNEL_TYPE.WX_SCORE.value)

    @staticmethod
    def get_alipay_channel():
        """支付宝渠道（1，40）"""
        return (RIDING_CHANNEL_TYPE.ALIPAY.value,
                RIDING_CHANNEL_TYPE.ALIPAYLITE.value)

    @staticmethod
    def get_unionpay_channel():
        """银联渠道（5，6，7）"""
        return (RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE.value,
                RIDING_CHANNEL_TYPE.UNIONPAY_CODE.value,
                RIDING_CHANNEL_TYPE.UNIONPAY_APP.value)

    @staticmethod
    def get_all_apy_channel():
        """所有支付渠道"""
        return (RIDING_CHANNEL_TYPE.ALIPAY.value,
                RIDING_CHANNEL_TYPE.ALIPAYLITE.value,
                RIDING_CHANNEL_TYPE.WEPAY.value,
                RIDING_CHANNEL_TYPE.WXLITE.value,
                RIDING_CHANNEL_TYPE.WEPAY_H5.value,
                RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE.value,
                RIDING_CHANNEL_TYPE.UNIONPAY_CODE.value,
                RIDING_CHANNEL_TYPE.UNIONPAY_APP.value)


"""
押金卡支付渠道，来自XCEBikeAccount.js的CHANNEL_TYPE
"""


class DEPOSIT_CHANNEL_TYPE(MbEnum):
    ALIPAY = 0  # 支付宝支付
    WXLITE = 1  # 微信小程序支付
    NEWUSERACTIVITY = 2  # 新用户活动赠送
    REGULAR_ACTIVITY = 3  # 营销活动固定活动活动赠送
    TARGET_USER = 4  # 指定用户奖励赠送
    UNIONPAY_WXLITE = 5  # 云闪付 = > 微信小程序支付
    UNIONPAY_CODE = 6  # 云闪付 - 二维码支付
    UNIONPAY_APP = 7  # 云闪付 - APP支付
    ALI_LITE = 40  # 支付宝小程序

    CUSTOM_ACTIVITY = 101  # 营销活动自定义活动赠送
    PLATFORM_REFUND = 200  # 平台自定义押金卡退款, 用于兼容流水展示, 实际的金额
    ADD_VOUCHER = 300  # 兑换卷赠送

    @staticmethod
    def deposit_all_fake_income_type():
        return [DEPOSIT_CHANNEL_TYPE.NEWUSERACTIVITY.value,
                DEPOSIT_CHANNEL_TYPE.REGULAR_ACTIVITY.value,
                DEPOSIT_CHANNEL_TYPE.TARGET_USER.value,
                DEPOSIT_CHANNEL_TYPE.CUSTOM_ACTIVITY.value]


# 押金卡
class STATISTICS_TYPES(MbEnum):
    SERIAL_TYPE.DEPOSIT_CARD  # 押金卡


# 统计的渠道全是用户实打实支付的渠道不包括赠送的
class STATISTICS_CHANNELS(MbEnum):
    RIDING_CHANNEL_TYPE.ALIPAY  # 支付宝
    RIDING_CHANNEL_TYPE.WEPAY  # 微信APP支付
    RIDING_CHANNEL_TYPE.WXLITE  # 微信小程序支付
    RIDING_CHANNEL_TYPE.WEPAY_H5  # 微信H5支付
    RIDING_CHANNEL_TYPE.UNIONPAY_WXLITE  # 云闪付=>微信小程序支付
    RIDING_CHANNEL_TYPE.UNIONPAY_CODE  # 云闪付=>微信小程序支付
    RIDING_CHANNEL_TYPE.UNIONPAY_APP  # 云闪付-APP支付


class PAY_TYPE(MbEnum):
    ITINERARY = 1  # 行程支付
    WALLET = 2  # 钱包充值
    ITINERARY_REFOUND = 3  # 行程退款
    MANUAL_ENTRY = 4  # 人工修改
    BALANCE_REFUND = 5  # 余额退款
    GIVEWALLET = 6  # 钱包赠送
    REGULAR_ACTIVITY = 7  # 营销活动固定活动赠送
    TARGET_USER = 8  # 指定用户奖励
    CUSTOM_ACTIVITY = 9  # 营销活动自定义活动赠送
    REPORT_PENALTY = 100  # 举报罚金
    VOUCHER_USER = 300  # 兑换券
    INVITE_ACTIVITY = 500  # 邀请有礼
    IMPUNITY_PENALTY = 510  # 还车申请免罚罚金

    @staticmethod
    def wallet_reality_income_type():
        return [PAY_TYPE.WALLET.value,
                PAY_TYPE.BALANCE_REFUND.value]

    @staticmethod
    def activity_giving_type():
        return (PAY_TYPE.REGULAR_ACTIVITY.value,
                PAY_TYPE.TARGET_USER.value,
                PAY_TYPE.CUSTOM_ACTIVITY.value,
                PAY_TYPE.VOUCHER_USER.value,
                PAY_TYPE.INVITE_ACTIVITY.value)


class DEPOSIT_CONFIG_TYPE(MbEnum):
    DEPOSIT_CARD = 0
    ZHIMA = 1


class MERCHANTS_PAY(MbEnum):
    WX_PAY = 1
    ALI_PAY = 2
    UNION_PAY = 3


class DeductionType(MbEnum):
    TIME = 1
    DISTANCE = 2
    MONEY = 3
    COUNT = 4

    def get_serial_type(self):
        if self.value == 1:
            return SERIAL_TYPE.SUPER_RIDING_CARD_TIME.value
        elif self.value == 2:
            return SERIAL_TYPE.SUPER_RIDING_CARD_DISTANCE.value
        elif self.value == 3:
            return SERIAL_TYPE.SUPER_RIDING_CARD_MONEY.value
        elif self.value == 4:
            return SERIAL_TYPE.SUPER_RIDING_CARD_COUNT.value


class RidingCardConfigState(MbEnum):
    DISABLE = 0
    ENABLE = 1
    DELETE = 2


class UserRidingCardState(MbEnum):
    USING = 1
    EXPIRED = 2
    DELETE = 3
