from .riding_card_view import *
from .wallet_view import *
from .favorable_card_view import *
from .deposit_card_view import *
from .deposit_view import *

urls = [
        (r'/ridingCard', CreateRidingCardOrderHandler),  # 骑行卡支付
        # 不同支付渠道的notify, 不对内进行调用
        (r'/ridingCard/wepayNotify', NotifyWepayRindingCardOrderHandler),  # 接收微信购买骑行卡支付回调
        (r'/ridingCard/wxliteNotify', NotifyWxLitePayRindingCardOrderHandler),  # 接收微信小程序购买骑行卡支付回调
        (r'/ridingCard/alipayNotify', NotifyAliPayRidingCardOrderHandler),  # 接收支付宝购买骑行卡支付回调
        (r'/ridingCard/alipayliteNotify', NotifyAliLitePayRidingCardOrderHandler),  # 接收支付宝小程序购买骑行卡支付回调
        (r'/ridingCard/unionpayNotify', NotifyUnionLiteRidingCardOrderHandler),  # 接收云闪付=>微信小程序支付购买骑行卡的回调
        (r'/ridingCard/unionpayAppNotify', NotifyUnionAppRidingCardOrderHandler),   # 接收云闪付-APP支付购买骑行卡的回调
        (r'/ridingCard/unionpayCodeNotify', NotifyUnionCodeRidingCardOrderHandler),  # 接收云闪付-二维码支付购买骑行卡的回调
        (r'/ridingCard/refund', RidingCardRefundHandler),  # 退款

        (r'/depositCard', CreateDepositCardOrderHandler),  # 购买会员卡
        # 不同支付渠道的notify, 不对内进行调用
        (r'/depositCard/wepayNotify', NotifyWepayDepositCardOrderHandler),  # 接收微信购买会员卡支付回调
        (r'/depositCard/wxliteNotify', NotifyWxLitePayDepositCardOrderHandler),  # 接收微信小程序购买会员卡支付回调
        (r'/depositCard/alipayNotify', NotifyAliPayDepositCardOrderHandler),  # 接收支付宝购买会员卡支付回调
        (r'/depositCard/alipayliteNotify', NotifyAliLitePayDepositCardOrderHandler),  # 接收支付宝小程序购买会员卡支付回调
        (r'/depositCard/unionpayNotify', NotifyUnionLiteDepositCardOrderHandler),  # 接收云闪付=>微信小程序支付购买会员卡的回调
        (r'/depositCard/unionpayAppNotify', NotifyUnionAppDepositCardOrderHandler),  # 接收云闪付-APP支付购买会员卡的回调
        (r'/depositCard/unionpayCodeNotify', NotifyUnionCodeDepositCardOrderHandler),  # 接收云闪付-二维码支付购买会员卡的回调
        (r'/depositCard/refund', RefundDepositCardHandler),  # 退款

        (r'/deposit', CreateDepositOrderHandler),  # 购买押金
        (r'/deposit/wepayNotify', NotifyWepayDepositHandler),  # 接收微信购买押金支付回调
        (r'/deposit/wxliteNotify', NotifyWxLitePayDepositOrderHandler),  # 接收微信小程序购买押金支付回调
        (r'/deposit/alipayNotify', NotifyAliPayDepositOrderHandler),  # 接收支付宝购买押金支付回调
        (r'/deposit/alipayliteNotify', NotifyAliLitePayDepositOrderHandler),  # 接收支付宝小程序购买押金支付回调
        (r'/deposit/unionpayNotify', NotifyUnionLiteDepositOrderHandler),  # 接收云闪付=>微信小程序支付购买押金的回调
        (r'/deposit/unionpayAppNotify', NotifyUnionAppDepositOrderHandler),  # 接收云闪付-APP支付购买押金的回调
        (r'/deposit/unionpayCodeNotify', NotifyUnionCodeDepositOrderHandler),  # 接收云闪付-二维码支付购买会员卡的回调
        (r'/deposit/refund', RefundDepositHandler),  # 退款

        (r'/wallet', PaymentWalletHandler),  # 购买
        (r'/wallet/alipayNotify', NotifyAliWalletHandler),  # 支付宝回调
        (r'/wallet/wepayNotify', NotifyWepayWalletHandler),  # 微信app支付回调
        (r'/wallet/wxliteNotify', NotifyWxLiteWalletHandler),  # 微信小程序回调
        (r'/wallet/unionpayNotify', NotifyUnionWxLiteWalletHandler),  # 银联微信小程序支付回调
        (r'/wallet/unionpayCodeNotify', NotifyUnionCodeWalletHandler),  # 银联兑换码回调
        (r'/wallet/unionpayAppNotify', NotifyUnionAPPWalletHandler),  # 银联app支付回调
        (r'/wallet/alipayliteNotify', NotifyAliLIteWalletHandler),  # 支付宝小程序回调
        (r'/wallet/balance/refund', PaymentWalletBalanceRefundHandler),  # 退款

        (r'/favorableCard', CreateFavorableCardOrderHandler),  # 优惠卡支付 创建购买
        (r'/favorableCard/alipayNotify', NotifyAliFavorableCardHandler),  # 优惠卡支付（支付宝回调）
        (r'/favorableCard/wepayNotify', NotifyWepayFavorableCardHandler),  # 优惠卡支付 微信app支付回调
        (r'/favorableCard/wxliteNotify', NotifyWeLiteFavorableCardHandler),  # 优惠卡支付 微信小程序回调
        (r'/favorableCard/unionpayNotify', NotifyUnionWXLiteFavorableCardHandler),  # 优惠卡支付 银联微信小程序支付回调
        (r'/favorableCard/unionpayCodeNotify', NotifyUnionCodeFavorableCardHandler),  # 优惠卡支付 银联兑换码回调
        (r'/favorableCard/unionpayAppNotify', NotifyUnionAPPFavorableCardHandler),  # 优惠卡支付 银联app支付回调
        (r'/favorableCard/alipayliteNotify', NotifyAliLiteFavorableCardHandler),  # 优惠卡支付 支付宝小程序回调
        (r'/favorableCard/refund', FavorableCardRefundHandler),  # 退款

]




