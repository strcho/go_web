from model.all_model import *
from mbutils import dao_session, logger
from utils.constant.account import SERIAL_TYPE, RIDING_CHANNEL_TYPE, DEPOSIT_CHANNEL_TYPE
from . import ScreenService


class BigScreenMerchantService(ScreenService):

    # 根据不同的支付类型做区分，设计统一通过时间和服务区进行过滤
    def __init__(self, op_area_ids: tuple, start_time: datetime, end_time: datetime, merchants_channel):
        """
        @param op_area_ids: 服务区id
        @param start_time: 开始时间（时间类型）
        @param end_time: 结束时间（时间类型）
        @param merchants_channel: 支付渠道（微信，支付宝，银联）
        @param merchants_type: 支付类型（购买，退款，赠送）
        """
        super().__init__(op_area_ids)
        self.start_time = start_time
        self.end_time = end_time
        self.merchants_channel = merchants_channel

    def query_merchant(self, merchant, merchants_type):
        if merchant == "wallet":
            return self.query_merchants_wallet(merchants_type)
        elif merchant == "riding_card":
            return self.query_merchants_riding_card(merchants_type)
        elif merchant == "deposit_card":
            return self.query_merchants_deposit_card(merchants_type)
        elif merchant == "favorable_card":
            return self.query_merchants_favorable_card(merchants_type)
        elif merchant == "deposit":
            return self.query_merchants_deposit(merchants_type)
        else:
            return 0, 0

    # 商户统计钱包收入
    def query_merchants_wallet(self, merchants_type):
        try:
            m2 = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserWalletRecord.change), 0) / 100,
                                                 func.ifnull(func.count(XcEbikeAccount2.serialNo), 0)). \
                prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
                join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeAccount2.serialNo)
            m2 = m2.with_hint(XcEbikeUserWalletRecord, "force index(idx_createdAt_serviceId)")
            if self.op_area_ids:
                m2 = m2.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            if self.merchants_channel:
                m2 = m2.filter(XcEbikeAccount2.channel.in_(self.merchants_channel))
            m1 = m2.filter(XcEbikeAccount2.type.in_(merchants_type),
                           XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time)).first()
        except Exception as e:
            logger.error("wallet table index: idx_createdAt_serviceId not exit")
            m3 = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeUserWalletRecord.change), 0) / 100,
                                                 func.ifnull(func.count(XcEbikeAccount2.serialNo), 0)). \
                prefix_with("/*+JOIN_PREFIX(xc_ebike_user_wallet_record) */"). \
                join(XcEbikeUserWalletRecord, XcEbikeUserWalletRecord.orderId == XcEbikeAccount2.serialNo)
            if self.op_area_ids:
                m3 = m3.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
            if self.merchants_channel:
                m3 = m3.filter(XcEbikeAccount2.channel.in_(self.merchants_channel))
            m1 = m3.filter(XcEbikeAccount2.type.in_(merchants_type),
                           XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time)).first()
        return abs(m1[0]), m1[1]

    # 商户统计会员卡收入
    def query_merchants_deposit_card(self, merchants_type):
        """XcEbikeAccount2.amount使用amount,而不是XcEbike2DepositCard.money"""
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100,
                                            func.ifnull(func.count(XcEbikeAccount2.serialNo), 0)). \
            prefix_with("/*+JOIN_PREFIX(xc_ebike_account_2) */"). \
            join(XcEbike2DepositCard, XcEbikeAccount2.trade_no == XcEbike2DepositCard.trade_no)
        if self.op_area_ids:
            m = m.filter(XcEbike2DepositCard.serviceId.in_(self.op_area_ids))
        if self.merchants_channel:
            m = m.filter(XcEbikeAccount2.channel.in_(self.merchants_channel))
        m = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                     XcEbikeAccount2.createdAt.between(self.start_time, self.end_time),
                     XcEbikeAccount2.trade_no != "").first()
        return abs(m[0]), m[1]

    # 商户统计骑行卡收入
    def query_merchants_riding_card(self, merchants_type):
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcEbike2RidingcardAccount.money), 0) / 100,
                                            func.ifnull(func.count(XcEbike2RidingcardAccount.id), 0))
        if self.op_area_ids:
            m = m.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        if self.merchants_channel:
            m = m.filter(XcEbike2RidingcardAccount.channel.in_(self.merchants_channel))
        else:
            m = m.filter(XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        m = m.filter(XcEbike2RidingcardAccount.type.in_(merchants_type),
                     XcEbike2RidingcardAccount.createdAt.between(self.start_time, self.end_time)).first()
        return abs(m[0]), m[1]

    # 商户统计优惠卡收入
    def query_merchants_favorable_card(self, merchants_type):
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcMieba2FavorableCardAccount.price), 0) / 100,
                                            func.ifnull(func.count(XcMieba2FavorableCardAccount.id), 0))
        if self.op_area_ids:
            m = m.filter(XcMieba2FavorableCardAccount.service_id.in_(self.op_area_ids))
        if self.merchants_channel:
            m = m.filter(XcMieba2FavorableCardAccount.channel.in_(self.merchants_channel))
        m = m.filter(XcMieba2FavorableCardAccount.serial_type.in_(merchants_type),
                     XcMieba2FavorableCardAccount.created_at.between(self.start_time, self.end_time)).first()
        return abs(m[0]), m[1]

    # 商户统计诚信金收入
    def query_merchants_deposit(self, merchants_type):
        m = dao_session.sub_session().query(func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100,
                                            func.ifnull(func.count(XcEbikeAccount2.amount), 0)). \
            join(XcEbikeUsrs2, XcEbikeUsrs2.id == XcEbikeAccount2.objectId)
        if self.op_area_ids:
            m = m.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        if self.merchants_channel:
            m = m.filter(XcEbikeAccount2.channel.in_(self.merchants_channel))
        m = m.filter(XcEbikeAccount2.type.in_(merchants_type),
                     XcEbikeAccount2.createdAt.between(self.start_time, self.end_time)).first()
        return abs(m[0]), m[1]

    def query_merchants_history(self, merchant_type: int, is_refund: int):
        """
        获取商户相关的缓存数据（钱包，会员卡，骑行卡，优惠卡，会费（押金））
        @param merchant_type: 微信(1)，支付宝(2)，银联(3)
        @param is_refund: 是否为退款（退款（1），购买（0））
        @return:获取时间聚合后的数据, 两个字典类型的数据返回：1. 按时间聚合的数据，2.累积数据
        """
        m = dao_session.sub_session().query(
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.wallet), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.riding_card), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.deposit_card), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.favorable_card), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.deposit), 0) / 100,
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.wallet_num), 0),
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.riding_card_num), 0),
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.deposit_card_num), 0),
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.favorable_card_num), 0),
            func.ifnull(func.sum(XcMieba2BigScreenMerchant.deposit_num), 0),
            func.date_format(XcMieba2BigScreenMerchant.day_time, "%Y-%m-%d").label("ctime"))
        if self.op_area_ids:
            m = m.filter(XcMieba2BigScreenMerchant.service_id.in_(self.op_area_ids))
        if merchant_type:
            m = m.filter(XcMieba2BigScreenMerchant.merchant_type == merchant_type)
        m = m.filter(XcMieba2BigScreenMerchant.refund == is_refund,
                     XcMieba2BigScreenMerchant.day_time.between(self.start_time, self.end_time)). \
            group_by("ctime").all()
        w_t, r_c_t, d_c_t, f_c_t, d_t, w_n_t, r_c_n_t, d_c_n_t, f_c_n_t, d_n_t = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        data_dict, total_dict = {}, {}
        for w, r_c, d_c, f_c, d, w_n, r_c_n, d_c_n, f_c_n, d_n, day_time in m:
            data_dict[str(day_time)] = {
                "wallet": w, "riding_card": r_c, "deposit_card": d_c, "favorable_card": f_c, "deposit": d,
                "wallet_num": w_n, "riding_card_num": r_c_n, "deposit_card_num": d_c_n, "favorable_card_num": f_c_n,
                "deposit_num": d_n
            }
            w_t += w  # 计算钱包累积的金额
            r_c_t += r_c  # 计算会员卡累积的金额
            d_c_t += d_c  # 计算骑行卡累积的金额
            f_c_t += f_c  # 计算优惠卡累积的金额
            d_t += d  # 计算会费累积的金额
            w_n_t += w_n  # 计算钱包累积的交易数量
            r_c_n_t += r_c_n  # 计算骑行卡累积的交易数量
            d_c_n_t += d_c_n  # 计算会员卡累积的交易数量
            f_c_n_t += f_c_n  # 计算优惠卡累积的交易数量
            d_n_t += d_n  # 计算会费累积的交易数量
        total_dict = {
            "wallet_total": w_t, "riding_card_total": r_c_t, "deposit_card_total": d_c_t, "favorable_card_total": f_c_t,
            "deposit_total": d_t, "wallet_num_total": w_n_t, "riding_card_num_total": r_c_n_t,
            "deposit_card_num_total": d_c_n_t, "favorable_card_num_total": f_c_n_t, "deposit_num_total": d_n_t
        }
        return data_dict, total_dict

    # 新版骑行卡购买次数
    def query_riding_card_new_times(self):
        """
        骑行卡新版购买次数
        新增骑行卡次卡，增加了每种骑行卡的次数
        """
        money_times = dao_session.session().query(
            func.count(XcEbike2RidingcardAccount.id), XcEbike2RidingcardAccount.money,
            XcEbike2RidingcardAccount.ridingCardName). \
            filter(XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_new_riding_card_buy()),
                   XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        if self.op_area_ids:
            money_times = money_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        money_times = money_times.filter(XcEbike2RidingcardAccount.createdAt.between(self.start_time, self.end_time)). \
            group_by(XcEbike2RidingcardAccount.ridingCardName, XcEbike2RidingcardAccount.money).all()
        money_list = []
        for m in money_times:
            r_count, r_money, r_name = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}".format('%.2f' % (float(r_money) / 100), r_name)}
                money_list.append(m_dict)
        return money_list

    # 老版骑行卡售卖次数
    def query_riding_card_old_times(self):
        """
        骑行卡老版售卖(赠送)次数
        """
        money_times = dao_session.session().query(
            func.count(XcEbike2RidingcardAccount.id), XcEbike2RidingcardAccount.money, XcEbike2RidingcardAccount.type). \
            filter(XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_old_riding_card_buy()),
                   XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        if self.op_area_ids:
            money_times = money_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        money_times = money_times.filter(XcEbike2RidingcardAccount.createdAt.between(self.start_time, self.end_time)). \
            group_by(XcEbike2RidingcardAccount.type, XcEbike2RidingcardAccount.money).all()
        money_list = []
        for m in money_times:
            r_count, r_money, r_type = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}天骑行卡".format(
                    '%.2f' % (float(r_money) / 100), SERIAL_TYPE.riding_card_type_days().get(str(r_type), 0))}
                money_list.append(m_dict)
        return money_list

    # 旧版骑行卡的售卖次数柱状图
    def query_riding_card_sell_times_old(self):
        m_times = dao_session.session().query(
            func.date_format(XcEbike2RidingcardAccount.createdAt, "%Y-%m-%d").label("ctime"),
            func.ifnull(func.count(XcEbike2RidingcardAccount.id), 0),
            XcEbike2RidingcardAccount.money, XcEbike2RidingcardAccount.type).filter(
            XcEbike2RidingcardAccount.createdAt.between(self.start_time, self.end_time),
            XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_old_riding_card_buy()),
            XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type())
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by(XcEbike2RidingcardAccount.type, XcEbike2RidingcardAccount.money, "ctime").all()
        total_list = []
        for m in m_times:
            r_day, r_count, r_money, r_type = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}天骑行卡".format(
                    '%.2f' % (float(r_money) / 100), SERIAL_TYPE.riding_card_type_days().get(str(r_type), 0)),
                          "date": r_day}
                total_list.append(m_dict)
        return total_list

    # 新版骑行卡次卡的售卖次数柱状图
    def query_riding_card_sell_times_new(self):
        """
        新版骑行卡次卡的售卖次数（柱状图）
        """
        m_times = dao_session.session().query(
            func.date_format(XcEbike2RidingcardAccount.createdAt, "%Y-%m-%d").label("ctime"),
            func.ifnull(func.count(XcEbike2RidingcardAccount.id), 0),
            XcEbike2RidingcardAccount.money, XcEbike2RidingcardAccount.ridingCardName).filter(
            XcEbike2RidingcardAccount.createdAt.between(self.start_time, self.end_time),
            XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_new_riding_card_buy()),
            XcEbike2RidingcardAccount.channel.notin_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type())
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by(XcEbike2RidingcardAccount.ridingCardName, XcEbike2RidingcardAccount.money,
                                   "ctime").all()
        total_list = []
        for m in m_times:
            r_day, r_count, r_money, r_name = m
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "name": "{}元{}".format('%.2f' % (float(r_money) / 100), r_name),
                          "date": r_day}
                total_list.append(m_dict)
        return total_list

    # 会员（押金）卡的售卖次数
    def query_deposit_card_sell_times(self):
        m_times = dao_session.session().query(
            func.ifnull(func.count(XcEbike2DepositCard.id), 0), XcEbike2DepositCard.money).filter(
            XcEbike2DepositCard.createdAt.between(self.start_time, self.end_time),
            XcEbike2DepositCard.channel.notin_(DEPOSIT_CHANNEL_TYPE.deposit_all_fake_income_type())
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbike2DepositCard.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by(XcEbike2DepositCard.money).all()
        total_list = []
        for r_count, r_money in m_times:
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "money": round(float(r_money) / 100, 2)}
                total_list.append(m_dict)
        return total_list

    # 会员（押金）卡的售卖次数
    def query_deposit_card_sell_times_day(self):
        m_times = dao_session.session().query(
            func.date_format(XcEbike2DepositCard.createdAt, "%Y-%m-%d").label("ctime"),
            func.ifnull(func.count(XcEbike2DepositCard.id), 0),
            XcEbike2DepositCard.money, XcEbike2DepositCard.days).filter(
            XcEbike2DepositCard.createdAt.between(self.start_time, self.end_time),
            XcEbike2DepositCard.channel.notin_(DEPOSIT_CHANNEL_TYPE.deposit_all_fake_income_type())
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbike2DepositCard.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by(XcEbike2DepositCard.days, XcEbike2DepositCard.money, "ctime").all()
        total_dict = {}
        for days, r_count, r_money, r_day in m_times:
            money = round(float(r_money) / 100, 2)
            if money >= 0.2:
                d_dict = {"time": r_count, "money": money, "date": r_day}
                if days in total_dict:
                    total_dict[days].append(d_dict)
                else:
                    total_dict[days] = [d_dict]
        return total_dict

    # 会员（押金）卡的售卖次数
    def query_deposit_card_sell_times_days(self):
        m_times = dao_session.session().query(
            func.ifnull(func.count(XcEbike2DepositCard.id), 0), XcEbike2DepositCard.money,
            XcEbike2DepositCard.days).filter(
            XcEbike2DepositCard.createdAt.between(self.start_time, self.end_time),
            XcEbike2DepositCard.channel.notin_(DEPOSIT_CHANNEL_TYPE.deposit_all_fake_income_type())
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbike2DepositCard.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by(XcEbike2DepositCard.money, XcEbike2DepositCard.days).all()
        total_list = []
        for r_count, r_money, r_days in m_times:
            if float(r_money) / 100 >= 0.2:
                m_dict = {"times": r_count, "money": round(float(r_money) / 100, 2), "days": r_days}
                total_list.append(m_dict)
        return total_list

    def query_favorable_card_sell_times(self):
        """
        优惠卡的流水统计
        """
        account = dao_session.session().query(func.count(XcMieba2FavorableCardAccount.price).label("count"),
                                              XcMieba2FavorableCard.present_price, XcMieba2FavorableCard.card_time). \
            join(XcMieba2FavorableCard, XcMieba2FavorableCard.id == XcMieba2FavorableCardAccount.card_id). \
            filter(XcMieba2FavorableCardAccount.price > 0).group_by(XcMieba2FavorableCardAccount.card_id)
        card_price = dao_session.session().query(func.distinct(XcMieba2FavorableCard.present_price))
        if self.op_area_ids:
            account = account.filter(XcMieba2FavorableCardAccount.service_id.in_(self.op_area_ids))
            card_price = card_price.filter(XcMieba2FavorableCard.service_id.in_(self.op_area_ids))
        account = account.group_by(XcMieba2FavorableCardAccount.card_id)
        card_price = card_price.all()
        total_account = account.filter(
            XcMieba2FavorableCardAccount.created_at.between(self.start_time, self.end_time)).all()
        total_dict = {"{}".format(total.present_price): total.count for total in total_account}
        total_list = [{
            "money": card[0] / 100,
            "times": total_dict.get(str(card[0]), 0)
        } for card in card_price if total_dict.get(str(card[0]), 0)]
        return total_list
