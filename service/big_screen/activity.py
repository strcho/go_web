from model.all_model import *
from mbutils import dao_session
from utils.constant.account import RIDING_CHANNEL_TYPE, SERIAL_TYPE, DEPOSIT_CHANNEL_TYPE, PAY_TYPE
from . import ScreenService


class BigScreenActivityService(ScreenService):

    # 根据不同的支付类型做区分，设计统一通过时间和服务区进行过滤
    def __init__(self, op_area_ids: tuple, start_time: datetime, end_time: datetime):
        """
        @param op_area_ids: 服务区id
        @param start_time: 开始时间（时间类型）
        @param end_time: 结束时间（时间类型）
        """
        super().__init__(op_area_ids)
        self.start_time = start_time
        self.end_time = end_time

    # 骑行卡赠送金额
    def query_riding_card_giving_money(self):
        money = dao_session.session().query(func.ifnull(func.sum(XcEbike2RidingcardAccount.money), 0) / 100).filter(
            XcEbike2RidingcardAccount.channel.in_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
        if self.op_area_ids:
            money = money.filter(XcEbike2RidingcardAccount.serviceId.in_(self.op_area_ids))
        money = money.filter(XcEbike2RidingcardAccount.createdAt.between(self.start_time, self.end_time)).first()
        return {"riding_card": round(float(money[0]), 2)}

    # 新版骑行卡赠送次数
    def query_riding_card_new_times(self):
        """
        骑行卡新版购买（赠送）次数
        新增骑行卡次卡，增加了每种骑行卡的次数
        @param is_giving: 1:赠送，0：购买
        @return:
        """
        money_times = dao_session.session().query(
            func.count(XcEbike2RidingcardAccount.id), XcEbike2RidingcardAccount.money,
            XcEbike2RidingcardAccount.ridingCardName). \
            filter(XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_new_riding_card_buy()),
                   XcEbike2RidingcardAccount.channel.in_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
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

    # 老版骑行卡赠送次数
    def query_riding_card_old_times(self):
        """
        骑行卡老版售卖(赠送)次数
        is_giving: 1:赠送，0：购买
        """
        money_times = dao_session.session().query(
            func.count(XcEbike2RidingcardAccount.id), XcEbike2RidingcardAccount.money, XcEbike2RidingcardAccount.type). \
            filter(XcEbike2RidingcardAccount.type.in_(SERIAL_TYPE.merchants_old_riding_card_buy()),
                   XcEbike2RidingcardAccount.channel.in_(RIDING_CHANNEL_TYPE.riding_card_fake_income_type()))
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

    # 会员卡赠送金额
    def query_deposit_card_giving_money(self):
        money = dao_session.session().query(func.ifnull(func.sum(XcEbike2DepositCard.money), 0) / 100).filter(
            XcEbike2DepositCard.channel.in_(DEPOSIT_CHANNEL_TYPE.deposit_all_fake_income_type()))
        if self.op_area_ids:
            money = money.filter(XcEbike2DepositCard.serviceId.in_(self.op_area_ids))
        money = money.filter(XcEbike2DepositCard.createdAt.between(self.start_time, self.end_time)).first()
        return {"deposit_card": round(float(money[0]), 2)}

    # 会员卡赠送次数
    def query_deposit_card_giving_times(self):
        m_times = dao_session.session().query(
            func.ifnull(func.count(XcEbike2DepositCard.id), 0), XcEbike2DepositCard.money).filter(
            XcEbike2DepositCard.createdAt.between(self.start_time, self.end_time),
            XcEbike2DepositCard.channel.in_(DEPOSIT_CHANNEL_TYPE.deposit_all_fake_income_type())
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

    # 平台充值余额
    def query_platform_add_wallet(self):
        m_times = dao_session.session().query(func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100). \
            join(XcEbikeUsrs2, XcEbikeUsrs2.id == XcEbikeAccount2.objectId). \
            filter(XcEbikeAccount2.createdAt.between(self.start_time, self.end_time),
                   XcEbikeAccount2.type == SERIAL_TYPE.MANUAL.value)
        if self.op_area_ids:
            m_times = m_times.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        m_times = m_times.first()
        return m_times[0]

    # 平台充值余额按天聚合
    def query_platform_add_wallet_day(self):
        m_times = dao_session.session().query(
            func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100,
            func.date_format(XcEbikeAccount2.createdAt, "%Y-%m-%d").label("ctime")). \
            join(XcEbikeUsrs2, XcEbikeUsrs2.id == XcEbikeAccount2.objectId). \
            filter(XcEbikeAccount2.createdAt.between(self.start_time, self.end_time),
                   XcEbikeAccount2.type == SERIAL_TYPE.MANUAL.value)
        if self.op_area_ids:
            m_times = m_times.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by("ctime").all()
        data_dict, cost_sum = {}, 0,
        for c, d in m_times:
            data_dict[d] = {"wallet_recharge": c}
            cost_sum += c
        return data_dict, cost_sum

    # 充值赠送余额
    def query_recharge_giving_wallet(self):
        m_times = dao_session.session().query(func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100). \
            join(XcEbikeUsrs2, XcEbikeUsrs2.id == XcEbikeAccount2.objectId). \
            filter(XcEbikeAccount2.createdAt.between(self.start_time, self.end_time),
                   XcEbikeAccount2.type == SERIAL_TYPE.PLATFORMGIVEWALLET.value)
        if self.op_area_ids:
            m_times = m_times.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        m_times = m_times.first()
        return m_times[0]

    # 充值赠送余额按天聚合
    def query_recharge_giving_wallet_day(self):
        m_times = dao_session.session().query(
            func.ifnull(func.sum(XcEbikeAccount2.amount), 0) / 100,
            func.date_format(XcEbikeAccount2.createdAt, "%Y-%m-%d").label("ctime")). \
            join(XcEbikeUsrs2, XcEbikeUsrs2.id == XcEbikeAccount2.objectId). \
            filter(XcEbikeAccount2.createdAt.between(self.start_time, self.end_time),
                   XcEbikeAccount2.type == SERIAL_TYPE.PLATFORMGIVEWALLET.value)
        if self.op_area_ids:
            m_times = m_times.filter(XcEbikeUsrs2.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by("ctime").all()
        data_dict, cost_sum = {}, 0,
        for c, d in m_times:
            data_dict[d] = {"recharge_giving": c}
            cost_sum += c
        return data_dict, cost_sum

    # 活动赠送余额
    def query_giving_wallet(self):
        m_times = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserWalletRecord.change) / 100, 0)
        ).filter(
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
            XcEbikeUserWalletRecord.type.in_(PAY_TYPE.activity_giving_type()),
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
        m_times = m_times.first()
        return m_times[0]

    # 活动赠送余额按天聚合
    def query_giving_wallet_day(self):
        m_times = dao_session.sub_session().query(
            func.ifnull(func.sum(XcEbikeUserWalletRecord.change) / 100, 0),
            func.date_format(XcEbikeUserWalletRecord.createdAt, "%Y-%m-%d").label("ctime")
        ).filter(
            XcEbikeUserWalletRecord.createdAt.between(self.start_time, self.end_time),
            XcEbikeUserWalletRecord.type.in_(PAY_TYPE.activity_giving_type()),
        )
        if self.op_area_ids:
            m_times = m_times.filter(XcEbikeUserWalletRecord.serviceId.in_(self.op_area_ids))
        m_times = m_times.group_by("ctime").all()
        data_dict, cost_sum = {}, 0,
        for c, d in m_times:
            data_dict[d] = {"giving": c}
            cost_sum += c
        return data_dict, cost_sum
