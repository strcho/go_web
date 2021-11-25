# coding: utf-8
from sqlalchemy import (
    Column,
    DateTime,
    Index,
    String,
    text,
)
from sqlalchemy.dialects.mysql import (
    INTEGER,
)

from mbshort.orm import (
    CommonField,
)


class TUserWallet(CommonField):

    __tablename__ = 't_ebike_account_user_wallet' + '_suffix'
    __table_args__ = (
        {'mysql_charset': 'utf8mb4'}
    )

    pin = Column(String(64), nullable=False, comment="用户PIN")
    balance = Column(INTEGER(11), server_default=text("'0'"), comment="总余额")
    recharge = Column(INTEGER(11), server_default=text("'0'"), comment='充值余额')
    present = Column(INTEGER(11), server_default=text("'0'"), comment='赠送余额')

    deposited_mount = Column(INTEGER(11), server_default=text("'0'"), comment='押金金额')
    deposited_stats = Column(INTEGER(11), nullable=False, server_default=text("'0'"), comment='押金状态')


class TRidingCard(CommonField):

    __tablename__ = 't_ebike_account_riding_card' + '_suffix'
    __table_args__ = (
        Index('idx_pin_state', 'pin', 'state'),
    )

    pin = Column(String(64), nullable=False, comment="用户PIN")
    deduction_type = Column(INTEGER(11), server_default=text("'1'"), comment="抵扣类型, 1时长卡, 2里程卡, 3减免卡, 4次卡")
    config_id = Column(INTEGER(32), nullable=False, comment="骑行卡配置ID")
    free_time = Column(INTEGER(11), default=0, server_default=text("'0'"), comment="免时长, 单位秒")
    free_distance = Column(INTEGER(11), default=0, server_default=text("'0'"), comment="免里程, 单位米")
    free_money = Column(INTEGER(11), default=0, server_default=text("'0'"), comment="免金额, 单位分")
    iz_total_times = Column(INTEGER(11), default=0, server_default=text("'0'"), comment="是否次卡类")
    rece_times = Column(INTEGER(11), default=9999, server_default=text("'9999'"), comment="次卡类, 表示总累计次数; 非次卡类, 表示每日最大次数")
    effective_service_ids = Column(String(1024), nullable=False, server_default=text("''"), comment="服务区id列表, ;分割, "
                                                                                                    "''表示全部服务区")
    remain_times = Column(INTEGER(11), default=0, comment="骑行卡剩余使用次数")  # 剩余次数
    last_use_time = Column(DateTime, comment="骑行卡最后一次使用时间")
    start_time = Column(DateTime, nullable=False, comment="购卡时间")
    card_expired_date = Column(DateTime, nullable=False, comment="截止时间")
    content = Column(String(4096), nullable=False, comment="购卡时候配置信息")

    state = Column(INTEGER(11), nullable=False, default=1, server_default=text("'1'"), comment="1使用中, 2过期, 3删除")


class TDepositCard(CommonField):
    __tablename__ = 't_ebike_account_deposit_card' + '_suffix'

    pin = Column(String(64), nullable=False, index=True, comment='用户标识')
    # config_id = Column(INTEGER(32), nullable=False, comment='押金卡配置ID')
    # money = Column(INTEGER(10), comment='押金卡金额')
    # channel = Column(INTEGER(10), comment='获取方式')
    # days = Column(INTEGER(10), comment='有效天数')
    # trade_no = Column(String(64), nullable=False, index=True)
    # service_id = Column(INTEGER(11), comment='服务区ID')
    # content = Column(String(1024), nullable=False, comment='押金卡详情')
    expired_date = Column(DateTime, comment='到期时间')


# 用户的优惠卡
class TFavorableCard(CommonField):
    __tablename__ = 't_ebike_account_favorable_card' + '_suffix'

    pin = Column(String(64), nullable=False, index=True, comment='用户标识')
    begin_time = Column(DateTime, nullable=False, comment='开始时间')  # 使用优惠卡的开始时间
    end_time = Column(DateTime, nullable=False, comment='结束时间')  # 使用优惠卡的结束时间
    # config_id = Column(INTEGER(11), nullable=False, comment='计费配置的id')  # 计费配置的id
    # service_id = Column(INTEGER(11), nullable=False, index=True, comment='服务区的id')  # 服务区的id


# 用户折扣
class TDiscountsUser(CommonField):
    __tablename__ = 't_ebike_account_discounts_user' + '_suffix'

    pin = Column(String(64), nullable=False, index=True, comment='用户标识')
    discount_rate = Column(INTEGER(10), nullable=False, server_default=text("'0'"), comment='折扣信息 10 表示 1折')


# 用户免单
class TFreeOrderUser(CommonField):
    __tablename__ = 't_ebike_account_free_order_user' + '_suffix'

    pin = Column(String(64), nullable=False, index=True, comment='用户标识')
    free_second = Column(INTEGER(5), nullable=False, server_default=text("'0'"), comment='每单的免费时长')
    free_num = Column(INTEGER(5), nullable=False, server_default=text("'0'"), comment='免单次数')
