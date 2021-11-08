import json
import math
from datetime import timedelta

from model.all_model import *
from service.config import ConfigService
from mbutils import MbException
from mbutils import dao_session
from mbutils import logger
from utils.constant.account import SERIAL_TYPE, RIDING_CHANNEL_TYPE, PAY_TYPE, DEPOSIT_CONFIG_TYPE, DEPOSIT_CHANNEL_TYPE
from utils.constant.config import ConfigName
from utils.constant.redis_key import *
from utils.constant.user import UserState
from . import MBService
from .super_riding_card.internal import InternalService


class UserService(MBService):
    def query_one(self, query: dict):
        one = dao_session.session().query(XcEbike2Blacklist).filter_by(**query).first()
        return one

    @staticmethod
    def can_get_deposited_card(user_id):
        """
        在需要给用户发放押金卡的所有地方需要使用，骑车判断条件也类似多维判断
        押金卡过期key，订阅后如果是骑行中，支付中，则押金卡过期会产生一定问题
        :param user_id:
        :return:
        """
        # 如果用户有押金，则暂时不能领取奖励，需要用户发起退押金操作
        user = dao_session.session().query(XcEbikeUsrs2).filter_by(id=user_id).first()
        if not user:
            return False
        user_state = dao_session.redis_session.r.get(USER_STATE_KEY.format(user_id))
        if isinstance(user_state, str):
            user_state = int(user_state)
        if user_state and user_state == UserState.SIGN_UP.value:
            logger.debug("用户{}登录出去了，不能领取押金卡".format(user_id))
            return False
        if user.student == 2 and user_state != UserState.AUTHED.value:
            logger.debug("用户{}有学生认证，不能领取押金卡".format(user_id))
            return False
        if user.deposited == 1 and user.depositedInfo and user.depositedInfo != '{}':
            logger.debug("用户{}有已经押金，不能领取押金卡".format(user_id))
            return False  # 有押金卡的用户可以参加这个活动
        try:
            res = ConfigService().get_router_content(ConfigName.ZERODEPOSIT.value, user.serviceId)
            if res and res["content"] and res["content"]["zeroDeposit"]:
                logger.debug("服务区有一键免压功能，用户{}不能领取押金卡".format(user_id))
                return False
        except Exception:
            return True
        return True


class UserReward(MBService):

    def add_reward_2_user(self, user_id, reward_type, reward_remark, tp):
        """
        给用户发放奖励, node中 addReward2User
        :param user_id: 用户id
        :param reward_type: 用户奖励类型
        :param reward_remark: 用户奖励明细
        :param tp: 类型后期统计数据使用 1为固定活动 2为指定用户奖励 3为自定义活动
        :return:
        """
        if reward_type == RewardType.FREE:
            num, hour = reward_remark.split(',')
            res = self.add_free_order_2_user(user_id, num, hour, tp)
        elif reward_type == RewardType.RIDING_CARD:
            card_id = reward_remark.split(',')[0]
            res = self.add_riding_card_2_user(user_id, card_id, tp)
        elif reward_type == RewardType.BALANCE:
            money = int(reward_remark)
            res = self.add_balance_2_user(user_id, money, tp)
        elif reward_type == RewardType.DISCOUNT:
            discount = float(reward_remark)  # 小数，零点几
            res = self.add_discount_2_user(user_id, discount, tp)
        elif reward_type == RewardType.DEPOSIT_CARD:
            card_id = reward_remark.split(',')[0]
            res = self.add_deposit_2_user(user_id, card_id, tp)
        else:
            return False
        return res

    def add_free_order_2_user(self, user_id, num, hour, tp):
        """
        记录或更新用户领取奖励为免单的信息， # to0do 原来node里面搬过来的有bug，次数增加，时长覆盖，如果多个时长就有问题
        :param user_id: 用户id
        :param num: 用户次数
        :param hour: 用户时长
        :param tp:  type 1加免单次数 2消耗免单次数
        :return:
        """
        free_order_info = dao_session.redis_session.r.get(FREE_USER_KEY.format(user_id))
        temp_option = {"num": 0, "hour": 0}
        if free_order_info:
            option = json.loads(free_order_info)
            if tp == 1:
                temp_option["num"] = int(option["num"]) + int(num)
                temp_option["hour"] = ["hour"]
            elif tp == 2:
                if int(option.num) > 1:
                    temp_option["num"] = int(option["num"]) - 1
                    temp_option["hour"] = ["hour"]
                else:
                    temp_option["num"] = 0
                    temp_option["hour"] = option["hour"]
            else:
                temp_option["num"] = int(option["num"]) + int(num)
                temp_option["hour"] = ["hour"]
        else:
            temp_option["num"] = int(num)
            temp_option["hour"] = hour
        return dao_session.redis_session.r.set(FREE_USER_KEY.format(user_id), json.dumps(temp_option))

    def add_riding_card_2_user(self, user_id, card_id, tp):
        """
         骑行卡配置：{
             "ridingCardName": "日卡",
             "serialType": 10,
             "expiryDate": 1,
             "originCost": 100,
             "curCost": 1,
             "available_times": "1",
             "freeTime": "1",
             "pictureState": 0,
             "backOfCardUrl": "http://img.cdn.xiaoantech.com/ebikeplatform/bike-card-defalut.jpg",
             "state": 1,
             "sortType": 1,
             "slogan": "1天畅骑",
             "createdAt": "2019-08-30T03:01:11.577Z"
         }
        :param user_id:
        :param card_id:
        :param tp:
        :return:
        """
        # trade_no = self.create_trade_number()
        try:
            logger.info(f"user_id: {user_id}, card_id:{card_id}, tp:{tp}")
            if tp == 1:  # 完成营销活动固定活动送余额
                account_type = SERIAL_TYPE.REGULAR_ACTIVITY_ADD_RINGDINGCARD.value
                account_channel = RIDING_CHANNEL_TYPE.REGULAR_ACTIVITY.value
                riding_channel = RIDING_CHANNEL_TYPE.RIDING_CARD_REGULAR_ACTIVITY.value
            elif tp == 2:  # 指定用户奖励活动送余额
                account_type = SERIAL_TYPE.TARGET_USER_ADD_RINGDINGCARD.value
                account_channel = RIDING_CHANNEL_TYPE.TARGET_USER.value
                riding_channel = RIDING_CHANNEL_TYPE.RIDING_CARD_TARGET_USER.value
            elif tp == 3:  # 充值赠送活动
                account_type = SERIAL_TYPE.PLARFORMGIVECARD.value
                account_channel = RIDING_CHANNEL_TYPE.PLATFORMGIVE.value
                riding_channel = RIDING_CHANNEL_TYPE.PLATFORMGIVE.value
            else:  # 完成营销活动自定义活动送余额
                account_type = SERIAL_TYPE.CUSTOM_ACTIVITY_ADD_RINGDINGCARD.value
                account_channel = RIDING_CHANNEL_TYPE.CUSTOM_ACTIVITY.value
                riding_channel = RIDING_CHANNEL_TYPE.RIDING_CARD_CUSTOM_ACTIVITY.value
            # 记录用户骑行卡流水 20是固定活动channel 21是指定用户奖励活动 22自定义活动
            user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == user_id).first()
            if not user_info:
                logger.info("赠送钱包金额时，用户信息获取失败，{}".format(user_id))
            riding_card_config = dao_session.session().query(XcEbike2RidingConfig). \
                filter(XcEbike2RidingConfig.id == card_id, XcEbike2RidingConfig.state == 1,
                       XcEbike2RidingConfig.serviceId == user_info.serviceId).first()
            if not riding_card_config and riding_card_config.content:
                logger.info(f"riding_card config is failed，object_id: {user_id}, config_id: {card_id}")
                return
            riding_card_name, riding_content = "", json.loads(riding_card_config.content)
            days = riding_content.get("expiryDate", 0)
            if riding_card_config.type >= 310:
                riding_card_name = riding_content.get("ridingCardName", "")
            xc_a = XcEbikeAccount2()
            xc_a.objectId = user_id
            xc_a.amount = riding_content.get("curCost", 0)
            xc_a.type = account_type
            xc_a.channel = account_channel
            xc_a.trade_no = ''
            xc_a.paid_at = datetime.now()
            xc_a.createdAt = datetime.now()
            xc_a.updatedAt = datetime.now()
            # 插入流水表
            dao_session.session().add(xc_a)
            dao_session.session().flush()

            xc_rca = XcEbike2RidingcardAccount()
            xc_rca.objectId = user_id
            xc_rca.configId = card_id
            xc_rca.type = riding_card_config.type
            xc_rca.money = riding_content.get("curCost", 0)
            xc_rca.channel = riding_channel
            xc_rca.trade_no = ""
            xc_rca.content = riding_card_config.content
            xc_rca.agentId = user_info.agentId or 2
            xc_rca.serviceId = user_info.serviceId or 1
            xc_rca.ridingCardName = riding_card_name
            xc_rca.createdAt = datetime.now()
            xc_rca.updatedAt = datetime.now()
            dao_session.session().add(xc_rca)
            dao_session.session().flush()
            # 一起提交
            dao_session.session().commit()
            InternalService().add_card((card_id, user_id))
            logger.info(f"赠送骑行卡成功，tp: {tp}, object_id: {user_id}, config_id:{card_id}")
            return True
        except Exception as ex:
            logger.info(f"赠送骑行卡失败，tp: {tp}, object_id: {user_id}, config_id:{card_id}, e:{ex}")
            dao_session.session().rollback()
            dao_session.session().close()
            return False

    def add_balance_2_user(self, user_id, money, tp):
        trade_no = self.create_trade_number()
        if tp == 1:  # 完成营销活动固定活动送余额
            account_type = SERIAL_TYPE.REGULAR_ACTIVITY_ADD_MONEY
            account_channel = RIDING_CHANNEL_TYPE.REGULAR_ACTIVITY
            wallet_type = PAY_TYPE.REGULAR_ACTIVITY
        elif tp == 2:  # 指定用户奖励活动送余额
            account_type = SERIAL_TYPE.TARGET_USER_ADD_MONEY
            account_channel = RIDING_CHANNEL_TYPE.TARGET_USER
            wallet_type = PAY_TYPE.TARGET_USER
        elif tp == 3:  # 充值赠送活动
            account_type = SERIAL_TYPE.PLATFORMGIVEWALLET
            account_channel = RIDING_CHANNEL_TYPE.PLATFORMGIVE
            wallet_type = PAY_TYPE.GIVEWALLET
        else:  # 完成营销活动自定义活动送余额
            account_type = SERIAL_TYPE.CUSTOM_ACTIVITY_ADD_MONEY
            account_channel = RIDING_CHANNEL_TYPE.CUSTOM_ACTIVITY
            wallet_type = PAY_TYPE.CUSTOM_ACTIVITY
        account_type = account_type.value
        account_channel = account_channel.value
        wallet_type = wallet_type.value
        user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == user_id).first()
        if not user_info:
            logger.info("回调时用户信息获取失败，{}".format(user_id))
        account = XcEbikeAccount2()
        account.objectId = user_id
        account.amount = money
        account.type = account_type
        account.channel = account_channel
        account.trade_no = ""
        account.paid_at = datetime.now()
        account.createdAt = datetime.now()
        account.updatedAt = datetime.now()
        dao_session.session().add(account)
        dao_session.session().flush()

        wallet = XcEbikeUserWalletRecord()
        wallet.userId = user_id
        wallet.type = wallet_type
        wallet.change = money
        wallet.rechargeChange = 0
        wallet.presentChange = money
        wallet.orderId = account.serialNo
        wallet.agentId = user_info.agentId  # 充值购买时，service_id,agent_id跟随用户id
        wallet.serviceId = user_info.serviceId
        wallet.createdAt = datetime.now()
        wallet.updatedAt = datetime.now()
        dao_session.session().add(wallet)

        user_info.balance += money
        user_info.present += money
        try:
            dao_session.session().commit()
            logger.info(f"钱包赠送成功, tp: {tp}, object_id:{user_id}, amount:{money}")
            return True
        except Exception as ex:
            logger.error(f"钱包赠送失败，tp: {tp}, object_id:{user_id}, amount:{money}, e:{ex}")
            return False

    def add_discount_2_user(self, user_id, discount, tp):
        deposit_info = dao_session.redis_session.r.get(DEPOSIT_USER_KEY.format(user_id))
        temp_list = []
        if deposit_info:
            temp_list.extend(json.loads(deposit_info))
            temp_list.append(discount)
        else:
            temp_list.append(discount)
        return dao_session.redis_session.r.set(DEPOSIT_USER_KEY.format(user_id), json.dumps(temp_list))

    def add_deposit_2_user(self, user_id, card_id, tp):
        trade_no = self.create_trade_number()
        if tp == 1:  # 完成营销活动固定活动送押金卡
            account_type = SERIAL_TYPE.REGULAR_ACTIVITY_ADD_DEPOSIT
            account_channel = RIDING_CHANNEL_TYPE.REGULAR_ACTIVITY
            deposit_card_channel = DEPOSIT_CHANNEL_TYPE.REGULAR_ACTIVITY
        elif tp == 2:  # 指定用户奖励活动送押金卡
            account_type = SERIAL_TYPE.TARGET_USER_ADD_DEPOSIT
            account_channel = RIDING_CHANNEL_TYPE.TARGET_USER
            deposit_card_channel = DEPOSIT_CHANNEL_TYPE.TARGET_USER
        else:  # 完成营销活动自定义活动送押金卡
            account_type = SERIAL_TYPE.CUSTOM_ACTIVITY_ADD_DEPOSIT
            account_channel = RIDING_CHANNEL_TYPE.CUSTOM_ACTIVITY
            deposit_card_channel = DEPOSIT_CHANNEL_TYPE.CUSTOM_ACTIVITY
        account_type = account_type.value
        account_channel = account_channel.value
        deposit_card_channel = deposit_card_channel.value
        # 记录免押金流水
        try:
            user_info = dao_session.session().query(XcEbikeUsrs2).filter_by(id=user_id).first()
            if not user_info:
                return False
            deposit_config = dao_session.session().query(XcEbike2DepositConfig).filter_by(id=card_id).first()
            content = deposit_config.content
            content_js = json.loads(content)
            duration_days = content_js["cardDurationDays"]
            money = content_js["curMoney"]
            params = {
                "objectId": user_id,
                "configId": card_id,
                "type": DEPOSIT_CONFIG_TYPE.DEPOSIT_CARD.value,
                "money": money,
                "channel": deposit_card_channel,
                "days": duration_days,
                "trade_no": trade_no,
                "expiredDate": datetime.now() + timedelta(days=duration_days),
                "content": content,
                "agentId": user_info.agentId,
                "serviceId": user_info.serviceId,
                "createdAt": datetime.now(),
                "updatedAt": datetime.now()
            }
            deposit_card = XcEbike2DepositCard(**params)
            dao_session.session().add(deposit_card)
            dao_session.session().commit()
        except Exception as ex:
            logger.error("add deposit 2 user:", params)
            logger.exception(ex)
            dao_session.session().rollback()
            return False

        # 记录总流水
        try:
            params = {
                "objectId": user_id,
                "amount": money,
                "type": account_type,
                "channel": account_channel,
                "trade_no": trade_no,
                "paid_at": datetime.now(),
                "orderId": deposit_card.id,
                "createdAt": datetime.now(),
                "updatedAt": datetime.now()
            }
            # 记录流水信息
            account_2 = XcEbikeAccount2(**params)
            dao_session.session().add(account_2)
            dao_session.session().commit()
        except Exception as ex:
            logger.error("add account 2 user:", params)
            logger.exception(ex)
            dao_session.session().rollback()

        # 更新用户免押金卡信息
        try:
            if user_info.haveDepositCard and user_info.depositCardExpiredDate and user_info.depositCardExpiredDate > datetime.now():
                user_info.deposited = 1
                user_info.depositCardExpiredDate = user_info.depositCardExpiredDate + timedelta(days=duration_days)
                expired_time = math.ceil((user_info.depositCardExpiredDate - datetime.now()).seconds)
            else:
                user_info.deposited = 1
                user_info.haveDepositCard = 1
                user_info.depositCardExpiredDate = datetime.now() + timedelta(days=duration_days)
                expired_time = duration_days * 24 * 3600
            dao_session.session().commit()

            # 修改押金卡过期时间，从集合押金卡过期中删除
            dao_session.redis_session.r.set(DEPOSITCARD_USER_KEY.format(user_id), user_id, ex=expired_time)
            dao_session.redis_session.r.srem(DEPOSITED_GROUP_KEY, user_id)
            user_state = dao_session.redis_session.r.get(USER_STATE_KEY.format(user_id))
            if user_state and user_state == UserState.AUTHED.value:
                dao_session.redis_session.r.set(USER_STATE_KEY.format(user_id), UserState.READY.value)
                logger.info(f"user_id: {user_id}, user_state change: {user_state} --> {UserState.READY.value}")
        except Exception as ex:
            logger.error("add deposit 2 user:", user_id, expired_time)
            logger.exception(ex)
            dao_session.session().rollback()
            return False
        return True


class UserInfo:

    def __init__(self, object_id):
        self.object_id = object_id

    def _user_info(self):
        """目前只返回这些参数，后续根据需要进行添加"""
        user_info = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == self.object_id).first()
        user_dict = {}
        if user_info:
            user_dict["object_id"] = user_info.id
            user_dict["phone"] = user_info.phone
            user_dict["deposited"] = user_info.deposited
            user_dict["balance"] = user_info.balance
            user_dict["haveDepositCard"] = user_info.haveDepositCard
            user_dict["depositCardExpiredDate"] = user_info.depositCardExpiredDate
            user_dict["agentId"] = user_info.agentId
            user_dict["serviceId"] = user_info.serviceId
        return user_dict

    def _user_state(self):
        user_state = dao_session.redis_session.r.get(USER_STATE.format(**{"user_id": self.object_id}))
        return {"state": user_state} if user_state else {"state": 0}

    def all_user_info(self):
        user_info = self._user_info()
        user_state = self._user_state()
        user_state.update(user_info)
        return user_state
