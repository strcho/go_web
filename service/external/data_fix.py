import subprocess
import datetime
import time

from service import MBService
from service.payment.riding_card import RidingCardService
from mbutils import dao_session
from mbutils import logger
from utils.constant.user import UserState


class GetLogService(MBService):
    def get_log(self, valid_data):
        _type, is_node, row_num = valid_data
        #  RidingCardService.handle_riding_card({"riding_card_type": 10, "object_id": "60fa5522f69486000112ef90", "trade_no": "4200001144202107316034318217", "amount": "6000", "channel": 3, "gmt_create": "20210731201055", "config_id": 1})
        if is_node:
            if _type == "info":
                res = subprocess.getoutput("tail -n {} /admin/logs/eBikeServer/eBikeServer-*.log".format(row_num))
            elif _type == "error":
                res = subprocess.getoutput("tail -n {} /admin/logs/eBikeServer/error.log.*.log".format(row_num))
            else:
                res = subprocess.getoutput("tail -n {} /admin/logs/eBikeServer/biz-*.log".format(row_num))
        else:
            if _type == "info":
                res = subprocess.getoutput("tail -n {} /admin/logs/mbServer/mbServer_*.log".format(row_num))
            elif _type == "error":
                res = subprocess.getoutput("tail -n {} /admin/logs/mbServer/error_*.log".format(row_num))
            else:
                res = subprocess.getoutput("tail -n {} /admin/logs/mbServer/biz_*.log|grep -v get_log".format(row_num))
        return res.split("\n")[::-1]


def batch_auto_pay_func(valid_data):
    """
    将某个时间范围的未支付订单小于钱包余额的订单自动扣除, 并将未支付状态改成已支付状态
    :param valid_data:
    :return:
    """
    # 钱包表减少钱,userId, type, change, orderId, agentId, serviceId
    # xc_ebike_usrs_2 的balance
    # 更新充送分离字段
    # 修改钱包未已经支付
    # 如果用户有未支付的订单，将用户置为待支付状态
    # 确认要修改的订单
    start_time, end_time = valid_data
    begin = time.strftime('%Y-%m-%d', time.localtime(start_time / 1000))
    end = time.strftime('%Y-%m-%d', time.localtime(end_time / 1000))

    rows = dao_session.session().execute("""
    SELECT o.`orderId`, o.`userId` , o.`cost`, u.`balance`   
    from xc_ebike_user_orders o JOIN `xc_ebike_usrs_2` u on u.id= o.`userId` 
    WHERE o.`isPaid`= 0
    and o.`createdAt` between :begin and :end
    and o.`cost` <=u.balance""", {"begin": begin, "end": end})
    if not rows:
        return 0
    all_user = set()
    # 修改订单
    # count = 0
    # for order_id, u_id, cost, balance in rows:
    #     try:
    #         order = dao_session.session().query(XcEbikeUserOrder).filter(XcEbikeUserOrder.orderId == order_id).one()
    #         user = dao_session.session().query(XcEbikeUsrs2).filter(XcEbikeUsrs2.id == u_id).one()
    #
    #         amount = order.cost
    #         recharge = user.recharge
    #         present = user.present
    #         recharge_change = 0  # 此次充值的钱
    #         present_change = 0  # 此次赠送的钱
    #         if recharge >= amount:
    #             recharge_change = amount
    #         elif recharge + present >= amount:
    #             recharge_change = recharge
    #             present_change = amount - recharge
    #         else:
    #             continue
    #
    #         user.balance = user.balance - amount
    #         user.present = present - present_change
    #         user.recharge = recharge - recharge_change
    #
    #         order.isPaid = 1
    #         order.rechargeCost = recharge_change
    #         order.presentCost = present_change
    #
    #         params = {
    #             "userId": order.userId,
    #             "type": 1,
    #             "change": -order.cost,
    #             "orderId": order.orderId,
    #             "agentId": order.agentId,
    #             "serviceId": order.serviceId,
    #             "createdAt": datetime.datetime.now(),
    #             "updatedAt": datetime.datetime.now(),
    #             "rechargeChange": - order.rechargeCost,
    #             "presentChange": - order.presentCost
    #         }
    #         one_wallet = XcEbikeUserWalletRecord(**params)
    #         dao_session.session().add(one_wallet)
    #         dao_session.session().commit()
    #         count += 1
    #         logger.info(
    #             "batch_auto_pay_func:order_id-{},u_id-{},cost-{},balance-{}".format(order_id, u_id, cost, balance))
    #     except Exception:
    #         dao_session.session().rollback()
    #     all_user.add(u_id)
    #     # 打印日志,打印操作
    #
    # # 如果用户没有未支付订单, 将用户变成待支付状态;
    # for u_id in all_user:
    #
    #     res = dao_session.session().query(XcEbikeUserOrder).filter(XcEbikeUserOrder.userId == u_id,
    #                                                                XcEbikeUserOrder.isPaid == 0).first()
    #     # 如果不存在未支付订单,则把它变成未支付状态.
    #     if not res:
    #         UserState.set_state(dao_session.redis_session.r, u_id, UserState.READY.value, UserState.TO_PAY.value)
    # return count
