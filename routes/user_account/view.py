import base64
import json
from datetime import datetime

from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.user_account.serializers import (
    UserAccountDeserializer,
    UserAccountSerializer,
    BusUserAccountDeserializer,
    CliUserAccountDeserializer,
)
from service.deposit_card_service import DepositCardService
from service.favorable_card_service import FavorableCardUserService
from service.free_order_service import UserFreeOrderService
from service.riding_card_service import RidingCardService
from service.user_discount_service import UserDiscountService
from service.wallet_service import WalletService


class UserAccount(MBHandler):
    """
    用户资产
    """

    @coroutine
    @use_args_query(UserAccountDeserializer)
    def post(self, args):
        """
          获取用户资产
          ---
          tags: [资产]
          summary: 获取用户资产
          description: 获取用户资产

          parameters:
            - in: body
              schema:
                  UserAccountDeserializer
          responses:
              200:
                  schema:
                      type: object
                      required:
                        - success
                        - code
                        - msg
                        - data
                      properties:
                          success:
                              type: boolean
                          code:
                              type: str
                          msg:
                              type: str
                          data:
                              UserAccountSerializer
          """

        user_wallet, user_riding_card, user_deposit_card, user_favorable_card, user_free_order, user_discount = yield [
            mb_async(WalletService().query_one)(args),
            mb_async(RidingCardService().current_duriong_card)(args),
            mb_async(DepositCardService().query_one)(args),
            mb_async(FavorableCardUserService().query_one)(args),
            mb_async(UserFreeOrderService().query_one)(args),
            mb_async(UserDiscountService().query_one)(args)
        ]

        one = user_riding_card
        car_info = {"id": one.id}
        car_info["card_id"] = one.config_id
        content = json.loads(one.content)
        car_info["name"] = content["name"]
        car_info["image_url"] = content["image_url"]
        car_info["description_tag"] = content.get("description_tag", "限全国")
        car_info["detail_info"] = content.get("detail_info", "") or str(
            base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
                content["valid_day"],
                "累计" if one.iz_total_times else "每日",
                content["available_times"], int(float(content["free_time_second"]))).encode("utf-8")),
            "utf-8")
        car_info["card_expired_date"] = one.card_expired_date
        car_info["remain_times"] = one.remain_times
        car_info["iz_total_times"] = one.iz_total_times
        car_info["rece_times"] = one.rece_times
        car_info["free_time_second"] = one.free_time
        car_info["free_distance_meter"] = one.free_distance
        car_info["free_money_cent"] = one.free_money
        car_info["promotion_tag"] = content.get("promotion_tag", "人气优选")
        car_info["deductionType"] = one.deduction_type
        car_info["effective_service_ids"] = content.get("effective_service_ids")
        car_info["effective_service_names"] = content.get("effective_service_names")

        data = {
            "user_wallet": user_wallet,
            "user_riding_card": car_info,
            "user_deposit_card": user_deposit_card,
            "user_favorable_card": None if user_favorable_card and user_favorable_card.end_time <= datetime.now() else user_favorable_card,
            "user_free_order": user_free_order,
            "user_discount": user_discount,
        }
        response = UserAccountSerializer().dump(data)

        self.success(response)


class BusUserAccount(MBHandler):
    """
    B端 用户资产
    """

    @coroutine
    @use_args_query(BusUserAccountDeserializer)
    def post(self, args):
        """
        获取用户资产
        ---
        tags: [B端-资产]
        summary: 获取用户资产
        description: 获取用户资产

        parameters:
        - in: body
          schema:
              BusUserAccountDeserializer
        responses:
          200:
              schema:
                  type: object
                  required:
                    - success
                    - code
                    - msg
                    - data
                  properties:
                      success:
                          type: boolean
                      code:
                          type: str
                      msg:
                          type: str
                      data:
                          UserAccountSerializer
        """
        args['commandContext'] = self.get_context()

        user_wallet = yield mb_async(WalletService().query_one)(args)
        user_riding_card = yield mb_async(RidingCardService().current_duriong_card)(args)
        user_deposit_card = yield mb_async(DepositCardService().query_one)(args)
        user_favorable_card = yield mb_async(FavorableCardUserService().query_one)(args)
        user_free_order = yield mb_async(UserFreeOrderService().query_one)(args)
        user_discount = yield mb_async(UserDiscountService().query_one)(args)

        one = user_riding_card
        car_info = {"id": one.id}
        car_info["card_id"] = one.config_id
        content = json.loads(one.content)
        car_info["name"] = content["name"]
        car_info["image_url"] = content["image_url"]
        car_info["description_tag"] = content.get("description_tag", "限全国")
        car_info["detail_info"] = content.get("detail_info", "") or str(
            base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
                content["valid_day"],
                "累计" if one.iz_total_times else "每日",
                content["available_times"], int(float(content["free_time_second"]))).encode("utf-8")),
            "utf-8")
        car_info["card_expired_date"] = one.card_expired_date
        car_info["remain_times"] = one.remain_times
        car_info["iz_total_times"] = one.iz_total_times
        car_info["rece_times"] = one.rece_times
        car_info["free_time_second"] = one.free_time
        car_info["free_distance_meter"] = one.free_distance
        car_info["free_money_cent"] = one.free_money
        car_info["promotion_tag"] = content.get("promotion_tag", "人气优选")
        car_info["deductionType"] = one.deduction_type
        car_info["effective_service_ids"] = content.get("effective_service_ids")
        car_info["effective_service_names"] = content.get("effective_service_names")

        data = {
            "user_wallet": user_wallet,
            "user_riding_card": car_info,
            "user_deposit_card": user_deposit_card,
            "user_favorable_card": None if user_favorable_card and user_favorable_card.end_time <= datetime.now() else user_favorable_card,
            "user_free_order": user_free_order,
            "user_discount": user_discount,
        }
        response = UserAccountSerializer().dump(data)

        self.success(response)


class ClientUserAccount(MBHandler):
    """
    C端 用户资产
    """

    @coroutine
    @use_args_query(CliUserAccountDeserializer)
    def post(self, args):
        """
        获取用户资产
        ---
        tags: [C端-资产]
        summary: 获取用户资产
        description: 获取用户资产

        parameters:
        - in: body
          schema:
              CliUserAccountDeserializer
        responses:
          200:
              schema:
                  type: object
                  required:
                    - success
                    - code
                    - msg
                    - data
                  properties:
                      success:
                          type: boolean
                      code:
                          type: str
                      msg:
                          type: str
                      data:
                          UserAccountSerializer
        """

        args['commandContext'] = self.get_context()
        user_wallet = yield mb_async(WalletService().query_one)(args)
        user_riding_card = yield mb_async(RidingCardService().current_duriong_card)(args)
        user_deposit_card = yield mb_async(DepositCardService().query_one)(args)
        user_favorable_card = yield mb_async(FavorableCardUserService().query_one)(args)
        user_free_order = yield mb_async(UserFreeOrderService().query_one)(args)
        user_discount = yield mb_async(UserDiscountService().query_one)(args)

        one = user_riding_card
        car_info = {"id": one.id}
        car_info["card_id"] = one.config_id
        content = json.loads(one.content)
        car_info["name"] = content["name"]
        car_info["image_url"] = content["image_url"]
        car_info["description_tag"] = content.get("description_tag", "限全国")
        car_info["detail_info"] = content.get("detail_info", "") or str(
            base64.b64encode("限制使用区域:全国\n限制使用天数:{}\n{}使用次数:{}次\n每次抵扣时长:{}分钟".format(
                content["valid_day"],
                "累计" if one.iz_total_times else "每日",
                content["available_times"], int(float(content["free_time_second"]))).encode("utf-8")),
            "utf-8")
        car_info["card_expired_date"] = one.card_expired_date
        car_info["remain_times"] = one.remain_times
        car_info["iz_total_times"] = one.iz_total_times
        car_info["rece_times"] = one.rece_times
        car_info["free_time_second"] = one.free_time
        car_info["free_distance_meter"] = one.free_distance
        car_info["free_money_cent"] = one.free_money
        car_info["promotion_tag"] = content.get("promotion_tag", "人气优选")
        car_info["deductionType"] = one.deduction_type
        car_info["effective_service_ids"] = content.get("effective_service_ids")
        car_info["effective_service_names"] = content.get("effective_service_names")

        data = {
            "user_wallet": user_wallet,
            "user_riding_card": car_info,
            "user_deposit_card": user_deposit_card,
            "user_favorable_card": None if user_favorable_card and user_favorable_card.end_time <= datetime.now() else user_favorable_card,
            "user_free_order": user_free_order,
            "user_discount": user_discount,
        }

        response = UserAccountSerializer().dump(data)

        self.success(response)
