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

        user_wallet = yield mb_async(WalletService().query_one)(args)
        user_riding_card = yield mb_async(RidingCardService().current_duriong_card)(args)
        user_deposit_card = yield mb_async(DepositCardService().query_one)(args)
        user_favorable_card = yield mb_async(FavorableCardUserService().query_one)(args)
        user_free_order = yield mb_async(UserFreeOrderService().query_one)(args)
        user_discount = yield mb_async(UserDiscountService().query_one)(args)

        data = {
            "user_wallet": user_wallet,
            "user_riding_card": user_riding_card,
            "user_deposit_card": user_deposit_card,
            "user_favorable_card": None if user_favorable_card.end_time <= datetime.now() else user_favorable_card,
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
        
        user_wallet = yield mb_async(WalletService().query_one)(args)
        user_riding_card = yield mb_async(RidingCardService().user_card_info)(args)
        user_deposit_card = yield mb_async(DepositCardService().query_one)(args)
        user_favorable_card = yield mb_async(FavorableCardUserService().query_one)(args)
        user_free_order = yield mb_async(UserFreeOrderService().query_one)(args)
        user_discount = yield mb_async(UserDiscountService().query_one)(args)

        data = {
            "user_wallet": user_wallet,
            "user_riding_card": user_riding_card,
            "user_deposit_card": user_deposit_card,
            "user_favorable_card": None if user_favorable_card.end_time <= datetime.now() else user_favorable_card,
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

        user_wallet = yield mb_async(WalletService().query_one)(args)
        user_riding_card = yield mb_async(RidingCardService().user_card_info)(args)
        user_deposit_card = yield mb_async(DepositCardService().query_one)(args)
        user_favorable_card = yield mb_async(FavorableCardUserService().query_one)(args)
        user_free_order = yield mb_async(UserFreeOrderService().query_one)(args)
        user_discount = yield mb_async(UserDiscountService().query_one)(args)

        data = {
            "user_wallet": user_wallet,
            "user_riding_card": user_riding_card,
            "user_deposit_card": user_deposit_card,
            "user_favorable_card": None if user_favorable_card.end_time <= datetime.now() else user_favorable_card,
            "user_free_order": user_free_order,
            "user_discount": user_discount,
        }

        response = UserAccountSerializer().dump(data)

        self.success(response)
