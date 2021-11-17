from tornado.gen import coroutine

from mbutils import mb_async
from mbutils.autodoc import use_args_query
from mbutils.mb_handler import MBHandler
from routes.riding_card.serializers import (
    GetRidingCardDeserializer,
    RidingCardSerializer,
)
from service.riding_card_service import RidingCardService


class RidingCardHandle(MBHandler):
    """
    骑行卡
    """

    @coroutine
    @use_args_query(GetRidingCardDeserializer)
    def get(self, args: dict):
        """
        获取用户骑行卡
        """
        pin_id = args.get('pin_id')
        valid_data = (pin_id, args)
        data = yield mb_async(RidingCardService().user_card_info)(valid_data)
        data = RidingCardSerializer().dump(data)

        self.success(data)

