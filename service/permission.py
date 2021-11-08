from model.all_model import XcOpman, XcRoleProperty
from service import MBService
from mbutils import dao_session
from utils.constant.redis_key import *
from utils.constant.device import PropertyType, DeviceState
from mbutils import logger


class PermissionService(MBService):
    def has_service_property_by_phone(self, phone, service_id):
        first = dao_session.sub_session().query(XcRoleProperty.propertyId) \
            .join(XcOpman, XcRoleProperty.roleId == XcOpman.roleId) \
            .filter(XcOpman.opManId == phone, XcRoleProperty.propertyType == PropertyType.OP_AREA.value,
                    XcRoleProperty.propertyId == service_id).first()
        return bool(first)
