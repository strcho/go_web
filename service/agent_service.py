from model.all_model import XcOpman, XcRoleProperty
from service import MBService
from mbutils import dao_session


class AgentService(MBService):

    def get_all_services(self):
        """
        获取所有的service_id
        :return:service_ids [1,2,3]
        """

        service_sql = """
                        SELECT
                            xeg2.id
                        FROM
                            xc_ebike_gfence_2 AS xeg2 
                        WHERE
                            xeg2.type = 1
                            AND xeg2.deletedAt IS NULL;
                       """
        service_info = dao_session.session().execute(service_sql).fetchall()
        service_ids = [s[0] for s in service_info]
        return service_ids

    def get_manager_service_ids(self, manager_id):
        """
        根据手机号获取当前管理人所管辖的服务区
        :param manager_id:
        :return:[1,2,3]
        """
        role_id_info = dao_session.session().query(XcOpman.roleId).filter(XcOpman.opManId == manager_id).first()
        if not role_id_info:
            return [0]
        role_id = role_id_info.roleId

        service_ids_info = dao_session.session().query(XcRoleProperty.propertyId).filter(
            XcRoleProperty.roleId == role_id).all()
        if not service_ids_info:
            return [0]
        service_ids = [s[0] for s in service_ids_info]
        return service_ids


if __name__ == '__main__':
    res = AgentService()
    result = res.get_all_services()
