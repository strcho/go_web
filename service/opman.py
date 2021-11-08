from model.all_model import *
from mbutils import dao_session, logger
from . import MBService
from sqlalchemy import and_


class OpManService(MBService):

    def query_one(self, valid_data, is_device=0):
        phone, device_code, _, _ = valid_data
        query = {
            "opManId": phone,
            "isDevice": is_device
        }
        query = self.remove_empty_param(query)
        opman = dao_session.session().query(XcOpman).filter_by(**query).first()
        if opman:
            return opman.isDevice, opman.name, opman.opManId
        else:
            return 0, '', ''

    def update_one(self, valid_data):
        opman_id, is_device = valid_data
        query = {
            "opManId": opman_id
        }
        query = self.remove_empty_param(query)
        dao_session.session().query(XcOpman).filter_by(**query).update({XcOpman.isDevice: is_device})
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()


class OpManDeviceService(MBService):

    def query_one_device(self, valid_data):
        phone, _, _, _ = valid_data
        device = dao_session.session().query(XcMieba2OpManDevice.id).filter(XcMieba2OpManDevice.phone == phone).first()
        return device[0] if device else 0

    def query_one(self, valid_data, login_address=''):
        phone, device_code, _, _ = valid_data
        device_info = dao_session.session().query(XcMieba2OpManDeviceLoginRecord.id.label('login_id'),
                                                  XcMieba2OpManDevice.id.label('device_id')) \
            .outerjoin(XcMieba2OpManDevice,and_(
                       XcMieba2OpManDevice.device_code == XcMieba2OpManDeviceLoginRecord.device_code,
                       XcMieba2OpManDevice.phone == XcMieba2OpManDeviceLoginRecord.phone)) \
            .filter(XcMieba2OpManDeviceLoginRecord.phone == phone)
        if login_address:
            device_info = device_info.filter(XcMieba2OpManDeviceLoginRecord.login_address == login_address).first()
        else:
            device_info = device_info.filter(XcMieba2OpManDeviceLoginRecord.device_code == device_code).first()
        if device_info:
            return device_info.login_id, device_info.device_id
        else:
            return '', ''

    def insert_one_login_record(self, valid_data, login_result=0, login_address=''):
        phone, device_code, device_name, device_type = valid_data
        login_record = XcMieba2OpManDeviceLoginRecord(
            device_code=device_code, device_name=device_name, device_type=device_type, phone=phone,
            login_result=login_result, login_at=datetime.now(), created_at=datetime.now(), updated_at=datetime.now(),
            login_address=login_address)
        dao_session.session().add(login_record)
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def insert_one(self, valid_data, opman_name=''):
        opman_id, device_code, device_name, device_type, phone = valid_data
        device = XcMieba2OpManDevice(
            device_code=device_code, device_name=device_name, device_type=device_type, phone=phone,
            opman_id=opman_id, opman_name=opman_name, created_at=datetime.now(), updated_at=datetime.now())
        dao_session.session().add(device)
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def delete_one(self, valid_data):
        device_id = valid_data
        dao_session.session().query(XcMieba2OpManDevice).filter(XcMieba2OpManDevice.id == device_id).delete()
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def update_one_login_record(self, valid_data, login_result=0, login_address=''):
        login_id = valid_data
        device_update = {
            "login_result": login_result,
            "login_at": datetime.now()
        }
        if login_address:
            device_update["login_address"] = login_address
        dao_session.session().query(XcMieba2OpManDeviceLoginRecord). \
            filter(XcMieba2OpManDeviceLoginRecord.id == login_id).update(device_update)
        try:
            dao_session.session().commit()
        except Exception as e:
            logger.exception(e)
            dao_session.session().rollback()

    def query_list(self, valid_data):
        phone, page, size = valid_data
        count = dao_session.session().query(func.count(XcMieba2OpManDevice.id)).\
            filter(XcMieba2OpManDevice.phone == phone).scalar()
        device_list = dao_session.session().query(XcMieba2OpManDevice).filter(XcMieba2OpManDevice.phone == phone).\
            order_by(XcMieba2OpManDevice.created_at.desc()).limit(size).offset(page * size).all()
        rows = [{
            "id": first.id,
            "opman_id": first.opman_id,
            "opman_name": first.opman_name,
            "device_code": first.device_code,
            "device_name": first.device_name,
            "device_type": first.device_type,
            "phone": first.phone,
        } for first in device_list]
        return count, rows

    def query_list_login_record(self, valid_data):
        phone, page, size = valid_data
        count = dao_session.session().query(func.count(XcMieba2OpManDeviceLoginRecord.id)).filter(
            XcMieba2OpManDeviceLoginRecord.phone == phone).scalar()
        login_list = dao_session.session().query(XcMieba2OpManDeviceLoginRecord, XcMieba2OpManDevice.id).outerjoin(
            XcMieba2OpManDevice, and_(XcMieba2OpManDevice.device_code == XcMieba2OpManDeviceLoginRecord.device_code,
                                 XcMieba2OpManDevice.phone == XcMieba2OpManDeviceLoginRecord.phone)). \
            filter(XcMieba2OpManDeviceLoginRecord.phone == phone). \
            order_by(XcMieba2OpManDeviceLoginRecord.login_at.desc()).limit(size).offset(page * size).all()
        rows = [{
            "id": first.id,
            "login_time": self.datetime2num(first.login_at),
            "login_result": first.login_result,
            "device_code": first.device_code,
            "device_name": first.device_name,
            "device_type": first.device_type,
            "phone": first.phone,
            "is_authorize": 1 if device_id else 0,
        } for first, device_id in login_list]
        return rows, count
