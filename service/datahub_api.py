from decimal import Decimal
import time
import urllib3
from datahub import DataHub
from datahub.models import RecordSchema, CursorType
from datahub.models import TupleRecord

urllib3.disable_warnings()
dh = DataHub('LTAI4GAXfaboHV45vZvgGVo7', 'QxkkD6S3sWQYTQ0YS8ILm5096KUMcu',
             endpoint='https://dh-cn-shenzhen.aliyuncs.com')

project_name = 'ebike'


# ============================= put tuple records =============================
def workman_send_2_datahub(values: list):
    """
    values满足下面datahub数据结构顺序的接口
    """
    try:
        record_schema = RecordSchema.from_json(
            {'fields': [
                {'name': 'agent_name', 'type': 'string'},
                {'name': 'service_id', 'type': 'bigint'},
                {'name': 'workman_id', 'type': 'bigint'},
                {'name': 'lat', 'type': 'decimal'},
                {'name': 'lng', 'type': 'decimal'},
                {'name': 'report_time', 'type': 'bigint'},
                {'name': 'capacity', 'type': 'bigint'}]
            })
        dh.put_records_by_shard(project_name, 'workman_gps', "0",
                                [TupleRecord(schema=record_schema, values=values)])
    except Exception as ex:
        print("Exception:send 2 datahub error")
        print(ex)


def dispatch_task_send_2_datahub(values: list):
    """
    values满足下面datahub数据结构顺序的接口
    """
    record_schema = RecordSchema.from_json(
        {'fields': [
            {'name': 'agent_name', 'type': 'string'},
            {'name': 'service_id', 'type': 'bigint'},
            {'name': 'imei', 'type': 'bigint'},
            {'name': 'ticket_id', 'type': 'bigint'},
            {'name': 'report_time', 'type': 'bigint'},
            {'name': 'start_lat', 'type': 'decimal'},
            {'name': 'start_lng', 'type': 'decimal'},
            {'name': 'end_lat', 'type': 'decimal'},
            {'name': 'end_lng', 'type': 'decimal'},
            {'name': 'state', 'type': 'bigint'},
            {'name': 'origin_type', 'type': 'bigint'},
            {'name': 'actual_type', 'type': 'bigint'},
            {'name': 'enable', 'type': 'boolean'},
            {'name': 'other', 'type': 'string'}]
        })

    values = [0.0 if item == Decimal('0E-10') else item for item in values]
    put_result = dh.put_records_by_shard(project_name, 'car_ticket', "0",
                                         [TupleRecord(schema=record_schema, values=values)])
    if put_result:
        print('send to datahub result:', put_result)


def dispatch_task_datahub_get_flow():
    """
    values满足下面datahub数据结构顺序的接口
    """
    record_schema = RecordSchema.from_json(
        {'fields': [
            {'name': 'agent_name', 'type': 'string'},
            {'name': 'service_id', 'type': 'bigint'},
            {'name': 'flow', 'type': 'string'},
            {'name': 'report_time', 'type': 'bigint'}]
        })
    tuple_cursor_result = dh.get_cursor(project_name, 'car_flow', '0', CursorType.SYSTEM_TIME,
                                        param=int((time.time() - time.localtime().tm_min * 60) * 1000))
    get_result = dh.get_tuple_records(project_name, "car_flow", '0', record_schema, tuple_cursor_result.cursor,
                                      limit_num=1000)
    return [record.values for record in get_result.records]
