from marshmallow import (
    Schema,
    fields,
    INCLUDE,
)

from mbutils.mb_handler import ContextDeserializer


class CommandContext(ContextDeserializer):
    # tenant_id = fields.String(required=False, description="租户ID", data_key='tenantId')
    tenantId = fields.String(required=True, description="租户ID")
    traceId = fields.String(allow_none=True, description="链路追踪ID")
    pin = fields.String(allow_none=True, description="用户PIN")
    ip = fields.String(allow_none=True, description="ip地址")
    source = fields.String(allow_none=True, description="系统标识")
    stressTesting = fields.Bool(allow_none=True, description="false  压测标识)true 压测请求 false 正常请求")


class ReqBaseDeserializer(ContextDeserializer):
    # pass
    commandContext = fields.Nested(CommandContext, required=True, description="公共请求信息")


class BaseSchema(Schema):
    class Meta:
        unknown = INCLUDE
