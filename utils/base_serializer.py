from marshmallow import (
    Schema,
    fields,
)


class CommandContext(Schema):
    tenant_id = fields.String(required=False, description="租户ID", data_key='tenantId')
    traceId = fields.String(required=False, description="链路追踪ID")
    pin = fields.String(required=False, description="用户PIN")
    ip = fields.String(required=False, description="ip地址")
    source = fields.String(required=False, description="系统标识")
    stressTesting = fields.String(required=False, description="false  压测标识)true 压测请求 false 正常请求")


class ReqBaseDeserializer(Schema):

    commandContext = fields.Nested(CommandContext, required=True, description="公共请求信息")
