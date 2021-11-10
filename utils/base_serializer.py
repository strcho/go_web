from marshmallow import (
    Schema,
    fields,
)


class CommandContext(Schema):
    tenantId = fields.String(required=False, description="租户ID")
    traceId = fields.String(required=False, description="链路追踪ID")
    pin = fields.String(required=False, description="用户PIN")
    ip = fields.String(required=False, description="ip地址")
    source = fields.String(required=False, description="系统标识")
    stressTesting = fields.String(required=False, description="false  压测标识)true 压测请求 false 正常请求")


class ReqBaseSerializer(Schema):

    commandContext = fields.Nested(CommandContext, required=True, description="公共请求头部")
