import typing
from marshmallow import fields


class EDateTime(fields.DateTime):

    def __init__(self, format: typing.Optional[str] = "%Y-%m-%d %H:%M:%S", **kwargs):
        super().__init__(format=format, **kwargs)
