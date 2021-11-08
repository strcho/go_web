import functools
import json

from apispec import APISpec
from apispec.exceptions import APISpecError
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.tornado import TornadoPlugin
from mbutils import cfg
from webargs.tornadoparser import TornadoParser

__all__ = [
    "use_args",
    "use_kwargs",
    "use_args_query",
    "use_kwargs_query",
    "SWAGGER_API_OUTPUT_FILE",
    "generate_swagger_file",
]


class Parser(TornadoParser):
    DEFAULT_VALIDATION_STATUS = 400


parser = Parser()
use_args = parser.use_args
use_kwargs = parser.use_kwargs

use_args_query = functools.partial(use_args, location="json")
use_kwargs_query = functools.partial(use_kwargs, location="json")

# 接口文档生成路径
SWAGGER_API_OUTPUT_FILE = "./swagger.json"


def generate_swagger_file(handlers, file_location: str = SWAGGER_API_OUTPUT_FILE):
    """Automatically generates Swagger spec file based on RequestHandler
    docstrings and saves it to the specified file_location.
    """

    # Starting to generate Swagger spec file. All the relevant
    # information can be found from here https://apispec.readthedocs.io/
    spec = APISpec(
        title="EbikePay API",
        version="1.0.0",
        openapi_version="3.0.2",
        info=dict(description="Documentation for the EbikePay API"),
        plugins=[TornadoPlugin(), MarshmallowPlugin()],
        servers=[
            {"url": f"http://localhost:{cfg['port']}/", "description": "Local environment",},
        ],
    )
    # Looping through all the handlers and trying to register them.
    # Handlers without docstring will raise errors. That's why we
    # are catching them silently.
    for handler in handlers:
        try:
            spec.path(urlspec=handler)
        except APISpecError:
            pass

    # Write the Swagger file into specified location.
    with open(file_location, "w", encoding="utf-8") as file:
        json.dump(spec.to_dict(), file, ensure_ascii=False, indent=4)
