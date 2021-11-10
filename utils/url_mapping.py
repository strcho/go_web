from mbutils.route_tool import include, url_wrapper, create_handlers
from mbutils import cfg


def add_handler(prefix):
    return url_wrapper([
        (r'{}'.format(prefix), include('routes.k8s_server')),
        (r'{}/pay'.format(prefix), include('routes.payment')),
        (r'{}/data_fix'.format(prefix), include('routes.data_fix')),
    ])


ROUTER = '/account'
K8SROUTER = ""
EBIKE_ROUTER = cfg.get("ROUTER", "")
handlers = create_handlers(ROUTER, add_handler, EBIKE_ROUTER)
