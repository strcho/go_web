from mbutils.route_tool import include, url_wrapper, create_handlers
from mbutils import cfg


def add_handler(prefix):
    return url_wrapper([
        (r'{}'.format(prefix), include('routes.k8s_server')),
        (r'{}/data_fix'.format(prefix), include('routes.data_fix')),
        (r'{}/internal'.format(prefix), include('routes.wallet')),
        (r'{}/internal'.format(prefix), include('routes.riding_card')),
        (r'{}/internal'.format(prefix), include('routes.favorable_card')),
        (r'{}/internal'.format(prefix), include('routes.free_order')),
        (r'{}/internal'.format(prefix), include('routes.discount')),
        (r'{}/internal'.format(prefix), include('routes.deposit_card')),
        (r'{}/internal'.format(prefix), include('routes.user_deposit')),
        (r'{}/internal'.format(prefix), include('routes.user_account')),
    ])


ROUTER = '/account'
K8SROUTER = ""
EBIKE_ROUTER = cfg.get("ROUTER", "")
handlers = create_handlers(ROUTER, add_handler, EBIKE_ROUTER)
