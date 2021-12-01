from mbutils.route_tool import include, url_wrapper, create_handlers
from mbutils import cfg


def add_handler(prefix):
    return url_wrapper([
        (r'{}'.format(prefix), include('routes.k8s_server')),
        (r'{}/data_fix'.format(prefix), include('routes.data_fix')),
        (r'{}/wallet'.format(prefix), include('routes.wallet')),
        (r'{}/riding_card'.format(prefix), include('routes.riding_card')),
        (r'{}/favorable_card'.format(prefix), include('routes.favorable_card')),
        (r'{}/free_order'.format(prefix), include('routes.free_order')),
        (r'{}/discount'.format(prefix), include('routes.discount')),
        (r'{}/deposit_card'.format(prefix), include('routes.deposit_card')),
        (r'{}/user_deposit'.format(prefix), include('routes.user_deposit')),
        (r'{}/user_account'.format(prefix), include('routes.user_account')),
    ])


ROUTER = '/ebike_account'
K8SROUTER = ""
EBIKE_ROUTER = cfg.get("ROUTER", "")
handlers = create_handlers(ROUTER, add_handler, EBIKE_ROUTER)
