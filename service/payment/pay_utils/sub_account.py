from copy import deepcopy

from service.config import ConfigService
from service.payment.pay_utils.pay_constant import CONFIG_NAME
from mbutils import cfg, dao_session, logger
from utils.constant.redis_key import GET_SERVICE_PAYMENT_COUNT


class ABSSeparateConfig():
    def __init__(self, service_id):
        self.service_id = service_id or 1  # 分账服务区区分,默认不进行区分,子类进行实例化
        self.unionpay_lite = deepcopy(cfg.get('Unionpay_WXlite', {}))
        self.unionpay_lite_single = deepcopy(cfg.get('Unionpay_WXlite_Single', {}))

    def get_separate_service_ids(self):
        config_name = CONFIG_NAME.get('SEPARATECFG', 'separateCfg')
        res = ConfigService().get_router_content(config_name, self.service_id)  # todo 获取服务区配置，若没有服务区参数，则返回默认配置
        logger.info(f"get_separate_service_ids res:{res}")
        if not res:
            return {"serviceIds": []}
        return res

    def calculate_current_probability(self, payment_mod, payment_type):
        """
        计算当前的支付比例概率
        支付比例 = 支付总数 % 总的支付比例 (主配置的separateProportion参数)
        @param {number} payment_mod 支付被除数
        @param {str} payment_type 商品支付类型
        @returns {number} 支付比例
        """
        payment_count = dao_session.redis_session.r.incr(
            GET_SERVICE_PAYMENT_COUNT.format(payment_type=payment_type, service_id=self.service_id)) - 1  # 模数需要减1
        return payment_count % payment_mod

    @staticmethod
    def filter_current_pay_probability(probability, sub_account):
        """
        获取支付配置的主函数,根据概率列表进行选择
         [1, 2, 3] 1:1:1:7的分账 probabilit < 2 return subAccount[1]
        @param {number} probability 比例概率
        @param {Array} sub_account 子商户配置
        @returns {object} undefine ? mainConfig : subAccount[idx]
        """
        probability_list = [i.get('separateProportion') for i in sub_account]
        idx = 0
        while idx < len(probability_list):
            if probability < probability_list[idx]:
                return sub_account[idx]
            idx += 1
        return {}

    def get_current_payment_config(self, config, payment_type):
        """
        根据支付比例,获取相应的支付配置的 main() 函数
        1.如果配置中没有定义子账号,返回默认主账号配置
        2.如果配置中,没有定义分账比例,默认返回主账号配置
        3.根据支付的配置比例,返回对应的支付配置,使用redis记录标志位,默认使用全局,在constructor中可自定义区分类型
        @param {object} config
        @returns {object} 支付配置
        """
        current_config = deepcopy(config)
        # 如果配置了服务区，则只在服务区内分账生效
        service_ids = self.get_separate_service_ids()
        service_ids_info = service_ids.get("serviceIds", [])
        if service_ids_info and self.service_id not in service_ids_info:
            logger.info(f"get_current_payment_config service_id is error, "
                        f"service_ids:{service_ids}, service_id:{self.service_id}")
            return current_config

        # 根据支付配置,获取对应的子商户配置,和支付比例
        sub_account: list = current_config.get('sub_account', [])
        payment_mod: int = int(current_config.get('separateProportion', 0))  # 分账比例参数
        if not (sub_account or payment_mod):
            logger.info(f"get_current_payment_config service_id is error, "
                        f"sub_account:{sub_account}, payment_mod:{payment_mod}")
            return current_config

        payment_type = payment_type or "payment"  # 分账类型区分,默认不进行区分,子类进行实例化
        current_probability = self.calculate_current_probability(payment_mod, payment_type)
        current_config.update(self.filter_current_pay_probability(current_probability, sub_account))
        current_config.pop('sub_account')
        logger.info(f"打印当前的商户Id: {current_config.get('mch_id')}")
        return current_config

    def get_current_refund_config(self):
        """获取退款处的配置, 获取所有的配置"""
        result1 = self.config_result(self.unionpay_lite)
        result2 = self.config_result(self.unionpay_lite_single)
        return result1 + result2

    @staticmethod
    def config_result(config):
        result = []
        if config:
            result.append({
                    "key": config.get("key", ""),
                    "appId": config.get("appId", ""),
                    "mch_id": config.get("mch_id", ""),
                    "version": config.get("version", ""),
                    "sign_type": config.get("sign_type", ""),
                    "url": config.get("url", ""),
                    "notify_url_head": config.get("notify_url_head", ""),
                    "mchName": config.get("mchName", "")
                })
            sub_account = config.get("sub_account", [])
            for sub in sub_account:
                result.append({
                    "key": sub.get("key", ""),
                    "appId": sub.get("appId", ""),
                    "mch_id": sub.get("mch_id", ""),
                    "version": config.get("version", ""),
                    "sign_type": config.get("sign_type", ""),
                    "url": config.get("url", ""),
                    "notify_url_head": config.get("notify_url_head", ""),
                    "mchName": config.get("mchName", "")
                })
        return result

    def get_config(self, single):
        logger.info("single", single, "unionpay_lite_single", self.unionpay_lite_single)
        if single and self.unionpay_lite_single:
            return self.unionpay_lite_single
        return self.unionpay_lite


class SubAccount(ABSSeparateConfig):
    """分账业务"""

    def __init__(self, channel, service_id=False, single=False):
        super().__init__(service_id)
        self.single = single
        self.channel = channel

    def get_payment_config(self):
        """
         根据支付类型,获取点击支付处的配置
         @param {*} channel 枚举类型
         @param {*} serviceId
         @param {*} single 是否走单独账户，由前端控制
        """
        config = self.get_config(self.single)
        # if self.channel and self.channel in ("deposite", "depositeCard", "wallet", "ridingCard", "favorableCard"):
        """不做该种支付的购买数量统计"""
        if self.channel == "deposite":  # 押金不做单独特殊分账（根据前端传值进行判断）
            config = self.get_config(single=False)
            config = deepcopy(config)
            del config["sub_account"]
            logger.info(f"打印当前的商户Id: {config.get('mch_id', '')}")
            return config
        result = self.get_current_payment_config(config, self.channel)
        return result
        # return config

    def get_refund_config(self):
        result = self.get_current_refund_config()
        return result