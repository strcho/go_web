from . import MbEnum

T1 = 120  # 找车时间(s)
T2 = 60  # 拍照时间(s)

T3 = 60 * 60  # 站点用车需求的间隔时间(s)


# 交通工具
class VehicleType(MbEnum):
    ELECTRIC = 1  # 电动车可挪车<=1辆
    TRICYCLE = 6  # 三轮车可挪车<=6辆
    TRUCK = 20  # 小卡车可挪车<=20辆

    @staticmethod
    def get_vehicle_type(move_car_capacity):
        """
        根据单次最大挪车能力判断车型
        :param move_car_capacity:
        :return:
        """
        if move_car_capacity <= VehicleType.ELECTRIC.value:
            return "ELECTRIC"
        elif VehicleType.ELECTRIC.value < move_car_capacity <= VehicleType.TRICYCLE.value:
            return "TRICYCLE"
        elif VehicleType.TRICYCLE.value < move_car_capacity <= VehicleType.TRUCK.value:
            return "TRUCK"
        else:
            return "TRUCK"


# 绩效因子k
K_CONFIG = {
    "MOVE": {
        "SINGLE": 1,  # 单台挪车
        "BATCH": 2  # 批量挪车
    },
    "CHANGE_BATTERY": 1,  # 换电
    "FIX": 1,  # 维修
    "INSPECT": 1,  # 巡检

}

VEHICLE_CONFIG = {
    "ELECTRIC": {
        # "KM": 2,  # 行驶里程km
        # "MIN": 12,  # 行程耗时min
        # "V": 10,  # 速度km/h
        "GD_TYPE": 1  # 高德路线类型 1：电动车  2：机动车
    },
    "TRICYCLE": {
        # "KM": 3,  # 行驶里程km
        # "MIN": 16,  # 行程耗时min
        # "V": 11.25,  # 速度km/h
        "GD_TYPE": 2  # 高德路线类型 1：电动车  2：机动车
    },
    "TRUCK": {
        # "KM": 4,  # 行驶里程km
        # "MIN": 17,  # 行程耗时min
        # "V": 14.12,  # 速度km/h
        "GD_TYPE": 2  # 高德路线类型 1：电动车  2：机动车
    },
}
