import math
import requests
import json
from mbutils import cfg, MbException, dao_session, logger
from utils.constant.redis_key import GFENCE_RADIUS
from utils.constant.device import GfenceType
from functools import reduce


def wgs84_to_gcj02(lon, lat):
    """ 国际标准wgs84转成国内标准gcj02 """
    pi = 3.1415926535897932384626
    a = 6378245.0
    ee = 0.00669342162296594323

    def transform_lat(x, y):
        ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * pi) + 20.0 * math.sin(2.0 * x * pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(y * pi) + 40.0 * math.sin(y / 3.0 * pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(y / 12.0 * pi) + 320 * math.sin(y * pi / 30.0)) * 2.0 / 3.0
        return ret

    def transform_lon(x, y):
        ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * pi) + 20.0 * math.sin(2.0 * x * pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(x * pi) + 40.0 * math.sin(x / 3.0 * pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(x / 12.0 * pi) + 300.0 * math.sin(x / 30.0 * pi)) * 2.0 / 3.0
        return ret

    dLat = transform_lat(lon - 105.0, lat - 35.0)
    dLon = transform_lon(lon - 105.0, lat - 35.0)
    radLat = lat / 180.0 * pi
    magic = math.sin(radLat)
    magic = 1 - ee * magic * magic
    sqrtMagic = math.sqrt(magic)
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi)
    dLon = (dLon * 180.0) / (a / sqrtMagic * math.cos(radLat) * pi)
    mgLat = lat + dLat
    mgLon = lon + dLon
    return mgLon, mgLat


def parse_gps_address(lng, lat, gps_type):
    """
    根据gps获取地理位置信息
    高德接口示例:
    {'status': '1', 'regeocode':
    {'addressComponent':
    {'city': '武汉市',
    'province': '湖北省',
    'adcode': '420111',
    'district': '洪山区',
    'towncode': '420111080000',
    'streetNumber': {
    'number': '77号',
    'location': '114.434915,30.500425',
    'direction': '北',
    'distance': '18.9759',
    'street': '光谷创业街'},
    'country': '中国',
    'township': '关东街道',
    'businessAreas': [{'location': '114.409500,30.495205', 'name': '关山', 'id': '420111'}, {'location': '114.414914,30.495456', 'name': '光谷', 'id': '420111'}],
    'building': {'name': [], 'type': []},
    'neighborhood': {'name': [], 'type': []}, 'citycode': '027'}, 'formatted_address': '湖北省武汉市洪山区关东街道光谷创业街77号中国武汉留学生创业园(光谷创业街)'},
    'info': 'OK',
    'infocode': '10000'}

    :param lng:
    :param lat:
    :param gps_type: gcj02/wgs84
    :return:
    """

    if gps_type == "wgs84":
        lng, lat = wgs84_to_gcj02(lng, lat)  # 国际标准wgs84转成国内标准gcj02
    key = cfg["amapConfig"]["key"]
    url = f'https://restapi.amap.com/v3/geocode/regeo?key={key}&output=json&location={lng},{lat}'
    headers = {'Content-type': 'application/json'}
    try:
        response = requests.get(url=url, headers=headers)
        data = response.json()
        # logger.info("高德接口:", response.text)
        address = data["regeocode"]["addressComponent"]
        return address.get("district", "") + \
               address.get("township", "") + \
               "".join(address.get("building", {}).get("name", [])) + \
               address.get("streetNumber", {}).get("street", "") + \
               address.get("streetNumber", {}).get("number", "") + \
               address.get("streetNumber", {}).get("direction", "")
    except Exception as ex:
        # logger.info("高德接口错误:", ex)
        raise MbException("高德接口错误")


def inside_polygon_area(point: [], polygon_list: []):
    """
    获取当前位置所在的serviceId和parkingId,park_service_id = getRelition(park, [], cur_location)
    :param point:
    :param polygon_list: [{"gFenceId":1567,"pointList":[(x,y),(x,y)]}] 服务区列表,或者站点列表
    :return: gfence_id or None
    """
    for polygon in polygon_list:
        if is_pt_in_poly(point, polygon["pointList"]):
            return polygon["gFenceId"]
    return None


def is_pt_in_poly(pt, poly):
    """
     判断点是否在多边形内部的(pnpoly算法)
    :param pt: (x,y)
    :param poly: [(x,y),(x,y),(x,y),(x,y)]
    :return:
    """
    nvert = len(poly)
    vertx = []
    verty = []
    testx = pt[0]
    testy = pt[1]
    for item in poly:
        vertx.append(item[0])
        verty.append(item[1])

    j = nvert - 1
    res = False
    for i in range(nvert):
        if (verty[j] - verty[i]) == 0:
            j = i
            continue
        x = (vertx[j] - vertx[i]) * (testy - verty[i]) / (verty[j] - verty[i]) + vertx[i]
        if ((verty[i] > testy) != (verty[j] > testy)) and (testx < x):
            res = not res
        j = i
    return res


def search_from_yuntu(tp, lng, lat, radius, count=20):
    """
    根据所给的GPS在yuntu中查找附近的电子围栏，并返回数据列表
    :param type:
    :param lng:
    :param lat:
    :param radius:
    :return:[ {   _id: '1',
                    _location: '114.525,30.511',
                    _name: '1',
                    _address: '湖北省武汉市江夏区丁姑山',
                    _createtime: '2017-12-02 16:12:54',
                    _updatetime: '2017-12-02 16:17:49',
                    _province: '湖北省',
                    _city: '武汉市',
                    _district: '江夏区',
                    _distance: '0',
                    _image: [] } ]
    """
    if not lng or not lat:
        return
    radius = int(dao_session.redis_session.r.get(GFENCE_RADIUS.format(gfence_type=tp)) or 0) or radius  # 优先用radius半径
    if tp == GfenceType.AT_SERVICE.value:
        # 以前迁移留下来的,云图的地址
        table_id = cfg["yuntu"]["gFenceServiceTableid"]
    elif tp == GfenceType.FOR_PARK.value:
        table_id = cfg["yuntu"]["gFenceUsableTableid"]
    elif tp == GfenceType.NO_PARKING.value:
        table_id = cfg["yuntu"]["noParkingTableid"]
    elif tp == GfenceType.TBEACON_PARKING.value:
        table_id = cfg["yuntu"]["gFenceUsableTableid"] + '_tbeacon'
    elif tp == GfenceType.CHANGE_BATTERY_PARKING.value:
        table_id = f"{tp}_changeBatteryTableid"
    elif tp == GfenceType.MOVE_CAR_PARKING.value:
        table_id = f"{tp}_moveCarTableid"
    elif tp == GfenceType.OPERATION_PARKING.value:
        table_id = f"{tp}_operationTableid"
    elif tp == GfenceType.NO_CRAWL_PARKING.value:
        table_id = f"{tp}_noCrawlTableid"
    else:
        return
    table_id = "xc_ebike_" + table_id
    datas = dao_session.redis_session.r.georadius(table_id, lng, lat, radius, unit="m", withdist=True, withcoord=True,
                                                  count=count, sort='ASC')
    logger.info("search_from_yuntu datas:", datas, table_id, lng, lat)
    return [{
        "_name": item[0],
        '_distance': int(item[1])
    } for item in datas]


def imei_trail_from_tsdb(imei, start_time: float, end_time: float) -> list:
    """
    tsdb结果:
    [{
    'metric': 'xc.device.gps',
    'tags': {'imei': '861251057159318', 'imsi': '861251057159318', 'objectType': '100', 'type': 'tcp'},
    'aggregateTags': [],
    'dps': {'1625558622': '{"hdop":"0.6000000238418579","lng":112.1729404071885,"lat":30.346452640736867,"timestamp":1625558617,"wgs84Lat":30.348907,"wgs84Lng":112.1671,"speed":0,"course":0,"totalMiles":"2167847","headingAngle":"","bmsSoc":"66","rfidAck":""}',
    '1625558626': '{"hdop":0.6000000238418579,"satellite":12,"lng":112.17293940844831,"lat":30.34645120575542,"timestamp":1625558622,"wgs84Lat":30.348905563354492,"wgs84Lng":112.16709899902344,"speed":0,"course":0,"totalMiles":"2167847"}',
    '1625558627': '{"hdop":0.6000000238418579,"satellite":12,"lng":112.17293940844831,"lat":30.34645120575542,"timestamp":1625558622,"wgs84Lat":30.348905563354492,"wgs84Lng":112.16709899902344,"speed":0,"course":0,"totalMiles":"2167847"}'
     }}]
    :param imei:
    :param start_time:10位, tsdb 限制小数点最多3位,表示毫秒级
    :param end_time:10位
    :return:
    """
    # 获得imei在这个时间段的轨迹
    tsdb_host = cfg["tsdb"]["host"]  # ts-wz9k945izy5k451l8.hitsdb.tsdb.aliyuncs.com
    tsdb_port = cfg["tsdb"]["port"]  # 8242
    # ----文档 https://help.aliyun.com/document_detail/60683.htm?spm=a2c4g.11186623.2.3.653c6803RoVmN4
    query = {
        "start": int(start_time) - 60,
        "end": int(end_time),
        "queries": [{
            "metric": "xc.device.gps",
            "aggregator": "none",
            "tags": {
                "imei": imei
            }
        }]
    }
    res = []
    try:
        response = requests.post(f"http://{tsdb_host}:{tsdb_port}/api/query", data=json.dumps(query),
                                 headers={'Content-type': 'application/json'})
        many = response.json()

        for one in many[0]["dps"].values():
            info = json.loads(one)
            res.append({
                "speed": float(info["speed"]),
                "course": float(info["course"]),
                "lon": float(info["wgs84Lng"]),
                "lat": float(info["wgs84Lat"]),
                "timestamp": info["timestamp"]
            })
    except Exception as ex:
        logger.error("获取tsdb中行程数据失败", ex)
    return res


def compute_distance_by_imei_trail(imei, start_time: float, end_time: float):
    """
    根据设备gps轨迹,计算移动总距离
    :param imei:
    :param start_time:10位, tsdb 限制小数点最多3位,表示毫秒级
    :param end_time:
    :return:
    """
    res = imei_trail_from_tsdb(imei, start_time, end_time)
    # 根据轨迹计算总里程
    total = 0
    if len(res) > 1:
        for i in range(len(res) - 1):
            total += spherical_distance(res[i]["lat"], res[i]["lon"], res[i + 1]["lat"], res[i + 1]["lon"])
            # logger.info("compute_distance_by_imei_trail total distance:", total, " res:", res)
    return total


def spherical_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """ 计算地球表面距离 """
    EARTH_REDIUS = 6378245.0
    pi = 3.14159265

    def rad(d):
        return d * pi / 180.0

    radLat1 = rad(lat1)
    radLat2 = rad(lat2)
    a = radLat1 - radLat2
    b = rad(lng1) - rad(lng2)
    s = 2 * math.asin(
        math.sqrt(math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    s = s * EARTH_REDIUS
    return s

# import requests
# import json
#
# tsdb_host = "ts-wz9k945izy5k451l8.hitsdb.tsdb.aliyuncs.com"
# tsdb_port = 8242
# imei = 861251057159318
# start_time = 1625558580
# end_time = 1625559360
# query = {
#     "start": start_time,
#     "end": end_time,
#     "queries": [{
#         "metric": "xc.device.gps",
#         "aggregator":"none",
#         "tags":{
#             "imei":imei
#         }
#     }]
# }
# print(requests.post(f"http://{tsdb_host}:{tsdb_port}/api/query", data=json.dumps(query),headers={'Content-type': 'application/json'}).json())
