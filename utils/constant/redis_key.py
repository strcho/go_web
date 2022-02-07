"""大写字母 = 小写字母的实际值’xc_ebike_deposit_{}‘ ，注释key的场景,为了兼容node，xc_ebike_为前缀"""
# 用户押金
DEPOSIT_USER_KEY = "xc_ebike_deposit_{}"
# 用户免单
FREE_USER_KEY = "xc_ebike_free_order_{}"
# 用户状态
USER_STATE_KEY = "xc_ebike_2_userState_{}"
# 用户押金卡
DEPOSITCARD_USER_KEY = "xc_ebike_depositCard_{}"
# 有押金用户
DEPOSITED_GROUP_KEY = "xc_ebike_deposit_card_expired_but_deposited_user"

# 互斥锁
NX_JOIN_CUS_ACT_KEY = "xc_ebike_nx_join_cus_act_{}_{}"
NX_GET_REWARD_CUS_ACT_KEY = "xc_ebike_nx_get_reward_cus_act_{}_{}"

# 配置的路由
CONFIG_ROUTER_SERVICE_KEY = "xc_ebike_2_config_{router}_{serviceId}"
CONFIG_ROUTER_KEY = "xc_ebike_2_config_{router}"

# 自定义活动用户过滤选项
CUSTOM_ACTIVITY_USER_FILTER = "xc_ebike_2_cus_act_user_filter_{}"

# 定时任务锁
SCHEDULE_JOB = "xc_anfu_schedule_job_{}"

# 用户查询自定义活动频率控制
CUSTOM_ACTIVITY_QUERY_CONTROL = "xc_ebike_cus_act_query_control_{}_{}"

# 活动奖励已经发完了，不需要再判断了
CUSTOM_ACTIVITY_QUOTA_FULL = "xc_ebike_cus_act_quota_full_{}"

# 大屏导出报表加锁 & 用户订单导出报表加锁
EXPORT_EXCEL_LOCK = "xc_ebike_export_excel_lock"

# 派单相关功能
DISPATCH_REPEAT_CHECK = "xc_mieba_repeat_check_{imei}"
DISPATCH_IMEI_ON_TASK = "xc_mieba_dispatch_on_task_{service_id}"  # 服务区下已经派单的车辆
DISPATCH_READY_SEND_TO_MANAGER = "xc_mieba_ready_send_to_manager"  # 已经派发但是不确定是否单子都被接收的列表
DISPATCH_BACKFLOW_QUEUE = 'xc_mieba_backflow_queue'  # 派单流回任务池重新派发

DISPATCH_WORKMAN_CONFIG = "xc_mieba_{workman_id}_config"
DISPATCH_WORKMAN_TASK_NUM = "xc_mieba_task_num_{workman_id}"  # 工人手上任务单数目
DISPATCH_WORKMAN_START_LIST = "xc_mieba_{service_id}_start_list"  # 服务区下所有开工工人
DISPATCH_WORKMAN_STOP_LIST = "xc_mieba_{service_id}_stop_list"

DISPATCH_WORKMAN_HOUR_POSITION = "xc_mieba_{service_id}_{hour}_position"  # 服务区整点前工人位置分布

DISPATCH_WORKMAN_PERSON_ZSET = "xc_mieba_{service_id}_{workman_id}_person_zset"
DISPATCH_WORKMAN_PERSON_HASH = "xc_mieba_{service_id}_{workman_id}_person_hash"
DISPATCH_MANAGER_PERSON_ZSET = "xc_mieba_{service_id}_manager_person_zset"
DISPATCH_DATAHUB_BREAK_POINT = "xc_mieba_datahub_breakpoint"

DISPATCH_TICKETS_ZSET = "xc_mieba_tickets_zset"  # ALARM = 1  # 报警单 FIX = 2  # 检修单
DISPATCH_FLOW_HASH = "xc_mieba_flow_{hour}"

DISPATCH_FIND_CAR = "xc_mieba_find_car"
DISPATCH_SAME_PATH = "xc_mieba_same_path_{position}"
DISPATCH_USER_FIND_CAR = "xc_mieba_find_{user_id}"
DISPATCH_FIND_CAR_HASH = "xc_mieba_find_car_hash"
# 根据carId获取imei的KEY
CAR_2_IMEI_KEY = 'xc_ebike_carImeiBindings_{car_id}'
# 根据imei获取carId的KEY
IMEI_2_CAR_KEY = 'xc_ebike_imeiCarBindings_{imei}'

# 获取当前选择服务区下的所有车辆imei  xc_ebike_serviceGfence_{service_id}_deviceCount SET
SERVICE_DEVICE_All_IMEIS = "xc_ebike_serviceGfence_{service_id}_deviceCount"

# 根据imei获取设备信息   xc_ebike_device_info_{imei} HASH
IMEI_BINDING_DEVICE_INFO = "xc_ebike_device_info_{imei}"
# 记录能够上传soc信息的设备
DEVICE_REPORT_SOC = "xc_ebike_device_report_soc_{imei}"

# 根据carid获取电池battery_name   xc_ebike_xc_battery_Name_{car_id} STRING
CAR_BINDING_BATTERY_NAME = "xc_ebike_xc_battery_Name_{car_id}"

# 获取车辆锁车时的时间 xc_ebike_{service_id}_lockCarTime ZSET
LOCK_CAR_TIME = "xc_ebike_{service_id}_lockCarTime"

# 获取代理商某类用户总量 xc_ebike_2_userStateCount_{state} SET
USER_STATE_COUNT = "xc_ebike_2_userStateCount_{state}"
# 报表统计-财务对账-导出商户对账报表 加锁
EXPORT_MERCHANT_REPORT_LOCK = "xc_mieba_export_merchant_report_lock"

# 报表统计-财务对账-导出营收对账报表 加锁
EXPORT_REVENUE_REPORT_LOCK = "xc_mieba_export_revenue_report_lock"

# 报表统计-财务对账-导出结算对账报表 加锁
EXPORT_SETTLEMENT_REPORT_LOCK = "xc_mieba_export_settlement_report_lock"

# 大屏显示数据
BIG_SCREEN_STATISTICS_COST = "xc_mieba_big_screen_statistics_cost_{operation_type}"
BIG_SCREEN_STATISTICS_NUM = "xc_mieba_big_screen_statistics_num_{operation_type}"
BIG_SCREEN_STATISTICS_PENALTY = "xc_mieba_big_screen_statistics_order_penalty_{operation_type}"
BIG_SCREEN_STATISTICS_RECHARGE = "xc_mieba_big_screen_statistics_recharge_cost_{operation_type}"
BIG_SCREEN_STATISTICS_PRESENT = "xc_mieba_big_screen_statistics_present_cost_{operation_type}"

BIG_SCREEN_STATISTICS_DEPOSIT_INCOME = "xc_mieba_big_screen_statistics_deposit_income"
BIG_SCREEN_STATISTICS_RIDING_INCOME = "xc_mieba_big_screen_statistics_riding_income"
BIG_SCREEN_STATISTICS_WALLET_INCOME = "xc_mieba_big_screen_statistics_wallet_income"

BIG_SCREEN_STATISTICS_WALLET_RECHARGE = "xc_mieba_big_screen_statistics_wallet_recharge"  # 总充值金额退款
BIG_SCREEN_STATISTICS_WALLET_PRESENT = "xc_mieba_big_screen_statistics_wallet_present"  # 总赠送金额退款

# 坏账数据缓存
BIG_SCREEN_STATISTICS_ARREARS_PROPORTION = "xc_mieba_big_screen_arrears_proportion_{month}"

# 商户统计（微信，支付宝，云闪付）merchants(account-MERCHANTS_PAY)
BIG_SCREEN_STATISTICS_MERCHANTS = "xc_mieba_big_screen_statistics_merchants_{merchants}"
BIG_SCREEN_STATISTICS_MERCHANTS_COUNT = "xc_mieba_big_screen_statistics_merchants_count_{merchants}"
BIG_SCREEN_STATISTICS_MERCHANTS_REFUND = "xc_mieba_big_screen_statistics_merchants_refund_{merchants}"
BIG_SCREEN_STATISTICS_MERCHANTS_REFUND_COUNT = "xc_mieba_big_screen_statistics_merchants_refund_count_{merchants}"

BIG_SCREEN_DEPOSIT_MERCHANTS = "xc_mieba_big_screen_deposit_merchants_{merchants}"
BIG_SCREEN_DEPOSIT_MERCHANTS_COUNT = "xc_mieba_big_screen_deposit_merchants_count_{merchants}"
BIG_SCREEN_DEPOSIT_MERCHANTS_REFUND = "xc_mieba_big_screen_deposit_merchants_refund_{merchants}"
BIG_SCREEN_DEPOSIT_MERCHANTS_REFUND_COUNT = "xc_mieba_big_screen_deposit_merchants_refund_count_{merchants}"

BIG_SCREEN_DEPOSIT_CARD_MERCHANTS = "xc_mieba_big_screen_deposit_card_merchants_{merchants}"
BIG_SCREEN_DEPOSIT_CARD_MERCHANTS_COUNT = "xc_mieba_big_screen_deposit_card_merchants_count_{merchants}"
BIG_SCREEN_DEPOSIT_CARD_MERCHANTS_REFUND = "xc_mieba_big_screen_deposit_card_merchants_refund_{merchants}"
BIG_SCREEN_DEPOSIT_CARD_MERCHANTS_REFUND_COUNT = "xc_mieba_big_screen_deposit_card_merchants_refund_count_{merchants}"

BIG_SCREEN_WALLET_MERCHANTS = "xc_mieba_big_screen_wallet_merchants_{merchants}"
BIG_SCREEN_WALLET_MERCHANTS_COUNT = "xc_mieba_big_screen_wallet_merchants_count_{merchants}"
BIG_SCREEN_WALLET_MERCHANTS_REFUND = "xc_mieba_big_screen_wallet_merchants_refund_{merchants}"
BIG_SCREEN_WALLET_MERCHANTS_REFUND_COUNT = "xc_mieba_big_screen_wallet_merchants_refund_count_{merchants}"

# 脚本执行时间
SCRIPT_EXECUTE_TIME = "xc_mieba_script_execute_time"

# 大屏数据重建锁
BIG_SCREEN_SCRIPT = "xc_mieba_script"

# 车辆相关的key
IMEI_LIST = "xc_ebike_serviceGfence_{service_id}_deviceCount"  # 获取服务区所有的imei

DEVICE_INFO = "xc_ebike_device_info_{imei}"  # 查询设备的当前信息

CAR_BINDING = "xc_ebike_imeiCarBindings_{imei}"  # 查询设备的车辆号

BATTERY_NAME = "xc_ebike_xc_battery_Name_{car_id}"  # 查询电池名称

# 获取当前选择服务区下的某种类型车辆imei xc_ebike_serviceGfence_{service_id}_stateDevices{state} SET
SERVICE_DEVICE_STATE = "xc_ebike_serviceGfence_{service_id}_stateDevices{state}"
AGENT_DEVICE_STATE = "xc_ebike_{agent_id}_stateDevices{state}"
ALLY_DEVICE_STATE = "xc_ebike_ally_null_stateDevices{state}"  # 加盟商
STATE_DEVICE = SERVICE_DEVICE_STATE  # 获取服务区所有的异常工单设备

FIX_DEVICE = "xc_ebike_serviceGfence_{service_id}_2FixDevices"  # 获取服务区所有的维修工单设备
AGENT_FIX_DEVICE = "xc_ebike_{agent_id}_2FixDevices"
ALLY_FIX_DEVICE = "xc_ebike_ally_null_2FixDevices"  # 加盟商

ALARM_DEVICE = "xc_ebike_serviceGfence_{service_id}_alarmDevices{alarm_type}"  # 获取服务区所有的异常工单设备
AGENT_ALARM_DEVICE = "xc_ebike_{agent_id}_alarmDevices{state}"
ALLY_ALARM_DEVICE = "xc_ebike_ally_null_alarmDevices{state}"  # 加盟商

GFENCE_INFO = "xc_ebike_gFence_2_{gfence_id}"
GFENCE_PILE_SET = "xc_ebike_gfence_pile_set"
IMEI_USER = "xc_ebike_deviceRidingUsrId_{imei}"  # 通过imei获取用户的id

MOVE_CAR_STATE = "xc_ebike_move_bike_{imei}"  # 通过imei获取用户的id

CAR_INFO_DATA = "xc_ebike_car_info_agg_by_hours_{hour}"  # 数据统计相关，24小时数据

SERVICE_ASSIGNED_WORKMANIDS = "xc_mieba_assigned_workmanids_{service_id}"

# 用户标识获取设备
USER_RIDING_DEVICE = "xc_ebike_{agent_id}_usrRidingDevice_{user_id}"
# imei 和 agent_id 绑定关系
IMEI_AGENT_BINDING = "xc_ebike_imeiAgentBinding_{imei}"
# 用户和代理商的绑定关系
USER_AGENT_BINDING = "xc_ebike_usrAgentBindings_{user_id}"
# device状态
DEVICE_STATE = "xc_ebike_device_state_{imei}"
# 通过device查看用户标识
DEVICE_RIDING_USER_ID = "xc_ebike_deviceRidingUsrId_{imei}"
# 查看用户状态
USER_STATE = "xc_ebike_2_userState_{user_id}"
# 车辆行程信息
DEVICE_ITINERARY_INFO = "xc_ebike_device_itinerary_info_{imei}"

# 转化过个人骑行卡的列表
USER_SUPER_CARD = "xc_ebike_user_super_card"
# 回滚转化过得个人骑行卡的列表
REVERT_USER_SUPER_CARD = "xc_ebike_revert_user_super_card"

ALL_USER_LAST_SERVICE_ID = "xc_ebike_all_user_last_service_id"
# 老骑行卡今日使用次数
RECE_TIMES_KEY = "xc_ebike_2_riding_card_used_times_{user_id}"

# 换电状态
DEVICE_STATE_BATTERY = "xc_ebike_device_state_battery_{imei}"
CHANGE_BATTERY_LOCK = "xc_ebike_change_bettery_lock{op_man_id}{imei}"
# 获取当前设备对应的服务区电子围栏id
IMEI_GFENCE_BIND = "xc_ebike_imeiServiceGfenceBinding_{imei}"
GFENCE_RADIUS = "xc_ebike_2_gfence_radius_{gfence_type}"
# 挪车锁
MOVING_STATE_CARID = "xc_ebike_in_movsuoing_state_{car_id}"
REPAIR_STATE_CARID = "xc_ebike_repair_{car_id}"
# 优惠卡配置变更
COST_CHANGE_NOTIFY = "xc_ebike_cost_change_notify_favorable_{service_id}"

# 挪车持续时长
MOVING_STATE_TIME = "xc_ebike_in_moving_state_time"

# 设备与站点绑定关系
DEVICE_BINDING_PARK_KEY = "xc_ebike_imeiParkGfenceBinding_{imei}"
TEMP_DEVICE_BINDING_PARK_KEY = "xc_ebike_tempDeviceBindingPakKey_{imei}"

# 出围栏标志
Out_Gfence_Service_Flag = "xc_ebike_OutGfenceServiceFlag_{imei}"
# 设备与禁停区绑定关系
DEVICE_BINDING_NO_PARK_KEY = "xc_ebike_imeiNoParkGfenceBinding_{imei}"
# 站点和设备的绑定关系
PARK_GFENCE_BINGING_IMEI = "xc_ebike_ParkGfenceBindingimei_{park_id}"
# 挪车中标志
MOVE_BIKE_ZSET = "xc_ebike_move_bike_zset"
DYNAMIC_MOVE_EBIKE_KEY = 'xc_ebike_dynamic_move_ebike_key'
# 超一日无单
ONE_DAY_WITHOUT_ORDER = "xc_ebike_oneDayWithoutOrder_{imei}"
# 记录车辆的开锁失败次数和类型
UNLOCK_FAIL_INFO = "xc_ebike_unlockFailInfo_{car_id}"
# 大屏统计用户数据的缓存
BIG_SCREEN_USER_INFO = "xc_mieba_big_screen_user_info"
BIG_SCREEN_USER_INFO_TREE = "xc_mieba_big_screen_user_info_tree"

# 押金卡已过期但是用户还是押金已缴纳状态的用户
USER_DEPOSIT_CARD_EXPIRED = "xc_ebike_deposit_card_expired_but_deposited_user"

# 支付锁
PAY_TYPE_TRADE_NO = "xc_ebike_{payType}_{tradNo}"

# 充值活动，活动id的记录
ACTIVE_ID_ROUTER_TRADE_NO = "xc_ebike_activeId_router_{trade_no}"

# ali暂存交易关联的一些信息
ALI_TRADE_INFO = "xc_ebike_alipay_trade_create_notify_{}"

DEPOSIT_CARD_USER_ID = "xc_ebike_depositCard_{user_id}"

# 获取当前服务区,该种类型商品的支付次数
GET_SERVICE_PAYMENT_COUNT = "SeparateProportion_{payment_type}_{service_id}"
REFUND_FAVORABLECARD_LOCK = "xc_ebike_REFUND_FAVORABLECARD_{object_id}_lock"
REFUND_RIDINGCARD_LOCK = "xc_ebike_REFUND_RIDINGCARD_{object_id}_lock"
REFUND_DEPOSITCARD_LOCK = "xc_ebike_REFUND_DEPOSITCARD_{object_id}_lock"


WXLITE_PAYMENT = "xc_mieba_payment_wxlite_{pay_type}"


# 新key

USER_WALLET_CACHE = "user_account_wallet_{tenant_id}_{pin}"
# FREE_ORDER_USER_CACHE = "user_account_free_order_{tenant_id}_{pin}"


EDIT_USER_FAVORABLE_CARD_LOCK = "edit_user_favorable_card_{tenant_id}_{pin}_{service_id}"


USER_REFUND_RECHARGE_LOCK = "user_refund_recharge_lock_{pin}"