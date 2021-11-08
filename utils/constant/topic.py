from . import MbEnum


# 原始派单分类
class TopicType(MbEnum):
    TASK_COME = 1  # 发送到工人手机用来抢单的消息
    TASK_GONE = 2  # 单已经被抢走的消息
    TASK_DECISION = 3  # 发送城市经理手机,用来决策是否派单的消息
    TASK_CANCEL = 4  # 工人发起取消申请的消息
    TASK_CHECK = 5  # 城市经理审批完的意见反馈


# 消息格式(客户名称/服务区id/消息类型/手机id)
TOPIC_DISPATCH_TASK_COME = "{agent_name}/{service_id}/1/{workman_id}"
TOPIC_DISPATCH_TASK_GONE = "{agent_name}/{service_id}/2/{workman_id}"
TOPIC_DISPATCH_TASK_DECISION = "{agent_name}/{service_id}/3"
TOPIC_DISPATCH_TASK_CANCEL = "{agent_name}/{service_id}/4"
TOPIC_DISPATCH_TASK_CHECK = "{agent_name}/{service_id}/5/{workman_id}"
TOPIC_DISPATCH_TASK_FORCE = "{agent_name}/{service_id}/6/{workman_id}"

TOPIC_DISPATCH_TASK_DECISION_FINISH = "{agent_name}/{service_id}/8"
