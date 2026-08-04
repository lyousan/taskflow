"""Taskqx 的领域异常。"""


class TaskqxError(Exception):
    """所有 Taskqx 异常的基类。"""


class ValidationError(TaskqxError, ValueError):
    """调用参数或消息内容不符合公共契约。"""


class LeaseLostError(TaskqxError):
    """投递租约已失效，当前消费者不再能改变消息状态。"""


class UnsupportedCapabilityError(TaskqxError):
    """当前 backend 未提供调用方所请求的能力。"""


class SerializerUnavailableError(ValidationError):
    """历史消息所需的 serializer 未注册或不可用。"""


class PayloadDecodingError(TaskqxError):
    """类型化 payload 不兼容；Worker 会将原始 envelope 作为 poison message 写入 DLQ。"""


class BrokerClosedError(TaskqxError):
    """broker 或消费者已经关闭。"""


class RetryableError(TaskqxError):
    """提示高层 Worker 该失败可按 RetryPolicy 重试。"""


class RejectMessage(TaskqxError):
    """提示高层 Worker 直接拒绝当前消息并写入 DLQ。"""
