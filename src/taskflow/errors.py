"""Taskflow 的领域异常。"""


class TaskflowError(Exception):
    """所有 Taskflow 异常的基类。"""


class ValidationError(TaskflowError, ValueError):
    """调用参数或消息内容不符合公共契约。"""


class LeaseLostError(TaskflowError):
    """投递租约已失效，当前消费者不再能改变消息状态。"""


class UnsupportedCapabilityError(TaskflowError):
    """当前 backend 未提供调用方所请求的能力。"""


class BrokerClosedError(TaskflowError):
    """broker 或消费者已经关闭。"""
