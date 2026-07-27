"""类型化 payload 的严格编码与解码边界。"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import MISSING, asdict, dataclass, fields, is_dataclass
from types import UnionType
from typing import Any, TypeVar, Union, get_args, get_origin, get_type_hints

from typing_extensions import NotRequired, Required, is_typeddict

from .errors import PayloadDecodingError, ValidationError

PayloadT = TypeVar("PayloadT")
PAYLOAD_UNSET = object()


@dataclass(frozen=True, slots=True)
class PayloadSchema:
    """独立于 serializer 的业务 payload schema 身份。"""

    name: str
    version: str = "1"


def schema_for(payload_type: type[Any]) -> PayloadSchema:
    """从 dataclass、TypedDict 或 Pydantic model 导出稳定 schema 身份。"""

    name = getattr(payload_type, "__taskflow_schema_name__", None)
    version = getattr(payload_type, "__taskflow_schema_version__", "1")
    if not isinstance(name, str) or not name:
        name = f"{payload_type.__module__}.{payload_type.__qualname__}"
    if not isinstance(version, str) or not version:
        raise ValidationError("payload schema version 必须为非空字符串")
    return PayloadSchema(name, version)


def normalize_payload(payload: Any, *, payload_type: type[Any] | None = None) -> tuple[Any, PayloadSchema | None]:
    """把受支持对象转换为 JSON payload，同时保留 schema 身份。"""

    if is_dataclass(payload) and not isinstance(payload, type):
        raw, inferred = asdict(payload), schema_for(type(payload))
    else:
        model_dump = getattr(payload, "model_dump", None)
        if callable(model_dump) and isinstance(payload.__class__, type):
            try:
                raw, inferred = model_dump(mode="json"), schema_for(type(payload))
            except (TypeError, ValueError) as exc:
                raise ValidationError("Pydantic payload 无法转换为 JSON 数据") from exc
        else:
            raw, inferred = payload, None
    if payload_type is None:
        return raw, inferred
    schema = schema_for(payload_type)
    try:
        decoded = _decode_value(raw, payload_type)
    except PayloadDecodingError as exc:
        # Producer input is an API-parameter failure.  PayloadDecodingError is
        # reserved for a persisted envelope that a worker must poison-route.
        raise ValidationError("payload 不符合 payload_type") from exc
    if is_dataclass(decoded) and not isinstance(decoded, type):
        return asdict(decoded), schema
    dump = getattr(decoded, "model_dump", None)
    if callable(dump):
        return dump(mode="json"), schema
    return decoded, schema


def decode_payload(payload: Any, payload_type: type[PayloadT], *,
                   schema_name: str | None, schema_version: str | None) -> PayloadT:
    """严格按 schema 解码，绝不以隐式类型转换掩盖损坏数据。"""

    expected = schema_for(payload_type)
    if schema_name != expected.name or schema_version != expected.version:
        raise PayloadDecodingError(
            f"payload schema 不兼容：收到 {schema_name!r} v{schema_version!r}，"
            f"期望 {expected.name!r} v{expected.version!r}")
    return _decode_value(payload, payload_type)


def reconstruct_payload(*, existing_payload: Any, existing_schema_name: str | None,
                        existing_schema_version: str | None, payload: Any = PAYLOAD_UNSET,
                        payload_type: type[Any] | None = None) -> tuple[Any, str | None, str | None]:
    """Build replay payload fields with the same normalization rules as ``submit``.

    Unchanged payloads retain their recorded schema.  An untyped raw override clears
    the old schema rather than attaching stale type metadata to new bytes.
    """

    if payload is PAYLOAD_UNSET and payload_type is None:
        return existing_payload, existing_schema_name, existing_schema_version
    encoded, schema = normalize_payload(
        existing_payload if payload is PAYLOAD_UNSET else payload,
        payload_type=payload_type,
    )
    return encoded, schema.name if schema else None, schema.version if schema else None


def _decode_value(value: Any, annotation: Any) -> Any:
    if annotation is Any:
        return value
    origin = get_origin(annotation)
    if origin in (Required, NotRequired):
        (inner_type,) = get_args(annotation)
        return _decode_value(value, inner_type)
    if origin in (Union, UnionType):
        for member in get_args(annotation):
            try:
                return _decode_value(value, member)
            except PayloadDecodingError:
                pass
        raise PayloadDecodingError("payload 不匹配任何联合类型成员")
    if origin is list:
        if not isinstance(value, list):
            raise PayloadDecodingError("payload 字段必须是 list")
        (item_type,) = get_args(annotation) or (Any,)
        return [_decode_value(item, item_type) for item in value]
    if origin is dict:
        if not isinstance(value, dict):
            raise PayloadDecodingError("payload 字段必须是 dict")
        key_type, item_type = get_args(annotation) or (Any, Any)
        return {_decode_value(key, key_type): _decode_value(item, item_type) for key, item in value.items()}
    if is_dataclass(annotation) and isinstance(annotation, type):
        if not isinstance(value, Mapping):
            raise PayloadDecodingError("dataclass payload 必须是对象")
        hints, declared = get_type_hints(annotation), {field.name for field in fields(annotation)}
        unexpected = set(value) - declared
        if unexpected:
            raise PayloadDecodingError(f"payload 包含未知字段：{sorted(unexpected)!r}")
        decoded: dict[str, Any] = {}
        for field in fields(annotation):
            if field.name not in value:
                if field.default is not MISSING or field.default_factory is not MISSING:
                    continue
                raise PayloadDecodingError(f"payload 缺少字段：{field.name}")
            decoded[field.name] = _decode_value(value[field.name], hints.get(field.name, Any))
        return annotation(**decoded)
    if is_typeddict(annotation):
        if not isinstance(value, dict):
            raise PayloadDecodingError("TypedDict payload 必须是对象")
        hints = get_type_hints(annotation)
        required = set(getattr(annotation, "__required_keys__", ()))
        # Python 3.10 may report incorrect __required_keys__ when future
        # annotations and typing_extensions.Required are combined.  Derive
        # explicit wrappers from resolved hints as the authoritative override.
        for key, hint in hints.items():
            wrapper = get_origin(hint)
            if wrapper is Required:
                required.add(key)
            elif wrapper is NotRequired:
                required.discard(key)
        if not required and getattr(annotation, "__total__", True):
            required = {key for key, hint in hints.items()
                        if get_origin(hint) is not NotRequired}
        unexpected, missing = set(value) - set(hints), required - set(value)
        if unexpected or missing:
            raise PayloadDecodingError(f"TypedDict 字段不匹配：缺少 {sorted(missing)!r}，未知 {sorted(unexpected)!r}")
        return {key: _decode_value(item, hints[key]) for key, item in value.items()}
    model_validate = getattr(annotation, "model_validate", None)
    if callable(model_validate):
        if not isinstance(value, Mapping):
            raise PayloadDecodingError("Pydantic payload 必须是对象")
        try:
            # A payload has crossed a JSON serializer boundary.  Pydantic v2's
            # JSON validator preserves strict numeric checks while correctly
            # restoring JSON-supported values such as datetime.
            model_validate_json = getattr(annotation, "model_validate_json", None)
            if callable(model_validate_json):
                return model_validate_json(json.dumps(value), strict=True)
            return model_validate(value, strict=True)
        except (TypeError, ValueError) as exc:
            raise PayloadDecodingError("Pydantic payload 校验失败") from exc
    if annotation is type(None):
        if value is not None:
            raise PayloadDecodingError("payload 字段必须为 null")
        return None
    if isinstance(annotation, type) and type(value) is annotation:
        return value
    raise PayloadDecodingError(f"payload 类型错误：期望 {annotation!r}，收到 {type(value)!r}")
