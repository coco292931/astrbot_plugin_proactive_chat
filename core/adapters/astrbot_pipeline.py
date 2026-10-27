"""AstrBot 原生事件流水线适配器。

把主动消息包装成伪用户事件投入 Context.get_event_queue()，由框架完整执行
唤醒检查、限流、LLM 调用、发送与历史存档，插件只负责触发时机。
"""

from __future__ import annotations

from typing import Any

from astrbot.api import logger

from .astrbot_event import build_pipeline_event_for_session


def _resolve_prefixes(plugin: Any, session_id: str) -> tuple[str, str]:
    """读取全局唤醒前缀与 provider 唤醒前缀。"""
    try:
        cfg = plugin.context.get_config(umo=session_id) or {}
    except Exception:
        cfg = {}

    wake_prefixes = cfg.get("wake_prefix") or []
    if isinstance(wake_prefixes, str):
        wake_prefix = wake_prefixes
    elif isinstance(wake_prefixes, list):
        wake_prefix = next((item for item in wake_prefixes if item), "")
    else:
        wake_prefix = ""

    platform_settings = cfg.get("platform_settings") or {}
    provider_prefix = platform_settings.get("provider_wake_prefix") or ""
    return wake_prefix, provider_prefix


async def dispatch_proactive_to_pipeline(
    plugin: Any, session_id: str, text: str
) -> bool:
    """把主动消息作为伪用户事件投入 AstrBot 原生流水线。

    Args:
        plugin: 插件实例。
        session_id: 会话 UMO。
        text: 主动消息正文，会被当作本轮用户消息。

    Returns:
        True 表示已成功入队；False 表示当前版本不支持或事件构造失败。
    """
    getter = getattr(plugin.context, "get_event_queue", None)
    if not callable(getter):
        logger.warning(
            "[主动消息] 当前 AstrBot 版本未提供事件队列，无法使用原生流水线喵。"
        )
        return False

    wake_prefix, provider_prefix = _resolve_prefixes(plugin, session_id)
    event = build_pipeline_event_for_session(
        plugin=plugin,
        session_id=session_id,
        text=text,
        wake_prefix=wake_prefix,
        provider_prefix=provider_prefix,
    )
    if event is None:
        logger.warning("[主动消息] 原生流水线事件构造失败喵。")
        return False

    try:
        await getter().put(event)
    except Exception as e:
        logger.error(f"[主动消息] 投递事件到原生流水线失败喵: {e}")
        return False

    logger.info("[主动消息] 已把本次主动消息投入 AstrBot 原生事件流水线喵。")
    return True


__all__ = ["dispatch_proactive_to_pipeline"]
