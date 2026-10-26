"""AstrBot 主 Agent 适配器：复用官方工具循环生成主动消息。

工具装配、人格/技能注入与多轮调用循环全部交给 AstrBot 的
build_main_agent + ToolLoopAgentRunner，插件只负责传入请求并取回最终结果。
"""

from __future__ import annotations

import dataclasses
from typing import Any

from astrbot.api import logger


def _load_entry() -> tuple[Any, Any]:
    """导入主 Agent 构建入口，当前版本不支持时返回 (None, None)。"""
    try:
        from astrbot.core.astr_main_agent import (  # noqa: PLC0415
            MainAgentBuildConfig,
            build_main_agent,
        )
    except ImportError:  # pragma: no cover - 取决于 AstrBot 版本
        return None, None
    return MainAgentBuildConfig, build_main_agent


def _build_config(
    config_cls: Any, plugin: Any, session_id: str, session_config: dict
) -> Any:
    """构造主 Agent 配置，并按当前版本支持的字段裁剪。

    Args:
        config_cls: 当前 AstrBot 版本的 MainAgentBuildConfig。
        plugin: 插件实例。
        session_id: 会话 UMO。
        session_config: 当前会话的最终配置。

    Returns:
        MainAgentBuildConfig 实例。
    """
    global_cfg = plugin.context.get_config(umo=session_id) or {}
    agent_cfg = (global_cfg.get("agent_runner") or {}).get("config") or {}
    misc = agent_cfg.get("misc") or {}
    persona = agent_cfg.get("persona") or {}
    provider_settings = global_cfg.get("provider_settings") or {}
    agent_conf = session_config.get("agent_settings") or {}

    wanted = {
        "tool_call_timeout": int(agent_conf.get("tool_call_timeout", 60)),
        "tool_schema_mode": misc.get("tool_schema_mode", "full"),
        # 主动消息没有用户消息前缀，显式留空避免被唤醒前缀拦截。
        "provider_wake_prefix": "",
        "streaming_response": False,
        "llm_safety_mode": persona.get("safety_mode", True),
        "safety_mode_strategy": persona.get("safety_mode_strategy", "system_prompt"),
        "computer_use_runtime": provider_settings.get("computer_use_runtime", "none"),
        "sandbox_cfg": provider_settings.get("sandbox") or {},
        # 主动消息不需要定时任务管理工具。
        "add_cron_tools": False,
        "provider_settings": provider_settings,
        "fallback_provider_ids": misc.get("fallback_provider_ids") or [],
        "timezone": str(plugin.timezone) if getattr(plugin, "timezone", None) else None,
    }
    supported = {field.name for field in dataclasses.fields(config_cls)}
    return config_cls(
        **{key: value for key, value in wanted.items() if key in supported}
    )


async def run_main_agent(
    plugin: Any,
    provider: Any,
    event: Any,
    req: Any,
    session_id: str,
    session_config: dict,
) -> Any:
    """执行一次主 Agent 工具循环，返回最终 LLMResponse。

    Args:
        plugin: 插件实例。
        provider: 已解析的聊天 Provider，避免主 Agent 重新选择。
        event: 贯穿本次主动消息的伪事件。
        req: 已构建好的 ProviderRequest。
        session_id: 会话 UMO。
        session_config: 当前会话的最终配置。

    Returns:
        最终 LLMResponse；当前版本不可用或执行失败时返回 None。
    """
    config_cls, build_main_agent = _load_entry()
    if config_cls is None or build_main_agent is None or event is None:
        return None

    config = _build_config(config_cls, plugin, session_id, session_config)

    # 伪事件沿用主链路的默认插件过滤与角色。
    event.plugins_name = None
    if not getattr(event, "role", None):
        event.role = "member"

    result = await build_main_agent(
        event=event,
        plugin_context=plugin.context,
        config=config,
        provider=provider,
        req=req,
    )
    if not result:
        return None

    # apply_reset 默认 True 时 reset_coro 为 None，否则由调用方负责初始化。
    if result.reset_coro is not None:
        await result.reset_coro

    runner = result.agent_runner
    max_steps = int((session_config.get("agent_settings") or {}).get("max_steps", 8))
    async for _ in runner.step_until_done(max_steps):
        pass

    from astrbot.core.agent.runners.base import AgentState  # noqa: PLC0415

    # 达到步数上限时 runner 进入 ERROR 状态且不抛异常，必须显式识别。
    if runner.state == AgentState.ERROR:
        logger.warning("[主动消息] 主 Agent 工具循环以 ERROR 状态结束喵。")
        return None

    return runner.get_final_llm_resp()
