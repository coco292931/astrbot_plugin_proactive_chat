"""REVIEW 阶段默认功能：生成期间新消息检测与终止保护。

对应旧 check_and_chat 中「检查生成期间是否有新消息」与
「发送前再次确认终止状态」两段逻辑。
"""

from __future__ import annotations

from typing import Any

from astrbot.api import logger

from ..domain.domain_enums import HookPoint, StageResult
from ..pipeline.stage_protocol import BaseStage


class ReviewStage(BaseStage):
    """生成结果复核阶段。"""

    name = "review_stage"
    hook = HookPoint.REVIEW
    priority = 10

    def __init__(self, orchestrator: Any) -> None:
        self.orchestrator = orchestrator

    async def handle(self, ctx) -> StageResult:
        plugin = self.orchestrator.plugin
        session_id = ctx.session_id

        current_state = {
            "last_message_time": plugin.last_message_times.get(session_id, 0),
            "unanswered_count": plugin.session_data.get(session_id, {}).get(
                "unanswered_count", 0
            ),
        }
        start_state = ctx.start_state or {}

        has_new_message = current_state["last_message_time"] > start_state.get(
            "last_message_time", 0
        ) or current_state["unanswered_count"] < start_state.get("unanswered_count", 0)
        if has_new_message:
            logger.info(
                "[主动消息] 检测到用户在LLM生成期间发送了新消息，丢弃本次主动消息喵。"
            )
            ctx.abort("new_message_during_generation")
            return StageResult.STOP

        if getattr(plugin, "_terminating", False):
            logger.info("[主动消息] 插件正在终止，丢弃本次已生成的主动消息喵。")
            ctx.abort("terminating")
            return StageResult.STOP

        # 命中“不发送回复文本”时跳过发送与存档，仅安排下一次主动消息。
        response_text = str(ctx.extra.get("response_text", "") or "")
        if response_text:
            raw_config = ctx.config.raw if ctx.config is not None else {}
            if plugin.context_service.is_proactive_response_suppressed(
                response_text, raw_config
            ):
                logger.info(
                    "[主动消息] LLM 回复命中抑制发送文本，本次不发送主动消息，也不写入对话历史喵。"
                )
                ctx.abort("suppressed_response")
                return StageResult.RESCHEDULE

        return StageResult.CONTINUE


class ReviewFeature:
    """REVIEW 阶段功能。"""

    name = "review_feature"

    def __init__(self, orchestrator: Any) -> None:
        self.orchestrator = orchestrator

    def register(self, registry) -> None:
        registry.add_stage(
            hook=HookPoint.REVIEW, stage=ReviewStage(self.orchestrator), priority=10
        )
