"""主动消息插件回归测试。

覆盖合并上游与新增功能后容易回退的行为：主动回复抑制、
REVIEW 阶段决策、主 Agent 工具循环接线、对话存档写入、
配置结构与注册表一致性。

脚本不依赖 AstrBot 运行时，缺失模块以最小桩件替代。

运行:
    python tests/test_regression.py
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import re
import sys
import types
import unittest
from datetime import timezone
from enum import Enum, auto
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
SESSION_ID = "default:FriendMessage:123456"


def _install_astrbot_stubs() -> None:
    """安装最小 astrbot 桩件，使插件模块可在无 AstrBot 环境下导入。"""
    if "astrbot" in sys.modules:
        return

    @dataclasses.dataclass
    class ProviderRequest:
        prompt: str | None = None
        session_id: str | None = ""
        image_urls: list = dataclasses.field(default_factory=list)
        audio_urls: list = dataclasses.field(default_factory=list)
        extra_user_content_parts: list = dataclasses.field(default_factory=list)
        func_tool: Any = None
        contexts: list = dataclasses.field(default_factory=list)
        system_prompt: str = ""
        conversation: Any = None
        model: str | None = None

    @dataclasses.dataclass
    class TextPart:
        text: str = ""

        def mark_as_temp(self) -> TextPart:
            return self

    @dataclasses.dataclass
    class UserMessageSegment:
        content: list = dataclasses.field(default_factory=list)

    @dataclasses.dataclass
    class AssistantMessageSegment:
        content: list = dataclasses.field(default_factory=list)

    class AgentState(Enum):
        IDLE = auto()
        RUNNING = auto()
        DONE = auto()
        ERROR = auto()

    class MessageType(Enum):
        GROUP_MESSAGE = auto()
        FRIEND_MESSAGE = auto()

    class PlatformStatus(Enum):
        RUNNING = auto()

    class MessageChain:
        def __init__(self, chain: list | None = None) -> None:
            self.chain = list(chain or [])

    @dataclasses.dataclass
    class Plain:
        text: str = ""

    @dataclasses.dataclass
    class Record:
        file: str = ""

    class MessageEventResult:
        pass

    class ResultContentType(Enum):
        LLM_RESULT = auto()

    class AsyncIOScheduler:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.running = False

        def start(self) -> None:
            self.running = True

        def shutdown(self, *args: Any, **kwargs: Any) -> None:
            self.running = False

    class _AsyncFileContext:
        async def __aenter__(self) -> _AsyncFileContext:
            return self

        async def __aexit__(self, *args: Any) -> bool:
            return False

        async def read(self) -> str:
            return ""

        async def write(self, data: str) -> None:
            return None

    def _aio_open(*args: Any, **kwargs: Any) -> _AsyncFileContext:
        return _AsyncFileContext()

    async def _aio_makedirs(*args: Any, **kwargs: Any) -> None:
        return None

    class AstrBotMessage:
        pass

    class Group:
        def __init__(self, group_id: str = "") -> None:
            self.group_id = group_id

    class MessageMember:
        def __init__(self, user_id: str = "", nickname: str | None = None) -> None:
            self.user_id = user_id
            self.nickname = nickname

    def _module(name: str, **attrs: Any) -> types.ModuleType:
        module = types.ModuleType(name)
        module.__dict__.update(attrs)
        return module

    modules = {
        "astrbot": _module("astrbot"),
        "astrbot.api": _module("astrbot.api", logger=_make_logger()),
        "astrbot.api.provider": _module(
            "astrbot.api.provider", ProviderRequest=ProviderRequest
        ),
        "astrbot.core": _module("astrbot.core"),
        "astrbot.core.agent": _module("astrbot.core.agent"),
        "astrbot.core.agent.message": _module(
            "astrbot.core.agent.message",
            TextPart=TextPart,
            UserMessageSegment=UserMessageSegment,
            AssistantMessageSegment=AssistantMessageSegment,
        ),
        "astrbot.core.agent.runners": _module("astrbot.core.agent.runners"),
        "astrbot.core.agent.runners.base": _module(
            "astrbot.core.agent.runners.base", AgentState=AgentState
        ),
        "astrbot.core.message": _module("astrbot.core.message"),
        "astrbot.core.message.message_event_result": _module(
            "astrbot.core.message.message_event_result",
            MessageChain=MessageChain,
            MessageEventResult=MessageEventResult,
            ResultContentType=ResultContentType,
        ),
        "astrbot.core.message.components": _module(
            "astrbot.core.message.components", Plain=Plain, Record=Record
        ),
        "astrbot.core.platform": _module("astrbot.core.platform"),
        "astrbot.core.platform.astrbot_message": _module(
            "astrbot.core.platform.astrbot_message",
            AstrBotMessage=AstrBotMessage,
            Group=Group,
            MessageMember=MessageMember,
        ),
        "astrbot.core.platform.message_type": _module(
            "astrbot.core.platform.message_type", MessageType=MessageType
        ),
        "astrbot.core.platform.platform": _module(
            "astrbot.core.platform.platform", PlatformStatus=PlatformStatus
        ),
        "apscheduler": _module("apscheduler"),
        "apscheduler.schedulers": _module("apscheduler.schedulers"),
        "apscheduler.schedulers.asyncio": _module(
            "apscheduler.schedulers.asyncio", AsyncIOScheduler=AsyncIOScheduler
        ),
        "aiofiles": _module("aiofiles", open=_aio_open),
        "aiofiles.os": _module("aiofiles.os", makedirs=_aio_makedirs),
    }
    sys.modules.update(modules)


def _make_logger() -> Any:
    import logging

    logging.basicConfig(level=logging.CRITICAL)
    return logging.getLogger("astrbot-plugin-test")


_install_astrbot_stubs()
sys.path.insert(0, str(PLUGIN_ROOT.parent))

from astrbot_plugin_proactive_chat.core.adapters.astrbot_conversation import (  # noqa: E402
    add_message_pair,
)
from astrbot_plugin_proactive_chat.core.adapters.astrbot_event import (  # noqa: E402
    build_pipeline_event_for_session,
)
from astrbot_plugin_proactive_chat.core.adapters.astrbot_main_agent import (  # noqa: E402
    _build_config,
    run_main_agent,
)
from astrbot_plugin_proactive_chat.core.adapters.astrbot_pipeline import (  # noqa: E402
    dispatch_proactive_to_pipeline,
)
from astrbot_plugin_proactive_chat.core.domain.domain_enums import (  # noqa: E402
    StageResult,
)
from astrbot_plugin_proactive_chat.core.domain.request_context import (  # noqa: E402
    ProactiveContext,
)
from astrbot_plugin_proactive_chat.core.domain.session_config import (  # noqa: E402
    SessionConfig,
)
from astrbot_plugin_proactive_chat.core.features.review_feature import (  # noqa: E402
    ReviewStage,
)
from astrbot_plugin_proactive_chat.core.services import (  # noqa: E402
    llm_service as llm_service_module,
)
from astrbot_plugin_proactive_chat.core.services.context_service import (  # noqa: E402
    ContextService,
)
from astrbot_plugin_proactive_chat.core.services.llm_service import LlmService  # noqa: E402
from astrbot_plugin_proactive_chat.core.store.override_store import (  # noqa: E402
    OverrideStore,
)

AGENT_STATE = sys.modules["astrbot.core.agent.runners.base"].AgentState


class SuppressionTest(unittest.TestCase):
    """主动回复抑制的解析与判定。"""

    def setUp(self) -> None:
        self.service = ContextService(None)

    def test_parse_supports_list_and_separators(self) -> None:
        self.assertEqual(
            self.service.parse_suppressed_response_texts(["不回复", " 无需回复 "]),
            {"不回复", "无需回复"},
        )
        self.assertEqual(
            self.service.parse_suppressed_response_texts(
                "不回复，无需回复\n不需要回复"
            ),
            {"不回复", "无需回复", "不需要回复"},
        )

    def test_section_missing_falls_back_to_defaults(self) -> None:
        self.assertTrue(
            self.service.is_proactive_response_suppressed("那我就不回复了", {})
        )
        self.assertFalse(
            self.service.is_proactive_response_suppressed("今天天气不错", {})
        )

    def test_custom_list_hit(self) -> None:
        config = {
            "response_suppression_settings": {
                "suppressed_response_texts": ["保持沉默"],
            }
        }
        self.assertTrue(
            self.service.is_proactive_response_suppressed("我决定保持沉默。", config)
        )
        self.assertFalse(
            self.service.is_proactive_response_suppressed("不回复", config)
        )

    def test_empty_list_disables_suppression(self) -> None:
        config = {"response_suppression_settings": {"suppressed_response_texts": []}}
        self.assertFalse(
            self.service.is_proactive_response_suppressed("不回复", config)
        )


class ReviewStageTest(unittest.TestCase):
    """REVIEW 阶段：抑制、新消息与终止保护。"""

    def _build_stage(self, *, terminating: bool = False) -> tuple[Any, Any]:
        plugin = SimpleNamespace(
            last_message_times={SESSION_ID: 0},
            session_data={SESSION_ID: {"unanswered_count": 0}},
            _terminating=terminating,
            context_service=ContextService(None),
        )
        return ReviewStage(SimpleNamespace(plugin=plugin)), plugin

    def _build_ctx(self, text: str, config: dict | None = None) -> ProactiveContext:
        ctx = ProactiveContext(
            session_id=SESSION_ID,
            config=SessionConfig(raw=config or {}),
        )
        ctx.start_state = {"last_message_time": 0, "unanswered_count": 0}
        if text:
            ctx.extra["response_text"] = text
        return ctx

    def test_suppressed_response_reschedules_without_sending(self) -> None:
        stage, _ = self._build_stage()
        ctx = self._build_ctx("好的，我这次就不回复了")

        result = asyncio.run(stage.handle(ctx))

        self.assertIs(result, StageResult.RESCHEDULE)
        self.assertEqual(ctx.abort_reason, "suppressed_response")

    def test_normal_response_continues(self) -> None:
        stage, _ = self._build_stage()
        ctx = self._build_ctx("你今天过得怎么样？")

        self.assertIs(asyncio.run(stage.handle(ctx)), StageResult.CONTINUE)

    def test_empty_response_continues(self) -> None:
        stage, _ = self._build_stage()
        ctx = self._build_ctx("")

        self.assertIs(asyncio.run(stage.handle(ctx)), StageResult.CONTINUE)

    def test_new_message_during_generation_stops(self) -> None:
        stage, plugin = self._build_stage()
        ctx = self._build_ctx("随便说点什么")
        plugin.last_message_times[SESSION_ID] = 10

        result = asyncio.run(stage.handle(ctx))

        self.assertIs(result, StageResult.STOP)
        self.assertEqual(ctx.abort_reason, "new_message_during_generation")

    def test_terminating_stops(self) -> None:
        stage, _ = self._build_stage(terminating=True)
        ctx = self._build_ctx("随便说点什么")

        result = asyncio.run(stage.handle(ctx))

        self.assertIs(result, StageResult.STOP)
        self.assertEqual(ctx.abort_reason, "terminating")


@dataclasses.dataclass
class _FakeConfig:
    """模拟低版本 MainAgentBuildConfig：字段少于当前版本。"""

    tool_call_timeout: int
    streaming_response: bool = True
    add_cron_tools: bool = True


class _FakeRunner:
    """记录调用参数的假 runner。"""

    def __init__(self, final: Any = None, state: Any = None) -> None:
        self.final = final
        self.state = state or AGENT_STATE.DONE
        self.max_step: int | None = None

    async def step_until_done(self, max_step: int):
        self.max_step = max_step
        if False:  # pragma: no cover - 仅为生成异步生成器
            yield None

    def get_final_llm_resp(self) -> Any:
        return self.final


class MainAgentAdapterTest(unittest.TestCase):
    """主 Agent 适配器：配置裁剪与循环驱动。"""

    def _plugin(self, timeout: int = 90) -> Any:
        return SimpleNamespace(
            context=SimpleNamespace(
                get_config=lambda umo: {
                    "agent_runner": {
                        "config": {
                            "misc": {"tool_schema_mode": "skills-like"},
                            "persona": {"safety_mode": False},
                        }
                    },
                    "provider_settings": {"computer_use_runtime": "local"},
                }
            ),
            timezone=None,
        )

    def test_config_filters_unsupported_fields(self) -> None:
        config = _build_config(
            _FakeConfig,
            self._plugin(),
            SESSION_ID,
            {"agent_settings": {"tool_call_timeout": 90}},
        )

        self.assertEqual(config.tool_call_timeout, 90)
        self.assertFalse(config.streaming_response)
        self.assertFalse(config.add_cron_tools)

    def test_config_uses_default_timeout(self) -> None:
        config = _build_config(_FakeConfig, self._plugin(), SESSION_ID, {})
        self.assertEqual(config.tool_call_timeout, 60)

    def _run(self, build_result: Any, session_config: dict) -> Any:
        async def fake_build(**kwargs: Any) -> Any:
            fake_build.kwargs = kwargs
            return build_result

        fake_build.kwargs = {}
        with patch(
            "astrbot_plugin_proactive_chat.core.adapters.astrbot_main_agent._load_entry",
            return_value=(_FakeConfig, fake_build),
        ):
            result = asyncio.run(
                run_main_agent(
                    self._plugin(),
                    "provider-obj",
                    SimpleNamespace(plugins_name="x", role=None),
                    "req-obj",
                    SESSION_ID,
                    session_config,
                )
            )
        return result, fake_build.kwargs

    def test_passes_provider_and_request_and_drives_loop(self) -> None:
        runner = _FakeRunner(final="final-response")
        result, kwargs = self._run(
            SimpleNamespace(agent_runner=runner, reset_coro=None),
            {"agent_settings": {"max_steps": 5}},
        )

        self.assertEqual(result, "final-response")
        self.assertEqual(kwargs["provider"], "provider-obj")
        self.assertEqual(kwargs["req"], "req-obj")
        self.assertIsNone(kwargs["event"].plugins_name)
        self.assertEqual(kwargs["event"].role, "member")
        self.assertEqual(runner.max_step, 5)

    def test_default_max_steps(self) -> None:
        runner = _FakeRunner(final="ok")
        self._run(SimpleNamespace(agent_runner=runner, reset_coro=None), {})

        self.assertEqual(runner.max_step, 8)

    def test_error_state_returns_none(self) -> None:
        runner = _FakeRunner(final="partial", state=AGENT_STATE.ERROR)
        result, _ = self._run(SimpleNamespace(agent_runner=runner, reset_coro=None), {})

        self.assertIsNone(result)

    def test_awaits_pending_reset(self) -> None:
        called: list[bool] = []

        async def reset() -> None:
            called.append(True)

        runner = _FakeRunner(final="ok")
        self._run(SimpleNamespace(agent_runner=runner, reset_coro=reset()), {})

        self.assertEqual(called, [True])

    def test_unavailable_version_returns_none(self) -> None:
        with patch(
            "astrbot_plugin_proactive_chat.core.adapters.astrbot_main_agent._load_entry",
            return_value=(None, None),
        ):
            result = asyncio.run(
                run_main_agent(None, None, SimpleNamespace(), None, SESSION_ID, {})
            )

        self.assertIsNone(result)


class LlmServiceRoutingTest(unittest.TestCase):
    """LlmService：主 Agent 与单次请求的路由及回退。"""

    def _service(self) -> LlmService:
        plugin = SimpleNamespace(
            timezone=timezone.utc,
            telemetry=None,
            context_service=ContextService(None),
        )
        return LlmService(plugin)

    def _generate(self, service: LlmService, session_config: dict) -> Any:
        return asyncio.run(
            service.generate_llm_response(
                SESSION_ID,
                session_config,
                [],
                "system",
                0,
                event=SimpleNamespace(),
            )
        )

    def _patches(self, agent_mock: AsyncMock, provider_mock: AsyncMock) -> list[Any]:
        return [
            patch.object(llm_service_module, "run_main_agent", new=agent_mock),
            patch.object(llm_service_module, "invoke_provider", new=provider_mock),
            patch.object(
                LlmService,
                "resolve_chat_provider",
                new=AsyncMock(return_value="provider"),
            ),
            patch.object(
                LlmService,
                "dispatch_llm_request_hooks",
                new=AsyncMock(return_value=False),
            ),
            patch.object(
                LlmService,
                "dispatch_llm_response_hooks",
                new=AsyncMock(return_value=False),
            ),
        ]

    def test_disabled_uses_single_request(self) -> None:
        response = SimpleNamespace(completion_text="你好呀", result_chain=None)
        agent_mock = AsyncMock()
        provider_mock = AsyncMock(return_value=response)

        patches = self._patches(agent_mock, provider_mock)
        for item in patches:
            item.start()
        try:
            result, _ = self._generate(
                self._service(),
                {"proactive_prompt": "", "agent_settings": {"enable": False}},
            )
        finally:
            for item in patches:
                item.stop()

        self.assertIs(result, response)
        self.assertEqual(agent_mock.await_count, 0)
        self.assertEqual(provider_mock.await_count, 1)

    def test_enabled_uses_main_agent(self) -> None:
        response = SimpleNamespace(
            completion_text="工具调用后的回复", result_chain=None
        )
        agent_mock = AsyncMock(return_value=response)
        provider_mock = AsyncMock()

        patches = self._patches(agent_mock, provider_mock)
        for item in patches:
            item.start()
        try:
            result, _ = self._generate(
                self._service(),
                {"proactive_prompt": "", "agent_settings": {"enable": True}},
            )
        finally:
            for item in patches:
                item.stop()

        self.assertIs(result, response)
        self.assertEqual(agent_mock.await_count, 1)
        self.assertEqual(provider_mock.await_count, 0)
        args = agent_mock.await_args.args
        self.assertEqual(args[1], "provider")
        self.assertEqual(args[4], SESSION_ID)

    def test_build_proactive_prompt_replaces_placeholders(self) -> None:
        service = self._service()
        text = service.build_proactive_prompt(
            {"proactive_prompt": "次数={{unanswered_count}} 时间={{current_time}}"},
            3,
        )
        self.assertIn("次数=3", text)
        self.assertNotIn("{{", text)

    def test_agent_failure_falls_back(self) -> None:
        response = SimpleNamespace(completion_text="回退成功", result_chain=None)
        agent_mock = AsyncMock(side_effect=RuntimeError("boom"))
        provider_mock = AsyncMock(return_value=response)

        patches = self._patches(agent_mock, provider_mock)
        for item in patches:
            item.start()
        try:
            result, _ = self._generate(
                self._service(),
                {"proactive_prompt": "", "agent_settings": {"enable": True}},
            )
        finally:
            for item in patches:
                item.stop()

        self.assertIs(result, response)
        self.assertEqual(agent_mock.await_count, 1)
        self.assertEqual(provider_mock.await_count, 1)


class NativePipelineTest(unittest.TestCase):
    """原生事件流水线：事件构造与投递。"""

    def _event(self) -> Any:
        return SimpleNamespace(
            message_str="",
            is_wake=False,
            is_at_or_wake_command=False,
            message_obj=SimpleNamespace(message=[], message_str=""),
        )

    def test_event_composes_group_prefixes(self) -> None:
        event = self._event()
        with patch(
            "astrbot_plugin_proactive_chat.core.adapters.astrbot_event."
            "build_proactive_event_for_session",
            return_value=event,
        ):
            built = build_pipeline_event_for_session(
                plugin=None,
                session_id="default:GroupMessage:42",
                text="文案",
                wake_prefix="/",
                provider_prefix="!",
            )

        self.assertEqual(built.message_str, "/!文案")
        self.assertEqual(built.message_obj.message_str, "/!文案")
        self.assertEqual(built.message_obj.message[0].text, "/!文案")
        self.assertTrue(built.is_wake)

    def test_event_skips_wake_prefix_in_private(self) -> None:
        event = self._event()
        with patch(
            "astrbot_plugin_proactive_chat.core.adapters.astrbot_event."
            "build_proactive_event_for_session",
            return_value=event,
        ):
            built = build_pipeline_event_for_session(
                plugin=None,
                session_id=SESSION_ID,
                text="文案",
                wake_prefix="/",
                provider_prefix="!",
            )

        self.assertEqual(built.message_str, "!文案")

    def test_dispatch_returns_false_without_queue(self) -> None:
        plugin = SimpleNamespace(context=SimpleNamespace())

        result = asyncio.run(dispatch_proactive_to_pipeline(plugin, SESSION_ID, "文案"))

        self.assertFalse(result)

    def test_dispatch_enqueues_event(self) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        plugin = SimpleNamespace(
            context=SimpleNamespace(
                get_event_queue=lambda: queue,
                get_config=lambda umo: {
                    "wake_prefix": ["/"],
                    "platform_settings": {"provider_wake_prefix": ""},
                },
            )
        )
        event = SimpleNamespace()
        with patch(
            "astrbot_plugin_proactive_chat.core.adapters.astrbot_pipeline."
            "build_pipeline_event_for_session",
            return_value=event,
        ):
            result = asyncio.run(
                dispatch_proactive_to_pipeline(plugin, SESSION_ID, "文案")
            )

        self.assertTrue(result)
        self.assertIs(queue.get_nowait(), event)


class HistoryWriteTest(unittest.TestCase):
    """存档仍然写入 user + assistant 一对。"""

    def test_writes_user_and_assistant_pair(self) -> None:
        manager = SimpleNamespace(add_message_pair=AsyncMock())

        asyncio.run(add_message_pair(manager, "conv-1", "主动提示词", "主动回复"))

        kwargs = manager.add_message_pair.await_args.kwargs
        self.assertEqual(kwargs["cid"], "conv-1")
        self.assertEqual(kwargs["user_message"].content[0].text, "主动提示词")
        self.assertEqual(kwargs["assistant_message"].content[0].text, "主动回复")


class ConfigAndRegistryTest(unittest.TestCase):
    """配置结构、注册表与遗留引用检查。"""

    def setUp(self) -> None:
        self.schema = json.loads((PLUGIN_ROOT / "_conf_schema.json").read_text("utf-8"))

    def test_agent_settings_present_in_both_sections(self) -> None:
        required = {"enable", "max_steps", "tool_call_timeout"}
        for section in ("friend_settings", "group_settings"):
            items = self.schema[section]["items"]
            self.assertIn("agent_settings", items)
            self.assertLessEqual(required, set(items["agent_settings"]["items"]))
            self.assertFalse(items["agent_settings"]["items"]["enable"]["default"])

    def test_native_pipeline_settings_present(self) -> None:
        for section in ("friend_settings", "group_settings"):
            items = self.schema[section]["items"]
            self.assertIn("native_pipeline_settings", items)
            self.assertFalse(
                items["native_pipeline_settings"]["items"]["enable"]["default"]
            )

    def test_suppression_settings_kept(self) -> None:
        for section in ("friend_settings", "group_settings"):
            self.assertIn(
                "response_suppression_settings", self.schema[section]["items"]
            )

    def test_override_allowlist_covers_new_keys(self) -> None:
        self.assertIn("agent_settings", OverrideStore.ALLOWED_ROOT_KEYS)
        self.assertIn("native_pipeline_settings", OverrideStore.ALLOWED_ROOT_KEYS)
        self.assertIn("response_suppression_settings", OverrideStore.ALLOWED_ROOT_KEYS)

    def test_no_legacy_module_imports(self) -> None:
        pattern = re.compile(
            r"^\s*(?:from|import)\s+\S*"
            r"(chat_flow|llm_adapter|message_events|message_sender|"
            r"task_scheduler|web_admin_server|session_parser)"
        )
        for path in [PLUGIN_ROOT / "main.py", *PLUGIN_ROOT.glob("core/**/*.py")]:
            for lineno, line in enumerate(path.read_text("utf-8").splitlines(), 1):
                if pattern.match(line):
                    self.fail(
                        f"{path.relative_to(PLUGIN_ROOT)}:{lineno} 仍引用旧模块: {line}"
                    )

    def test_no_conflict_markers(self) -> None:
        suffixes = {".py", ".json", ".yaml", ".yml"}
        for path in PLUGIN_ROOT.rglob("*"):
            if (
                not path.is_file()
                or path.suffix not in suffixes
                or ".git" in path.parts
            ):
                continue
            for lineno, line in enumerate(
                path.read_text("utf-8", errors="ignore").splitlines(), 1
            ):
                if (
                    line.startswith(("<<<<<<< ", ">>>>>>> "))
                    or line.strip() == "======="
                ):
                    self.fail(f"{path.relative_to(PLUGIN_ROOT)}:{lineno} 残留冲突标记")

    def test_metadata_version_matches_semver(self) -> None:
        text = (PLUGIN_ROOT / "metadata.yaml").read_text("utf-8")
        match = re.search(r"^version:\s*\"?([^\"\n]+)\"?", text, re.M)
        self.assertIsNotNone(match)
        self.assertRegex(match.group(1), r"^v?\d+\.\d+\.\d+")


if __name__ == "__main__":
    unittest.main(verbosity=2)
