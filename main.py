"""小蓝鲸上下文哨兵：只读监测 AstrBot data_v4.db 中指定会话的 token_usage。

- 数据库路径通过 get_astrbot_data_path() 获取，不写死 /root/AstrBot。
- SQLite 以 mode=ro 打开，并启用 query_only；不创建连接以外的写入操作。
- 必须识别并验证更新时间列；数据过期/时间戳异常时绝不触发阈值提醒。
- 状态只写入本插件自己的 data/plugin_data 目录，不触碰 AstrBot 主数据库。
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

PLUGIN_NAME = "astrbot_plugin_context_guard"
VERSION = "0.2.0"

# 普通用户必须在插件配置中填写自己的数据库 user_id；不包含作者的个人会话标识。
# 常见更新时间字段名。若实际字段名不在列表中，可在插件配置里明确指定。
TIMESTAMP_CANDIDATES = (
    "updated_at", "update_at", "updated", "update_time", "updatetime",
    "last_updated", "last_update", "modified_at", "modified_time",
    "last_modified", "update_ts", "updated_ts", "updated_timestamp",
    "timestamp", "mtime", "end_time",
)


def _ident(value: str) -> str:
    """安全引用由 SQLite 元数据读出的标识符。"""
    return '"' + value.replace('"', '""') + '"'


def _number(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _parse_timestamp(value: Any) -> float | None:
    """支持 Unix 秒/毫秒/微秒/纳秒及常见 ISO 时间字符串。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = float(value)
        if n > 1e17:
            n /= 1e9
        elif n > 1e14:
            n /= 1e6
        elif n > 1e11:
            n /= 1e3
        return n if n > 0 else None
    text = str(value).strip()
    if not text:
        return None
    try:
        n = float(text)
        return _parse_timestamp(n)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # SQLite 常见的无时区时间按本机时区解释。
            return dt.timestamp()
        return dt.astimezone(timezone.utc).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


class ContextGuard(Star):
    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        super().__init__(context)
        self.config = config or {}
        self.data_path = Path(get_astrbot_data_path())
        self.db_path = self.data_path / "data_v4.db"
        self.plugin_data_dir = self.data_path / "plugin_data" / PLUGIN_NAME
        self.state_path = self.plugin_data_dir / "state.json"
        self._state: dict[str, Any] = self._load_state()
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._last_diag: dict[str, Any] = {}
        self._error_log_at = 0.0

    # ------------------------------ lifecycle ------------------------------
    async def initialize(self) -> None:
        self._stopping = False
        self._task = asyncio.create_task(self._monitor_loop(), name="context-guard-monitor")
        logger.info("上下文哨兵 v%s 已启动；数据库只读路径：%s", VERSION, self.db_path)

    async def terminate(self) -> None:
        self._stopping = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("上下文哨兵停止时出现异常")
        self._task = None
        self._save_state()
        logger.info("上下文哨兵已停止")

    # ------------------------------ config/state ----------------------------
    def _cfg(self, key: str, default: Any) -> Any:
        return self.config.get(key, default) if isinstance(self.config, dict) else default

    def _settings(self) -> dict[str, Any]:
        def percent(key: str, default: float, upper: float) -> float:
            try:
                value = float(self._cfg(key, default))
            except (TypeError, ValueError, OverflowError):
                value = default
            return min(upper, max(1.0, value))

        return {
            "enabled": bool(self._cfg("enabled", True)),
            "context_window": max(1, _number(self._cfg("context_window_tokens", 1_000_000), 1_000_000)),
            "threshold_percent": percent("threshold_percent", 80.0, 99.0),
            "reset_percent": percent("reset_percent", 75.0, 98.0),
            "poll_seconds": min(3600, max(15, _number(self._cfg("poll_interval_seconds", 60), 60))),
            "freshness_enabled": bool(self._cfg("freshness_enabled", False)),
            "freshness_seconds": min(86400, max(30, _number(self._cfg("freshness_seconds", 600), 600))),
            "table_name": str(self._cfg("table_name", "") or "").strip(),
            "updated_at_column": str(self._cfg("updated_at_column", "") or "").strip(),
            "friend_user_id": str(self._cfg("friend_user_id", "") or "").strip(),
            "group_user_id": str(self._cfg("group_user_id", "") or "").strip(),
        }

    def _load_state(self) -> dict[str, Any]:
        default = {
            "notify_friend_umo": "",
            "notify_group_umo": "",
            "sessions": {
                "friend": {"alerted": False, "delivered": {}, "last_value": None, "last_fresh_at": None},
                "group": {"alerted": False, "delivered": {}, "last_value": None, "last_fresh_at": None},
            },
            "last_stale_notice": {},
        }
        try:
            if self.state_path.exists():
                loaded = json.loads(self.state_path.read_text("utf-8"))
                if isinstance(loaded, dict):
                    default.update(loaded)
        except Exception as exc:
            logger.warning("读取插件状态失败，将使用初始状态：%s", exc)
        default.setdefault("sessions", {})
        for key in ("friend", "group"):
            default["sessions"].setdefault(key, {"alerted": False, "delivered": {}})
        return default

    def _save_state(self) -> None:
        try:
            self.plugin_data_dir.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), "utf-8")
            tmp.replace(self.state_path)
        except Exception as exc:
            logger.error("保存插件自身状态失败：%s", exc)

    # ------------------------------ database --------------------------------
    def _open_readonly(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise FileNotFoundError(f"数据库不存在：{self.db_path}")
        # quote path 中的空格、#、? 等 URI 特殊字符；不使用 immutable，避免忽略 WAL 最新页。
        uri = "file:" + quote(str(self.db_path), safe="/:") + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=1.5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        conn.execute("PRAGMA busy_timeout = 1500")
        return conn

    def _discover_table(self, conn: sqlite3.Connection, settings: dict[str, Any]) -> tuple[str, list[str]]:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        candidates: list[tuple[str, list[str]]] = []
        for row in rows:
            table = str(row[0])
            if settings["table_name"] and table != settings["table_name"]:
                continue
            cols = [str(r[1]) for r in conn.execute(f"PRAGMA table_info({_ident(table)})").fetchall()]
            lower = {c.lower() for c in cols}
            if "user_id" in lower and "token_usage" in lower:
                candidates.append((table, cols))
        if not candidates:
            raise RuntimeError("没有找到同时包含 user_id 和 token_usage 的表；可在 table_name 配置中指定表名")

        # 优先选择含有用户在配置中填写的 user_id 的表。
        wanted = tuple(x for x in (settings["friend_user_id"], settings["group_user_id"]) if x)
        for table, cols in candidates:
            if not wanted:
                continue
            user_col = next(c for c in cols if c.lower() == "user_id")
            marks = ",".join("?" for _ in wanted)
            found = conn.execute(
                f"SELECT COUNT(*) FROM {_ident(table)} WHERE {_ident(user_col)} IN ({marks})", wanted
            ).fetchone()[0]
            if found:
                return table, cols
        if len(candidates) == 1:
            return candidates[0]
        if not wanted:
            raise RuntimeError("发现多个包含 user_id/token_usage 的表。请先在插件配置中填写 table_name，或稍后指定正确的数据表。")
        raise RuntimeError("多个表都含有目标列，但没有一个表匹配已配置的会话 user_id；请检查 friend_user_id、group_user_id 或 table_name")

    def _read_snapshot(self) -> dict[str, Any]:
        settings = self._settings()
        conn = self._open_readonly()
        try:
            table, cols = self._discover_table(conn, settings)
            user_col = next(c for c in cols if c.lower() == "user_id")
            token_col = next(c for c in cols if c.lower() == "token_usage")
            if settings["updated_at_column"]:
                stamp_col = next((c for c in cols if c.lower() == settings["updated_at_column"].lower()), None)
                if stamp_col is None:
                    raise RuntimeError(f"配置的更新时间列 {settings['updated_at_column']} 不存在；当前列：{', '.join(cols)}")
            else:
                by_lower = {c.lower(): c for c in cols}
                stamp_col = next((by_lower[n] for n in TIMESTAMP_CANDIDATES if n in by_lower), None)
            if stamp_col is None and settings["freshness_enabled"]:
                raise RuntimeError(
                    "已开启更新时间过滤，但未识别更新时间列。请在插件配置 updated_at_column 中填入真实字段；"
                    f"当前列：{', '.join(cols)}。若无法确认更新时间与 token_usage 同步，请关闭 freshness_enabled。"
                )

            configured_ids = {
                "friend": settings["friend_user_id"],
                "group": settings["group_user_id"],
            }
            wanted = tuple(dict.fromkeys(x for x in configured_ids.values() if x))
            if wanted:
                marks = ",".join("?" for _ in wanted)
                timestamp_expr = _ident(stamp_col) if stamp_col else "NULL"
                result_rows = conn.execute(
                    f"SELECT {_ident(user_col)} AS user_id, {_ident(token_col)} AS token_usage, "
                    f"{timestamp_expr} AS updated_at FROM {_ident(table)} "
                    f"WHERE {_ident(user_col)} IN ({marks})",
                    wanted,
                ).fetchall()
            else:
                result_rows = []
            by_id = {str(r["user_id"]): dict(r) for r in result_rows}
            now = time.time()
            sessions: dict[str, Any] = {}
            for label, user_id in configured_ids.items():
                if not user_id:
                    sessions[label] = {"found": False, "configured": False, "user_id": ""}
                    continue
                row = by_id.get(user_id)
                if not row:
                    sessions[label] = {"found": False, "configured": True, "user_id": user_id}
                    continue
                try:
                    usage = int(row["token_usage"])
                except (TypeError, ValueError, OverflowError):
                    usage = None
                updated_epoch = _parse_timestamp(row.get("updated_at"))
                age = None if updated_epoch is None else now - updated_epoch
                # updated_at 未必与 token_usage 同步刷新；只有用户确认该字段可靠后才启用新鲜度过滤。
                fresh = (not settings["freshness_enabled"]) or (
                    age is not None and -300 <= age <= settings["freshness_seconds"]
                )
                sessions[label] = {
                    "found": True,
                    "user_id": user_id,
                    "token_usage": usage,
                    "updated_at_raw": str(row.get("updated_at")),
                    "age_seconds": age,
                    "fresh": fresh,
                }
            return {
                "ok": True, "table": table, "columns": cols, "updated_at_column": stamp_col,
                "sessions": sessions, "read_at": now,
                "freshness_enabled": settings["freshness_enabled"],
            }
        finally:
            conn.close()

    # ------------------------------ monitoring -----------------------------
    async def _monitor_loop(self) -> None:
        # 首次立即检查，之后按配置间隔轮询。
        while not self._stopping:
            try:
                if self._settings()["enabled"]:
                    await self._check_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                now = time.time()
                if now - self._error_log_at >= 300:
                    logger.exception("上下文哨兵本轮检查失败：%s", exc)
                    self._error_log_at = now
                self._last_diag = {"ok": False, "error": str(exc), "read_at": now}
            await asyncio.sleep(self._settings()["poll_seconds"])

    async def _check_once(self) -> None:
        snapshot = await asyncio.to_thread(self._read_snapshot)
        self._last_diag = snapshot
        if not snapshot.get("ok"):
            return
        settings = self._settings()
        threshold = int(settings["context_window"] * settings["threshold_percent"] / 100)
        rearm = int(settings["context_window"] * settings["reset_percent"] / 100)
        for label, item in snapshot["sessions"].items():
            state = self._state["sessions"][label]
            if not item.get("found"):
                continue
            usage = item.get("token_usage")
            if usage is None:
                continue
            state["last_value"] = usage
            if not item.get("fresh"):
                # 陈旧数据只记录，不用于阈值判断。
                continue
            state["last_fresh_at"] = item.get("updated_at_raw")
            if usage <= rearm:
                state["alerted"] = False
                state["delivered"] = {}
                continue
            if usage >= threshold and not state.get("alerted"):
                await self._send_alert(label, usage, threshold, item)
        self._save_state()

    async def _send_alert(self, label: str, usage: int, threshold: int, row: dict[str, Any]) -> None:
        state = self._state["sessions"][label]
        name = "私聊" if label == "friend" else "群聊"
        age = row.get("age_seconds")
        age_text = f"更新时间距今 {max(0, int(age))} 秒" if age is not None else "更新时间未知"
        text = (
            f"🐋 小蓝鲸记忆提醒：{name}上下文已达到预警线。\n"
            f"当前 token_usage：{usage:,}\n"
            f"预警阈值：{threshold:,}\n"
            f"{age_text}。\n"
            "请安排日记和笔记整理；本插件只负责监测，不会修改日记、笔记或聊天原文。"
        )
        targets = [
            ("friend", str(self._state.get("notify_friend_umo", "") or "").strip()),
            ("group", str(self._state.get("notify_group_umo", "") or "").strip()),
        ]
        any_target = False
        complete = True
        for target_name, umo in targets:
            if not umo:
                complete = False
                continue
            any_target = True
            if state.setdefault("delivered", {}).get(target_name):
                continue
            try:
                sent = await self.context.send_message(umo, MessageChain([Plain(text)]))
                if sent:
                    state["delivered"][target_name] = True
                    self._save_state()
                else:
                    complete = False
                    logger.warning("阈值提醒未投递：%s 目标会话无可用平台，UMO=%s", target_name, umo)
            except Exception as exc:
                complete = False
                logger.warning("阈值提醒发送到 %s 失败：%s", target_name, exc)
        if any_target and complete:
            state["alerted"] = True
            logger.warning("%s 上下文达到阈值：%s / %s，已通知绑定会话", name, usage, threshold)
        elif not any_target:
            logger.warning("%s 上下文达到阈值，但尚未绑定通知会话。请在私聊和群聊分别发送 /记忆监控 绑定私聊 或 /记忆监控 绑定群聊", name)

    def _list_session_candidates(self, limit: int = 30) -> dict[str, Any]:
        settings = self._settings()
        conn = self._open_readonly()
        try:
            table, cols = self._discover_table(conn, settings)
            user_col = next(c for c in cols if c.lower() == "user_id")
            token_col = next(c for c in cols if c.lower() == "token_usage")
            by_lower = {c.lower(): c for c in cols}
            stamp_col = (
                next((by_lower[n] for n in TIMESTAMP_CANDIDATES if n in by_lower), None)
                if not settings["updated_at_column"]
                else by_lower.get(settings["updated_at_column"].lower())
            )
            select = f"{_ident(user_col)} AS user_id, {_ident(token_col)} AS token_usage"
            if stamp_col:
                select += f", {_ident(stamp_col)} AS updated_at"
                order = f"ORDER BY {_ident(stamp_col)} DESC"
            else:
                order = f"ORDER BY {_ident(user_col)}"
            rows = conn.execute(
                f"SELECT {select} FROM {_ident(table)} {order} LIMIT ?", (max(1, min(100, limit)),)
            ).fetchall()
            return {"table": table, "columns": cols, "rows": [dict(r) for r in rows], "stamp_col": stamp_col}
        finally:
            conn.close()

    # -------------------------------- commands ------------------------------
    @filter.command("记忆监控", alias={"ctxguard"})
    async def command(self, event: AstrMessageEvent):
        raw = (getattr(event, "message_str", "") or "").strip().lstrip("/／").strip()
        if raw.startswith("记忆监控"):
            arg = raw[len("记忆监控"):].strip()
        elif raw.startswith("ctxguard"):
            arg = raw[len("ctxguard"):].strip()
        else:
            arg = ""
        umo = str(getattr(event, "unified_msg_origin", "") or "")

        if arg in ("绑定私聊", "绑定群聊"):
            if not getattr(event, "is_admin", lambda: False)():
                yield event.plain_result("绑定通知目标需要管理员权限。")
                return
            is_friend = "FriendMessage" in umo
            is_group = "GroupMessage" in umo
            if arg == "绑定私聊" and is_friend:
                self._state["notify_friend_umo"] = umo
                self._save_state()
                yield event.plain_result("已绑定当前私聊为预警通知目标。")
                return
            if arg == "绑定群聊" and is_group:
                self._state["notify_group_umo"] = umo
                self._save_state()
                yield event.plain_result("已绑定当前群聊为预警通知目标。")
                return
            yield event.plain_result("命令所在会话类型不匹配：绑定私聊请在私聊执行，绑定群聊请在群里执行。")
            return

        if arg in ("会话列表", "会话", "sessions"):
            if not getattr(event, "is_admin", lambda: False)():
                yield event.plain_result("查看数据库会话列表需要管理员权限。")
                return
            try:
                result = await asyncio.to_thread(self._list_session_candidates, 30)
            except Exception as exc:
                yield event.plain_result(f"读取会话列表失败：{exc}")
                return
            lines = [
                f"🐋 数据库会话候选（表：{result['table']}）",
                "请从下方找到要监控的私聊/群聊 user_id，再填入插件配置 friend_user_id / group_user_id。",
                "注意：列表可能包含其他会话标识，只在可信的管理员私聊中查看。",
            ]
            for row in result["rows"]:
                value = row.get("token_usage")
                stamp = row.get("updated_at", "未提供")
                uid = str(row.get("user_id", ""))
                lines.append(f"user_id：{uid}｜token_usage：{value}｜更新时间：{stamp}")
            yield event.plain_result("\n".join(lines))
            return

        if arg in ("检查", "check", "状态", "status", ""):
            try:
                snap = await asyncio.to_thread(self._read_snapshot)
                self._last_diag = snap
            except Exception as exc:
                yield event.plain_result(f"上下文哨兵检查失败：{exc}")
                return
            settings = self._settings()
            threshold = int(settings["context_window"] * settings["threshold_percent"] / 100)
            lines = [
                f"🐋 上下文哨兵 v{VERSION}",
                f"数据库：{self.db_path}",
                f"只读模式：是（SQLite mode=ro + query_only）",
                f"表：{snap['table']}｜更新时间列：{snap['updated_at_column']}",
                f"预估窗口：{settings['context_window']:,}｜预警线：{settings['threshold_percent']}% = {threshold:,}",
                f"数据新鲜度过滤：{'已开启（最近 ' + str(settings['freshness_seconds']) + ' 秒）' if settings['freshness_enabled'] else '关闭（不使用更新时间拦截）'}",
                f"数据库私聊 user_id：{'已配置' if settings['friend_user_id'] else '未配置'}｜群聊 user_id：{'已配置' if settings['group_user_id'] else '未配置'}",
                f"通知私聊：{'已绑定' if self._state.get('notify_friend_umo') else '未绑定'}｜通知群聊：{'已绑定' if self._state.get('notify_group_umo') else '未绑定'}",
            ]
            for label, item in snap["sessions"].items():
                name = "私聊" if label == "friend" else "群聊"
                if not item.get("found"):
                    if not item.get("configured"):
                        lines.append(f"{name}：尚未配置数据库 user_id")
                    else:
                        lines.append(f"{name}：未找到已配置的 user_id 行")
                else:
                    age = item.get("age_seconds")
                    age_desc = "未知" if age is None else f"{max(0, int(age))} 秒前"
                    if settings["freshness_enabled"]:
                        freshness = "新鲜" if item.get("fresh") else "过期/时间异常（不触发）"
                    else:
                        freshness = "未启用更新时间过滤"
                    lines.append(f"{name}：{item.get('token_usage')} tokens｜更新于 {age_desc}｜{freshness}")
            yield event.plain_result("\n".join(lines))
            return

        if arg in ("测试", "test"):
            if not getattr(event, "is_admin", lambda: False)():
                yield event.plain_result("测试通知需要管理员权限。")
                return
            targets = [self._state.get("notify_friend_umo"), self._state.get("notify_group_umo")]
            sent_count = 0
            for target in targets:
                if target:
                    try:
                        ok = await self.context.send_message(target, MessageChain([Plain("🐋 上下文哨兵测试通知：消息投递正常。")]))
                        sent_count += int(bool(ok))
                    except Exception:
                        logger.exception("测试通知发送失败")
            yield event.plain_result(f"测试消息已尝试投递，成功 {sent_count} 个目标。")
            return

        yield event.plain_result(
            "用法：/记忆监控 状态（或 检查）\n"
            "在私聊发送 /记忆监控 绑定私聊\n"
            "在目标群发送 /记忆监控 绑定群聊\n"
            "管理员可用 /记忆监控 会话列表 查看数据库 user_id 候选；"
            "管理员可用 /记忆监控 测试 检查通知是否通畅。"
        )


# AstrBot 插件加载器按约定查找 Main。
Main = ContextGuard
