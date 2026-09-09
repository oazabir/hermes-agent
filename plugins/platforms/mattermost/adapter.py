"""Mattermost gateway adapter — REST API v4 + WebSocket via aiohttp (no Mattermost SDK).

Environment variables:
    MATTERMOST_URL              Server URL (e.g. https://mm.example.com)
    MATTERMOST_TOKEN            Bot token or personal-access token
    MATTERMOST_ALLOWED_USERS    Comma-separated user IDs
    MATTERMOST_HOME_CHANNEL     Channel ID for cron/notification delivery
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import mimetypes
import os
import re
from pathlib import Path
from urllib.parse import unquote as _unquote
from typing import Any, Dict, List, Optional, Tuple

from gateway.config import Platform, PlatformConfig
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms.base import gateway_trust_env, BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms._shared import get_scoped_secret as _get_scoped_secret, profile_scoped as _profile_scoped_config_load

logger = logging.getLogger(__name__)

_Metadata = Optional[Dict[str, Any]]

# Server default is 16383, but 4000 is the practical limit for readable messages.
MAX_POST_LENGTH = 4000

# Channel type codes returned by the Mattermost API ("P" private → treat as group).
_CHANNEL_TYPE_MAP = {"D": "dm", "G": "group", "P": "group", "O": "channel"}

_MATTERMOST_DISABLE_MENTIONS_PROPS = {"disable_mentions": True}

_RECONNECT_BASE_DELAY, _RECONNECT_MAX_DELAY, _RECONNECT_JITTER = 2.0, 60.0, 0.2  # exponential backoff

_POST_WITH_FILE_ERROR = "Failed to post with file"
_MEDIA_MSG_TYPES = (("image/", MessageType.PHOTO), ("audio/", MessageType.VOICE))  # first match wins
_INBOUND_CACHE_EXT = {"image/": ".png", "audio/": ".ogg"}  # mime prefix → default extension for cached media


def _with_mentions_disabled(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return a post payload that prevents Mattermost from firing mentions."""
    props, disable = payload.get("props"), _MATTERMOST_DISABLE_MENTIONS_PROPS
    payload["props"] = {**props, **disable} if isinstance(props, dict) else dict(disable)
    return payload


def _channel_id_set(raw: Any) -> set:
    """Parse a list or comma-separated string of channel IDs into a stripped set."""
    items = raw if isinstance(raw, list) else str(raw).split(",")
    return {str(c).strip() for c in items if str(c).strip()}


def _csv(value: Any) -> str:
    return ",".join(str(v) for v in value) if isinstance(value, list) else str(value)


def _post_result(data: Dict[str, Any], error: str) -> SendResult:
    if not data or "id" not in data:
        return SendResult(success=False, error=error)
    return SendResult(success=True, message_id=data["id"])


def _url_filename(url: str, fallback: str) -> str:
    return url.rsplit("/", 1)[-1].split("?")[0] or fallback


def _url_and_token(config) -> Tuple[str, str]:
    """(server URL, token): ``config`` first, MATTERMOST_URL / MATTERMOST_TOKEN env fallback."""
    extra = getattr(config, "extra", {}) or {}
    return (extra.get("url") or _get_scoped_secret("MATTERMOST_URL", ""),
            getattr(config, "token", None) or _get_scoped_secret("MATTERMOST_TOKEN", ""))


def check_mattermost_requirements() -> bool:
    """Return True if the Mattermost adapter runtime dependency is available."""
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("Mattermost: aiohttp not installed")
        return False


def validate_mattermost_config(config: PlatformConfig) -> bool:
    """Return True when Mattermost has enough config to connect."""
    url, token = _url_and_token(config)
    if not token.strip():
        logger.debug("Mattermost: MATTERMOST_TOKEN not set")
        return False
    if not url.strip():
        logger.warning("Mattermost: MATTERMOST_URL not set")
        return False
    return True


class MattermostAdapter(BasePlatformAdapter):
    """Gateway adapter for Mattermost (self-hosted or cloud)."""

    splits_long_messages = True  # send() chunks via truncate_message(MAX_POST_LENGTH)

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.MATTERMOST)
        self._base_url, self._token = _url_and_token(config)
        self._base_url = self._base_url.rstrip("/")
        self._bot_user_id = self._bot_username = ""
        self._session: Any = None  # aiohttp.ClientSession
        self._ws: Any = None  # aiohttp.ClientWebSocketResponse
        self._ws_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._closing = False
        # Reply mode: "thread" to nest replies, "off" for flat messages.
        self._reply_mode: str = (
            config.extra.get("reply_mode", "") or _get_scoped_secret("MATTERMOST_REPLY_MODE", "off")).lower()
        self._last_post_status: Optional[int] = None  # POST-only, read by the broken-thread-root fallback
        self._last_post_error: str = ""
        self._dedup = MessageDeduplicator()

        # Track threads the bot is participating in (thread root post IDs).
        self._bot_threads: set[str] = set()

    # --- HTTP helpers ---

    def _headers(self) -> Dict[str, str]:
        return {**self._auth_header(), "Content-Type": "application/json"}

    def _auth_header(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def _api(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """{method} /api/v4/{path}; POST also records _last_post_status/_last_post_error."""
        if ".." in path:
            logger.error("MM API path traversal blocked: %s", path)
            return {}
        url = f"{self._base_url}/api/v4/{path.lstrip('/')}"
        is_post = method == "POST"
        if is_post:
            self._last_post_status, self._last_post_error = None, ""
        kwargs: Dict[str, Any] = {"headers": self._headers()}
        if payload is not None:
            kwargs["json"] = payload
        # Use asyncio.wait_for() instead of aiohttp ClientTimeout to avoid
        # "Timeout context manager should be used inside a task" errors when
        # invoked via asyncio.run_coroutine_threadsafe() from cron jobs.
        async def _do():
            async with getattr(self._session, method.lower())(url, **kwargs) as resp:
                if is_post:
                    self._last_post_status = resp.status
                if resp.status >= 400:
                    body = await resp.text()
                    if is_post:
                        self._last_post_error = body or ""
                    logger.error("MM API %s %s → %s: %s", method, path, resp.status, body[:200])
                    return {}
                return await resp.json()
        try:
            return await asyncio.wait_for(_do(), timeout=30)
        except asyncio.TimeoutError:
            if is_post:
                self._last_post_error = "request timed out"
            logger.error("MM API %s %s timed out", method, path)
            return {}
        except Exception as exc:
            if is_post:
                self._last_post_error = str(exc)
            logger.error("MM API %s %s error: %s", method, path, exc)
            return {}

    async def _api_get(self, path: str) -> Dict[str, Any]:
        return await self._api("GET", path)

    async def _api_post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return await self._api("POST", path, payload)

    def _last_post_failure_is_broken_thread_root(self) -> bool:
        """Return True only for clear invalid/missing Mattermost thread roots."""
        body = (self._last_post_error or "").lower()
        if self._last_post_status not in {400, 404} or not body:
            return False
        return (any(marker in body for marker in ("root_id", "rootid", "root id", "thread", "post"))
                and any(marker in body for marker in ("invalid", "not found", "does not exist", "missing")))

    async def _post_preserving_thread(
        self, chat_id: str, payload: Dict[str, Any], metadata: _Metadata) -> Dict[str, Any]:
        """Post once, optionally falling back flat for final notify content."""
        data = await self._api_post("posts", payload)
        if data and "root_id" in payload:
            # Record this thread root so we know the bot is participating.
            self._bot_threads.add(payload["root_id"])
        if (data or "root_id" not in payload or not (isinstance(metadata, dict) and metadata.get("notify"))
                or not self._last_post_failure_is_broken_thread_root()):
            return data
        flat_payload = {k: v for k, v in payload.items() if k != "root_id"}
        flat_payload["message"] = ("⚠️ Mattermost thread delivery failed; posting final reply in channel.\n\n"
                                   + str(flat_payload.get("message") or "")).strip()
        logger.warning("Mattermost: falling back to flat channel delivery for notify-worthy post in %s", chat_id)
        return await self._api_post("posts", flat_payload)

    async def _post_message(self, chat_id: str, message: str, reply_to: Optional[str], metadata: _Metadata,
                            file_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Build a mentions-disabled post payload (+ optional root_id) and post it."""
        base: Dict[str, Any] = {"channel_id": chat_id, "message": message}
        if file_ids is not None:
            base["file_ids"] = file_ids
        payload = _with_mentions_disabled(base)
        if self._reply_mode == "thread":
            # root_id from reply_to, else metadata["thread_id"]/["root_id"], resolved to the true thread root.
            candidate = reply_to or (
                isinstance(metadata, dict) and (metadata.get("thread_id") or metadata.get("root_id")))
            if candidate:
                payload["root_id"] = await self._resolve_root_id(str(candidate))
        return await self._post_preserving_thread(chat_id, payload, metadata)

    async def _post_with_file(self, chat_id: str, file_id: str, caption: Optional[str], reply_to: Optional[str],
                              metadata: _Metadata) -> SendResult:
        return _post_result(await self._post_message(chat_id, caption or "", reply_to, metadata, [file_id]),
                            _POST_WITH_FILE_ERROR)

    async def _upload_file(self, channel_id: str, file_data: bytes, filename: str,
                           content_type: str = "application/octet-stream") -> Optional[str]:
        """Upload a file and return its file ID, or None on failure."""
        import aiohttp
        url = f"{self._base_url}/api/v4/files"
        form = aiohttp.FormData()
        form.add_field("channel_id", channel_id)
        form.add_field("files", file_data, filename=filename, content_type=content_type)
        headers = self._auth_header()
        # Use asyncio.wait_for() instead of aiohttp ClientTimeout to avoid
        # "Timeout context manager should be used inside a task" errors when
        # invoked via asyncio.run_coroutine_threadsafe() from cron jobs.
        async def _do():
            async with self._session.post(url, headers=headers, data=form) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error("MM file upload → %s: %s", resp.status, body[:200])
                    return None
                data = await resp.json()
                infos = data.get("file_infos", [])
                return infos[0]["id"] if infos else None
        try:
            return await asyncio.wait_for(_do(), timeout=60)
        except asyncio.TimeoutError:
            logger.error("MM file upload timed out")
            return None
        except Exception as exc:
            logger.error("MM file upload error: %s", exc)
            return None

    # --- Required overrides ---

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to Mattermost and start the WebSocket listener."""
        import aiohttp
        if not self._base_url or not self._token:
            logger.error("Mattermost: URL or token not configured")
            return False

        # No session-level timeout: _api()/`_upload_file` each use asyncio.wait_for()
        # to avoid "Timeout context manager should be used inside a task" errors
        # when invoked via asyncio.run_coroutine_threadsafe() from cron jobs.
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False),
            trust_env=gateway_trust_env(),
        )
        self._closing = False
        me = await self._api_get("users/me")
        if not me or "id" not in me:
            logger.error("Mattermost: failed to authenticate — check MATTERMOST_TOKEN and MATTERMOST_URL")
            await self._session.close()
            return False
        self._bot_user_id, self._bot_username = me["id"], me.get("username", "")
        logger.info(
            "Mattermost: authenticated as @%s (%s) on %s", self._bot_username, self._bot_user_id, self._base_url)
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._mark_connected()
        self._wire_plugin_handlers(None)  # plugin-registered native handlers
        return True

    async def disconnect(self) -> None:
        self._closing = True
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._ws_task
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("Mattermost: disconnected")

    async def _resolve_root_id(self, post_id: str) -> str:
        """Resolve a post_id to its thread root_id (a reply's own ID causes "Invalid RootId parameter")."""
        if not post_id:
            return post_id
        data = await self._api_get(f"posts/{post_id}")
        return data["root_id"] if data and data.get("root_id") else post_id

    async def send(
        self, chat_id: str, content: str, reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        """Send a message (or multiple chunks) to a channel; reply_to / metadata["thread_id"] is the root post."""
        if not content:
            return SendResult(success=True)
        result = SendResult(success=True)
        for chunk in self.truncate_message(self.format_message(content), MAX_POST_LENGTH):
            result = _post_result(await self._post_message(chat_id, chunk, reply_to, metadata), "Failed to create post")
            if not result.success:
                break
        return result

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        data = await self._api_get(f"channels/{chat_id}")
        if not data:
            return {"name": chat_id, "type": "channel"}
        return {"name": data.get("display_name") or data.get("name") or chat_id,
                "type": _CHANNEL_TYPE_MAP.get(data.get("type", "O"), "channel")}

    # --- Optional overrides ---

    async def send_typing(self, chat_id: str, metadata: _Metadata = None) -> None:
        await self._api_post(f"users/{self._bot_user_id}/typing", {"channel_id": chat_id})

    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False) -> SendResult:
        payload = _with_mentions_disabled({"message": self.format_message(content)})
        return _post_result(await self._api("PUT", f"posts/{message_id}/patch", payload), "Failed to edit post")

    async def send_image(self, chat_id: str, image_url: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        return await self._send_url_as_file(chat_id, image_url, caption, reply_to, "image", metadata)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        return await self._send_local_file(chat_id, image_path, caption, reply_to, metadata=metadata)

    async def send_document(
        self, chat_id: str, file_path: str, caption: Optional[str] = None, file_name: Optional[str] = None,
        reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        return await self._send_local_file(chat_id, file_path, caption, reply_to, file_name, metadata)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        return await self._send_local_file(chat_id, audio_path, caption, reply_to, metadata=metadata)

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        return await self._send_local_file(chat_id, video_path, caption, reply_to, metadata=metadata)

    def format_message(self, content: str) -> str:
        """Mattermost renders standard Markdown; reduce ![alt](url) to the bare URL (inline preview)."""
        return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", content)

    # --- File helpers ---

    async def _send_url_as_file(self, chat_id: str, url: str, caption: Optional[str], reply_to: Optional[str],
                                kind: str = "file", metadata: _Metadata = None) -> SendResult:
        """Download a URL and upload it as a file attachment (text fallback with the URL on failure)."""
        from tools.url_safety import is_safe_url

        async def fallback() -> SendResult:
            return await self.send(chat_id, f"{caption or ''}\n{url}".strip(), reply_to, metadata=metadata)

        if not is_safe_url(url):
            logger.warning("Mattermost: blocked unsafe URL (SSRF protection)")
            return await fallback()
        import aiohttp
        for attempt in range(3):  # retry 5xx/429 and network errors twice with linear backoff
            try:
                async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if (resp.status >= 500 or resp.status == 429) and attempt < 2:
                        logger.debug("Mattermost download retry %d/2 for %s (status %d)",
                                     attempt + 1, url[:80], resp.status)
                    elif resp.status >= 400:
                        return await fallback()
                    else:
                        file_data, ct = await resp.read(), resp.content_type or "application/octet-stream"
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == 2:
                    logger.warning("Mattermost: failed to download %s after %d attempts: %s", url, attempt + 1, exc)
                    return await fallback()
            await asyncio.sleep(1.5 * (attempt + 1))
        file_id = await self._upload_file(chat_id, file_data, _url_filename(url, f"{kind}.png"), ct)
        return await self._post_with_file(chat_id, file_id, caption, reply_to, metadata) if file_id else await fallback()

    async def _send_local_file(
        self, chat_id: str, file_path: str, caption: Optional[str], reply_to: Optional[str],
        file_name: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        """Upload a local file and attach it to a post."""
        p = Path(file_path)
        if not p.exists():
            logger.warning("Mattermost: local file not found, skipping: %s", file_path)
            return SendResult(success=True, message_id=None)
        fname = file_name or p.name
        file_id = await self._upload_file(chat_id, p.read_bytes(), fname,
                                          mimetypes.guess_type(fname)[0] or "application/octet-stream")
        if not file_id:
            return SendResult(success=False, error="File upload failed")
        return await self._post_with_file(chat_id, file_id, caption, reply_to, metadata)

    async def _load_batch_image(self, image_url: str, index: int) -> Optional[Tuple[bytes, str, str]]:
        """Read a file:// or remote image for a batch post → (data, filename, content_type), or None to skip."""
        import aiohttp
        if image_url.startswith("file://"):
            local_path = _unquote(image_url[7:])
            p = Path(local_path)
            if not p.exists():
                logger.warning("Mattermost: skipping missing image %s", local_path)
                return None
            return p.read_bytes(), p.name, mimetypes.guess_type(p.name)[0] or "image/png"
        from tools.url_safety import is_safe_url
        if not is_safe_url(image_url):
            logger.warning("Mattermost: blocked unsafe image URL in batch")
            return None
        try:
            async with self._session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status >= 400:
                    logger.warning("Mattermost: failed to download image (HTTP %d): %s", resp.status, image_url[:80])
                    return None
                file_data, ct = await resp.read(), resp.content_type or "image/png"
        except Exception as dl_err:
            logger.warning("Mattermost: download failed for %s: %s", image_url[:80], dl_err)
            return None
        return file_data, _url_filename(image_url, f"image_{index}.png"), ct

    async def send_multiple_images(self, chat_id: str, images: List[Tuple[str, str]],
                                   metadata: _Metadata = None, human_delay: float = 0.0) -> SendResult:
        """Send a batch of images as one post; chunked at Mattermost's 5-``file_ids`` cap, each chunk
        falling back to the base per-image loop on failure."""
        if not images:
            return SendResult(success=False, error="no images to send")
        chunks = [images[i:i + 5] for i in range(0, len(images), 5)]  # Mattermost post file_ids cap
        delivered = False
        for chunk_idx, chunk in enumerate(chunks):
            if human_delay > 0 and chunk_idx > 0:
                await asyncio.sleep(human_delay)
            file_ids, caption_parts = [], []
            try:
                for image_url, alt_text in chunk:
                    if alt_text:
                        caption_parts.append(alt_text)
                    loaded = await self._load_batch_image(image_url, len(file_ids))
                    if loaded is not None and (fid := await self._upload_file(chat_id, *loaded)):
                        file_ids.append(fid)
                if not file_ids:
                    continue
                logger.info("Mattermost: sending %d image(s) as single post (chunk %d/%d)",
                            len(file_ids), chunk_idx + 1, len(chunks))
                data = await self._post_message(chat_id, "\n".join(caption_parts), None, metadata, file_ids)
                if data and "id" in data:
                    delivered = True
                else:
                    logger.warning("Mattermost: multi-image post failed, falling back")
                    fallback = await super().send_multiple_images(chat_id, chunk, metadata, human_delay=human_delay)
                    delivered = delivered or fallback.success
            except Exception as e:
                logger.warning("Mattermost: multi-image send failed (chunk %d/%d), falling back: %s",
                               chunk_idx + 1, len(chunks), e, exc_info=True)
                fallback = await super().send_multiple_images(chat_id, chunk, metadata, human_delay=human_delay)
                delivered = delivered or fallback.success
        return SendResult(success=delivered, error=None if delivered else "all images failed to send")

    # --- WebSocket ---

    async def _ws_loop(self) -> None:
        """Connect to the WebSocket and listen for events, reconnecting on failure."""
        import aiohttp
        import random
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._ws_connect_and_listen()
                delay = _RECONNECT_BASE_DELAY  # clean disconnect — reset backoff
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closing:
                    return
                # Permanent auth failure: escalate via the fatal-error hook (a bare return leaves is_connected()
                # healthy with a dead listener). Type-based: substring "401" matching misclassified transient errors.
                if isinstance(exc, aiohttp.WSServerHandshakeError) and exc.status in {401, 403}:
                    logger.error("Mattermost WS auth failed (HTTP %d) — stopping reconnect", exc.status)
                    # Escalate through the fatal-error hook instead of a bare return: the old silent exit
                    # left _running True, so is_connected() kept reporting healthy while the listener was
                    # dead and the gateway was never told (OOF-156 class). Type-based only — the substring
                    # fallback that used to sit below this branch misclassified transient errors whose
                    # message merely contained "401" (#80489).
                    self._set_fatal_error(
                        "mattermost_auth_error",
                        f"Mattermost WebSocket authentication rejected (HTTP {exc.status}). The bot token is "
                        "invalid, revoked, or lacks permission — check MATTERMOST_TOKEN and the bot account in "
                        "the System Console.", retryable=False)
                    await self._notify_fatal_error()
                    return
                logger.warning("Mattermost WS error: %s — reconnecting in %.0fs", exc, delay)
            if self._closing:
                return
            await asyncio.sleep(delay + delay * _RECONNECT_JITTER * random.random())
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _ws_connect_and_listen(self) -> None:
        """Single WebSocket session: connect, authenticate, process events."""
        ws_url = re.sub(r"^http", "ws", self._base_url) + "/api/v4/websocket"  # https→wss, http→ws
        logger.info("Mattermost: connecting to %s", ws_url)
        self._ws = await self._session.ws_connect(ws_url, heartbeat=30.0)
        await self._ws.send_json({"seq": 1, "action": "authentication_challenge", "data": {"token": self._token}})
        logger.info("Mattermost: WebSocket connected and authenticated")

        async for raw_msg in self._ws:
            if self._closing:
                return
            kind = raw_msg.type
            if kind in {kind.TEXT, kind.BINARY}:
                try:
                    event = json.loads(raw_msg.data)
                except (json.JSONDecodeError, TypeError):
                    continue
                await self._handle_ws_event(event)
            elif kind in {kind.ERROR, kind.CLOSE, kind.CLOSING, kind.CLOSED}:
                logger.info("Mattermost: WebSocket closed (%s)", kind)
                break

    def _extra_or_env(self, key: str, env: str, default: str = "") -> Any:
        """config.yaml ``mattermost.<key>`` (PlatformConfig.extra) first, env var fallback."""
        raw = self.config.extra.get(key) if self.config.extra else None
        return _get_scoped_secret(env, default) if raw is None else raw

    def _apply_channel_gating(self, channel_id: str, message_text: str) -> Optional[str]:
        """Mention-gate a non-DM post; return the cleaned text, or None to ignore it. allowed_channels is a
        whitelist checked first (@mentions elsewhere are ignored); require_mention (default true) is
        bypassed in free_response_channels."""
        allowed_channels = _channel_id_set(self._extra_or_env("allowed_channels", "MATTERMOST_ALLOWED_CHANNELS"))
        if allowed_channels and channel_id not in allowed_channels:
            logger.debug("Mattermost: ignoring message in non-allowed channel: %s", channel_id)
            return None
        require_mention = str(self._extra_or_env("require_mention", "MATTERMOST_REQUIRE_MENTION", "true")
                              ).lower() not in {"false", "0", "no"}
        free_channels = _channel_id_set(
            self._extra_or_env("free_response_channels", "MATTERMOST_FREE_RESPONSE_CHANNELS"))
        mention_patterns = [f"@{self._bot_username}", f"@{self._bot_user_id}"]
        has_mention = any(pattern.lower() in message_text.lower() for pattern in mention_patterns)
        if require_mention and channel_id not in free_channels and not has_mention:
            logger.debug("Mattermost: skipping non-DM message without @mention (channel=%s)", channel_id)
            return None
        if has_mention:  # strip the @mention so the agent sees clean input
            for pattern in mention_patterns:
                message_text = re.sub(re.escape(pattern), "", message_text, flags=re.IGNORECASE).strip()
        return message_text

    async def _gate_thread_reply(self, channel_id: str, thread_root: str, message_text: str) -> Optional[str]:
        """Thread-reply gating: auto-continue conversations the bot already started.

        Returns the cleaned message text to process, or None to ignore the reply.
        """
        if thread_root not in self._bot_threads:
            # Check if the root post itself mentions the bot (thread was started
            # by mentioning the bot) -- covers the case after a gateway restart
            # when _bot_threads is empty.
            root_post = await self._api_get(f"posts/{thread_root}")
            if root_post:
                root_msg = root_post.get("message", "")
                mention_patterns = [f"@{self._bot_username}", f"@{self._bot_user_id}"]
                if any(p.lower() in root_msg.lower() for p in mention_patterns):
                    self._bot_threads.add(thread_root)
            if thread_root not in self._bot_threads:
                logger.debug(
                    "Mattermost: ignoring thread reply in non-bot thread (root=%s, channel=%s)",
                    thread_root, channel_id,
                )
                return None
        # In threads the bot started: respond to ALL replies automatically,
        # unless the reply mentions someone else (directed at them). The user
        # only needs to @mention the bot once to start the thread; subsequent
        # replies flow without re-mentioning.
        mention_patterns = [f"@{self._bot_username}", f"@{self._bot_user_id}"]
        all_mentions = re.findall(r"@\S+", message_text)
        bot_mentioned = any(m.lower() in {p.lower() for p in mention_patterns} for m in all_mentions)
        non_bot_mentions = [m for m in all_mentions if m.lower() not in {p.lower() for p in mention_patterns}]
        if non_bot_mentions and not bot_mentioned:
            logger.debug(
                "Mattermost: ignoring thread reply directed at others (root=%s, channel=%s, mentions=%s)",
                thread_root, channel_id, non_bot_mentions,
            )
            return None
        for pattern in mention_patterns:
            message_text = re.sub(re.escape(pattern), "", message_text, flags=re.IGNORECASE).strip()

        # Command detection BEFORE thread-context injection: injected context
        # ("[Original message...]") would break startswith() checks in the
        # caller, and commands do not need thread-root context anyway.
        if not message_text.lstrip().startswith(("!", "/")):
            # Fetch the original message that started this thread so the
            # agent has full context of what was asked.
            root_post = await self._api_get(f"posts/{thread_root}")
            if root_post:
                root_message = root_post.get("message", "")
                root_sender = root_post.get("user_id", "")
                if root_sender == self._bot_user_id:
                    # The thread root is the bot's own reply -- fetch the user's
                    # message that the bot replied to.
                    parent_id = root_post.get("root_id")
                    if parent_id:
                        parent_post = await self._api_get(f"posts/{parent_id}")
                        parent_msg = parent_post.get("message", "") if parent_post else ""
                        if parent_msg:
                            message_text = f"[Original message that started this thread: {parent_msg}]\n\n{message_text}"
                elif root_message:
                    message_text = f"[Original message that started this thread: {root_message}]\n\n{message_text}"
        return message_text

    async def _download_attachments(self, file_ids: List[str]) -> Tuple[List[str], List[str]]:
        """Download attachments now (URLs need auth headers downstream tools lack) → (paths, mime types)."""
        import aiohttp
        from gateway.platforms.base import (
            cache_audio_from_bytes_async,
            cache_document_from_bytes_async,
            cache_image_from_bytes_async,
        )
        media_urls, media_types = [], []
        cache_fns = {"image/": cache_image_from_bytes_async, "audio/": cache_audio_from_bytes_async}
        for fid in file_ids:
            try:
                file_info = await self._api_get(f"files/{fid}/info")
                fname = file_info.get("name", f"file_{fid}")
                mime = file_info.get("mime_type", "application/octet-stream")
                async with self._session.get(
                    f"{self._base_url}/api/v4/files/{fid}", headers=self._auth_header(),
                    timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status >= 400:
                        logger.warning("Mattermost: failed to download file %s: HTTP %s", fid, resp.status)
                        continue
                    file_data = await resp.read()
                    prefix = next((p for p in cache_fns if mime.startswith(p)), None)
                    if prefix:
                        media_urls.append(
                            await cache_fns[prefix](file_data, Path(fname).suffix or _INBOUND_CACHE_EXT[prefix]))
                    else:
                        media_urls.append(await cache_document_from_bytes_async(file_data, fname))
                    media_types.append(mime)
            except Exception as exc:
                logger.warning("Mattermost: error downloading file %s: %s", fid, exc)
        return media_urls, media_types

    async def _handle_ws_event(self, event: Dict[str, Any]) -> None:
        if event.get("event") != "posted":
            return
        data = event.get("data", {})
        try:
            post = json.loads(data.get("post") or "")
        except (json.JSONDecodeError, TypeError):
            return
        # Ignore own messages, system posts and redeliveries.
        sender_id, post_id = post.get("user_id", ""), post.get("id", "")
        if sender_id == self._bot_user_id or post.get("type") or self._dedup.is_duplicate(post_id):
            return
        channel_id, is_dm = post.get("channel_id", ""), data.get("channel_type", "O") == "D"
        message_text = post.get("message", "")
        thread_root = post.get("root_id") or None
        if not is_dm:
            if thread_root:
                # allowed_channels whitelist check -- must pass before thread
                # auto-continuation too, same as the non-thread gating path.
                allowed_channels = _channel_id_set(
                    self._extra_or_env("allowed_channels", "MATTERMOST_ALLOWED_CHANNELS"))
                if allowed_channels and channel_id not in allowed_channels:
                    logger.debug("Mattermost: ignoring message in non-allowed channel: %s", channel_id)
                    return
                message_text = await self._gate_thread_reply(channel_id, thread_root, message_text)
                if message_text is None:
                    return
            else:  # DMs need no gating; non-thread channel posts are mention-gated.
                message_text = self._apply_channel_gating(channel_id, message_text)
                if message_text is None:
                    return
        # Thread support: replies use root_id; in thread mode a top-level channel post is itself a valid root.
        thread_id = thread_root
        if not thread_id and self._reply_mode == "thread" and not is_dm and post_id:
            thread_id = post_id
        # Mattermost intercepts /-prefixed messages client-side as slash commands --
        # they never reach the bot -- and mangles \-prefixed text via Markdown escape
        # processing. Use ! as the gateway command prefix instead; / is kept as a
        # fallback for paths that DO reach the bot (e.g. webhooks).
        if message_text[:1].isspace() and message_text.lstrip().startswith(("!", "/")):
            message_text = message_text.lstrip()
        msg_type = MessageType.TEXT
        if message_text.startswith("!"):
            message_text = "/" + message_text[1:]
            msg_type = MessageType.COMMAND
            logger.debug(
                "Mattermost: exclamation command detected, converted '%s' -> '%s'",
                post.get("message", ""), message_text,
            )
        elif message_text.startswith("/"):
            msg_type = MessageType.COMMAND
        media_urls, media_types = await self._download_attachments(post.get("file_ids") or [])
        if msg_type != MessageType.COMMAND and media_types:
            msg_type = next((mt for prefix, mt in _MEDIA_MSG_TYPES if any(m.startswith(prefix) for m in media_types)),
                            MessageType.DOCUMENT)
        source = self.build_source(
            chat_id=channel_id, chat_type=_CHANNEL_TYPE_MAP.get(data.get("channel_type", "O"), "channel"),
            user_id=sender_id, user_name=data.get("sender_name", "").lstrip("@") or sender_id,
            thread_id=thread_id, message_id=post_id)
        from gateway.platforms.base import resolve_channel_prompt
        msg_event = MessageEvent(
            text=message_text, message_type=msg_type, source=source, raw_message=post, message_id=post_id,
            media_urls=media_urls or None, media_types=media_types or None,
            channel_prompt=resolve_channel_prompt(self.config.extra, channel_id, None))

        # Send an instant "thinking..." acknowledgment before processing so the
        # user sees the message was received while the agent runs in the background.
        _think_root_id = thread_id if self._reply_mode == "thread" else thread_root
        await self._api_post("posts", {
            "channel_id": channel_id,
            "message": "thinking...",
            **({"root_id": _think_root_id} if _think_root_id else {}),
        })

        await self.handle_message(msg_event)


# --- Plugin standalone-send (out-of-process cron delivery via Mattermost REST) ---

async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: Optional[str] = None,
                           media_files: Optional[list] = None, force_document: bool = False) -> Dict[str, Any]:
    """Send via the Mattermost v4 REST API without a live gateway adapter (out-of-process cron).

    Token/URL: ``pconfig`` with env fallback. ``media_files`` upload via ``POST /files`` and attach by
    file_id; ``thread_id`` becomes ``root_id``. ``force_document`` is signature parity only (unused).
    """
    try:
        import aiohttp
    except ImportError:
        return {"error": "aiohttp not installed. Run: pip install aiohttp"}

    base_url, token = _url_and_token(pconfig)
    base_url, token = base_url.rstrip("/"), token.strip()
    if not base_url or not token:
        return {"error": "Mattermost standalone send: MATTERMOST_URL and MATTERMOST_TOKEN must both be set"}
    upload_headers = {"Authorization": f"Bearer {token}"}
    headers = {**upload_headers, "Content-Type": "application/json"}
    try:
        # One ClientSession (with proxy) covers the optional uploads + final post.
        from gateway.platforms.base import resolve_proxy_url, proxy_kwargs_for_aiohttp
        _sess_kw, _req_kw = proxy_kwargs_for_aiohttp(resolve_proxy_url(platform_env_var="MATTERMOST_PROXY"))
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), **_sess_kw) as session:
            file_ids: List[str] = []
            for media in media_files or []:
                file_path = media.get("path") if isinstance(media, dict) else media
                if not file_path or not os.path.exists(file_path):
                    continue
                form = aiohttp.FormData()
                form.add_field("channel_id", chat_id)  # required so the server can attribute the upload
                with open(file_path, "rb") as fh:
                    form.add_field("files", fh.read(), filename=os.path.basename(file_path))
                async with session.post(f"{base_url}/api/v4/files", data=form, headers=upload_headers,
                                        **_req_kw) as upload_resp:
                    if upload_resp.status not in {200, 201}:
                        body = await upload_resp.text()
                        return {"error": f"Mattermost file upload failed ({upload_resp.status}): {body[:400]}"}
                    upload_data = await upload_resp.json()
                    file_ids.extend(info["id"] for info in upload_data.get("file_infos", []) if info.get("id"))
            payload: Dict[str, Any] = {"channel_id": chat_id, "message": message}
            if thread_id:
                payload["root_id"] = thread_id
            if file_ids:
                payload["file_ids"] = file_ids
            async with session.post(f"{base_url}/api/v4/posts", headers=headers, json=payload, **_req_kw) as resp:
                if resp.status not in {200, 201}:
                    body = await resp.text()
                    return {"error": f"Mattermost API error ({resp.status}): {body[:400]}"}
                data = await resp.json()
            return {"success": True, "platform": "mattermost", "chat_id": chat_id, "message_id": data.get("id")}
    except aiohttp.ClientError as exc:
        return {"error": f"Mattermost send failed (network): {exc}"}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"Mattermost send failed: {exc}"}


# --- Interactive setup wizard ---

def interactive_setup() -> None:
    """Guide the user through Mattermost bot setup (URL + token, allowlist, home channel)."""
    from hermes_cli.config import get_env_value, remove_env_value, save_env_value
    from hermes_cli.cli_output import prompt, prompt_yes_no, print_header, print_info, print_success

    def info(*lines: str) -> None:
        for line in lines:
            print_info(line)

    print_header("Mattermost")
    if get_env_value("MATTERMOST_TOKEN"):
        print_info("Mattermost: already configured")
        if not prompt_yes_no("Reconfigure Mattermost?", False):
            return
    info("Works with any self-hosted Mattermost instance.",
         "   1. In Mattermost: Integrations → Bot Accounts → Add Bot Account", "   2. Copy the bot token")
    print()
    mm_url = prompt("Mattermost server URL (e.g. https://mm.example.com)")
    if mm_url:
        save_env_value("MATTERMOST_URL", mm_url.rstrip("/"))
    token = prompt("Bot token", password=True)
    if not token:
        return
    save_env_value("MATTERMOST_TOKEN", token)
    print_success("Mattermost token saved")
    print()
    info("🔒 Security: Restrict who can use your bot", "   To find your user ID: click your avatar → Profile",
         "   or use the API: GET /api/v4/users/me")
    print()
    allowed_users = prompt("Allowed user IDs (comma-separated, leave empty for open access)")
    if allowed_users:
        save_env_value("MATTERMOST_ALLOWED_USERS", allowed_users.replace(" ", ""))
        print_success("Mattermost allowlist configured")
    else:
        print_info("⚠️  No allowlist set - anyone who can message the bot can use it!")
    print()
    info("📬 Home Channel: where Hermes delivers cron job results and notifications.",
         "   To get a channel ID: click channel name → View Info → copy the ID",
         "   You can also set this later by typing /set-home in a Mattermost channel.")
    home_channel = prompt("Home channel ID (leave empty to set later with /set-home)").strip()
    if home_channel:
        save_env_value("MATTERMOST_HOME_CHANNEL", home_channel)
    elif remove_env_value("MATTERMOST_HOME_CHANNEL"):
        print_info("Home channel cleared.")
    print_info("   Open config in your editor:  hermes config edit")


# --- YAML → env config bridge (apply_yaml_config_fn) ---

_YAML_BRIDGE = (  # (yaml key, env var, yaml value → env string); allowed_channels is a whitelist
    ("require_mention", "MATTERMOST_REQUIRE_MENTION", lambda v: str(v).lower()),
    ("free_response_channels", "MATTERMOST_FREE_RESPONSE_CHANNELS", _csv),
    ("allowed_channels", "MATTERMOST_ALLOWED_CHANNELS", _csv))


def _apply_yaml_config(yaml_cfg: dict, mattermost_cfg: dict) -> dict | None:
    """Translate ``config.yaml`` ``mattermost:`` keys into env vars + ``PlatformConfig.extra``.

    Env vars win over YAML (writes guarded by ``not os.getenv``). Under a multiplexed secondary
    profile the env write is skipped (it would leak into every profile via ``os.environ``); the
    values are returned so the caller seeds this profile's ``extra``, which read sites check first.

    Implements the ``apply_yaml_config_fn`` contract (#24836 / #25443). Mirrors the legacy
    ``mattermost_cfg`` block that used to live in ``gateway/config.py::load_gateway_config()`` before this
    migration.
    """
    skip_env_bridge = _profile_scoped_config_load()
    seeded: dict = {}
    for key, env, to_env in _YAML_BRIDGE:
        value = mattermost_cfg.get(key)
        if value is None and not (key == "require_mention" and key in mattermost_cfg):
            continue
        seeded[key] = value
        if not skip_env_bridge and not os.getenv(env):
            os.environ[env] = to_env(value)
    return seeded or None


def _is_connected(config) -> bool:
    """Connected when BOTH MATTERMOST_TOKEN and MATTERMOST_URL are set (``get_env_value`` looked up at
    call time so tests patching ``gateway_mod.get_env_value`` can suppress ambient env vars)."""
    import hermes_cli.gateway as gateway_mod
    return bool(
        (gateway_mod.get_env_value("MATTERMOST_TOKEN") or "").strip()
        and (gateway_mod.get_env_value("MATTERMOST_URL") or "").strip())


# --- Plugin registration entry point ---

def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="mattermost", label="Mattermost", adapter_factory=MattermostAdapter,
        check_fn=check_mattermost_requirements, validate_config=validate_mattermost_config,
        is_connected=_is_connected, required_env=["MATTERMOST_URL", "MATTERMOST_TOKEN"],
        install_hint="pip install aiohttp", setup_fn=interactive_setup,
        apply_yaml_config_fn=_apply_yaml_config,  # YAML→env bridge (see _YAML_BRIDGE)
        allowed_users_env="MATTERMOST_ALLOWED_USERS", allow_all_env="MATTERMOST_ALLOW_ALL_USERS",
        cron_deliver_env_var="MATTERMOST_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,  # out-of-process cron; without it `deliver=mattermost` fails
        max_message_length=MAX_POST_LENGTH, emoji="💬", allow_update_command=True)
