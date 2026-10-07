"""请求体读取契约（可追踪、模式化、可回收）。

审计中间件提前读取上传内容计算摘要后，后续处理器再读时得到空数据；
流式请求被异常处理器接管后也无法说明内容是否已经消费。本模块为请求体
建立可追踪的读取契约，让中间件、处理器和异常路径能够显式选择读取模式：

- ``BodyReadMode.SHARED_CACHE`` —— 共享缓存：首个读取者从源头拉取并写入
  缓存（内存超出阈值时安全转入临时存储），后续读取者从缓存重读，互不影响。
- ``BodyReadMode.EXCLUSIVE_STREAM`` —— 独占流：仅允许一个消费者直读源头，
  不缓存；获取后任何其他读取尝试都会抛出契约冲突。
- ``BodyReadMode.LIMITED_REPLAY`` —— 受限重放：完整内容可被重读的次数受限，
  达到上限后缓存被清除，再次获取抛出 :class:`BodyReplayExhausted`。

契约记录每一次获取（谁、哪种模式、读了多少、是否读完），异常路径可以通过
``request.body_contract`` 查询内容是否已被消费。取消、超时、部分读取和
客户端断开后，协议层会调用 ``aclose`` 回收内存与临时文件。日志接口
（``log_safe`` / ``__repr__``）只暴露长度与摘要等安全信息，绝不包含内容。
"""

from __future__ import annotations

import hashlib

from asyncio import Lock
from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from tempfile import TemporaryFile
from time import perf_counter
from typing import TYPE_CHECKING, Any, BinaryIO

from sanic.exceptions import BodyContractViolation, BodyReplayExhausted
from sanic.log import logger


if TYPE_CHECKING:
    from sanic.request import Request


DEFAULT_MEMORY_THRESHOLD = 1024 * 1024  # 1 MiB
DEFAULT_REPLAY_LIMIT = 1

__all__ = (
    "BodyAccess",
    "BodyContract",
    "BodyContractState",
    "BodyReadMode",
    "BodyReader",
    "SpoolBuffer",
)


class BodyReadMode(str, Enum):
    """请求体读取模式。"""

    SHARED_CACHE = "shared_cache"
    EXCLUSIVE_STREAM = "exclusive_stream"
    LIMITED_REPLAY = "limited_replay"


class BodyContractState(str, Enum):
    """请求体契约状态机。"""

    PRISTINE = "pristine"  # 尚未发生任何读取
    BUFFERING = "buffering"  # 正在从源头拉取并缓存
    CACHED = "cached"  # 已完整缓存，可共享重读
    STREAMING = "streaming"  # 独占流读取中
    EXHAUSTED = "exhausted"  # 已耗尽（独占读完或重放后缓存已清除）
    PARTIAL = "partial"  # 部分读取后中断（取消/失败/提前放弃）
    CLOSED = "closed"  # 资源已回收，不可再读


@dataclass
class BodyAccess:
    """一次读取获取的追踪记录。"""

    label: str
    mode: BodyReadMode
    started_at: float
    bytes_read: int = 0
    completed: bool = False
    ended_at: float | None = None

    def complete(self) -> None:
        """标记本次读取已完整到达源头末尾。"""
        self.completed = True
        if self.ended_at is None:
            self.ended_at = perf_counter()

    def finish(self) -> None:
        """结束本次读取（无论是否完整）。"""
        if self.ended_at is None:
            self.ended_at = perf_counter()


class SpoolBuffer:
    """请求体缓存：内存持有，超过阈值后安全转入临时文件。

    临时文件由 ``tempfile.TemporaryFile`` 创建（关闭即删除），
    ``close``/``clear`` 幂等，保证取消与异常路径下资源必然回收。
    """

    def __init__(
        self,
        threshold: int = DEFAULT_MEMORY_THRESHOLD,
        temp_dir: str | None = None,
    ) -> None:
        self._threshold = max(0, threshold)
        self._temp_dir = temp_dir
        self._memory = bytearray()
        self._file: BinaryIO | None = None
        self._size = 0
        self._closed = False

    def __repr__(self) -> str:
        return (
            f"<SpoolBuffer size={self._size} spilled={self.spilled} "
            f"closed={self._closed}>"
        )

    @property
    def size(self) -> int:
        """当前缓存的字节数。"""
        return self._size

    @property
    def spilled(self) -> bool:
        """是否已溢出到临时存储。"""
        return self._file is not None

    @property
    def closed(self) -> bool:
        return self._closed

    def write(self, chunk: bytes) -> None:
        """追加一块内容；超过内存阈值时转入临时文件。"""
        if self._closed:
            raise BodyContractViolation("请求体缓存已关闭，无法写入")
        if not chunk:
            return
        if self._file is None and self._size + len(chunk) > self._threshold:
            self._file = TemporaryFile(  # nosec B101
                prefix="sanic-body-", dir=self._temp_dir
            )
            if self._memory:
                self._file.write(self._memory)
                self._memory.clear()
        if self._file is not None:
            self._file.write(chunk)
        else:
            self._memory += chunk
        self._size += len(chunk)

    def read(self, offset: int = 0) -> bytes:
        """读取 offset 之后的全部内容（不消耗，可重复读）。"""
        if offset >= self._size:
            return b""
        if self._file is not None:
            self._file.seek(offset)
            data = self._file.read()
            self._file.seek(0, 2)  # 回到末尾，供后续追加
            return data
        return bytes(self._memory[offset:])

    def read_all(self) -> bytes:
        """读取缓存的全部内容。"""
        return self.read(0)

    def clear(self) -> None:
        """清空内容并释放临时文件（受限重放达到上限后调用）。"""
        self._close_file()
        self._memory.clear()
        self._size = 0

    def close(self) -> None:
        """关闭并释放全部资源（幂等）。"""
        self._close_file()
        self._memory.clear()
        self._size = 0
        self._closed = True

    def _close_file(self) -> None:
        if self._file is not None:
            try:
                self._file.close()  # TemporaryFile 关闭即删除
            finally:
                self._file = None


class BodyContract:
    """请求体读取契约。

    协调中间件、处理器与异常路径对请求体的读取，提供共享缓存、
    独占流与受限重放三种模式，并记录全部获取行为以供追踪。
    """

    def __init__(
        self,
        request: Request,
        *,
        memory_threshold: int | None = None,
        replay_limit: int | None = None,
        temp_dir: str | None = None,
    ) -> None:
        config = getattr(getattr(request, "app", None), "config", None)
        if memory_threshold is None:
            memory_threshold = getattr(
                config,
                "REQUEST_BODY_MEMORY_THRESHOLD",
                DEFAULT_MEMORY_THRESHOLD,
            )
        if replay_limit is None:
            replay_limit = getattr(
                config, "REQUEST_BODY_REPLAY_LIMIT", DEFAULT_REPLAY_LIMIT
            )
        self._request = request
        self._spool = SpoolBuffer(memory_threshold, temp_dir)
        self._replay_limit = max(0, replay_limit)
        self._pull_lock = Lock()
        self._digest = hashlib.sha256()
        self._state = BodyContractState.PRISTINE
        self._bytes_received = 0
        self._bytes_lost = 0  # 已消费但未入缓存的字节（独占流）
        self._source_eof = False
        self._source_failed = False
        self._spooling = False
        self._cache_wiped = False
        self._preset_delivered = False
        self._accesses: list[BodyAccess] = []
        self._active_readers = 0
        self._limited_acquires = 0
        self._close_reason: str | None = None

    def __repr__(self) -> str:
        # 只暴露长度与摘要等安全信息，绝不包含内容
        digest: str = "-"
        if self._bytes_received:
            digest = self._digest.hexdigest()[:16] + "..."
        return (
            f"<BodyContract state={self._state.value} "
            f"received={self._bytes_received} sha256={digest} "
            f"cached={self._spool.size} spilled={self._spool.spilled}>"
        )

    # ------------------------------------------------------------------ #
    # 状态查询（异常路径使用）
    # ------------------------------------------------------------------ #

    @property
    def state(self) -> BodyContractState:
        """契约当前状态。"""
        return self._state

    @property
    def was_consumed(self) -> bool:
        """源头内容是否已被完整消费（无论是否缓存）。"""
        return self._source_eof

    @property
    def source_failed(self) -> bool:
        """源头读取是否曾失败（取消、断连等导致无法继续）。"""
        return self._source_failed

    @property
    def bytes_received(self) -> int:
        """已从源头接收的字节数。"""
        return self._bytes_received

    @property
    def bytes_cached(self) -> int:
        """当前缓存持有的字节数。"""
        return self._spool.size

    @property
    def spilled(self) -> bool:
        """缓存是否已溢出到临时存储。"""
        return self._spool.spilled

    @property
    def digest(self) -> str | None:
        """已接收内容的 SHA-256 摘要；尚无内容时为 None。"""
        if not self._bytes_received:
            return None
        return self._digest.hexdigest()

    @property
    def digest_complete(self) -> bool:
        """摘要是否覆盖完整请求体（源头已干净地到达末尾）。"""
        return self._source_eof

    @property
    def accesses(self) -> tuple[BodyAccess, ...]:
        """全部读取获取的追踪记录。"""
        return tuple(self._accesses)

    @property
    def closed(self) -> bool:
        return self._state is BodyContractState.CLOSED

    @property
    def close_reason(self) -> str | None:
        """契约关闭原因（completed/cancelled/client-lost/error 等）。"""
        return self._close_reason

    @property
    def replays_remaining(self) -> int:
        """受限重放剩余可用次数。"""
        return max(0, (1 + self._replay_limit) - self._limited_acquires)

    def log_safe(self) -> dict[str, Any]:
        """仅含长度、摘要等安全信息的日志字段，绝不包含内容。"""
        return {
            "body_state": self._state.value,
            "body_bytes_received": self._bytes_received,
            "body_bytes_cached": self._spool.size,
            "body_sha256": self.digest,
            "body_digest_complete": self._source_eof,
            "body_spilled": self._spool.spilled,
            "body_accesses": len(self._accesses),
            "body_consumers": [access.label for access in self._accesses],
        }

    # ------------------------------------------------------------------ #
    # 读取获取
    # ------------------------------------------------------------------ #

    def acquire(
        self,
        mode: BodyReadMode = BodyReadMode.SHARED_CACHE,
        *,
        label: str = "unknown",
    ) -> BodyReader:
        """按指定模式获取一个读取句柄。"""
        self._ensure_open()
        reader_cls: type[BodyReader]
        if mode is BodyReadMode.EXCLUSIVE_STREAM:
            if self._state is not BodyContractState.PRISTINE or self._accesses:
                raise BodyContractViolation(
                    "独占流必须在任何读取发生之前获取"
                    f"（当前状态：{self._state.value}）"
                )
            self._state = BodyContractState.STREAMING
            reader_cls = _ExclusiveReader
        elif mode is BodyReadMode.SHARED_CACHE:
            self._ensure_cache_available()
            self._start_spooling()
            reader_cls = _SharedReader
        elif mode is BodyReadMode.LIMITED_REPLAY:
            if self._limited_acquires >= 1 + self._replay_limit:
                raise BodyReplayExhausted(
                    f"请求体重放次数已达上限（{self._replay_limit}）"
                )
            self._ensure_cache_available()
            self._limited_acquires += 1
            self._start_spooling()
            reader_cls = _SharedReader
        else:  # pragma: no cover - 防御未知模式
            raise BodyContractViolation(f"未知的请求体读取模式：{mode!r}")

        access = BodyAccess(label=label, mode=mode, started_at=perf_counter())
        self._accesses.append(access)
        self._active_readers += 1
        return reader_cls(self, access)

    def stream_shared(self, *, label: str = "shared") -> BodyReader:
        """共享缓存模式：流式读取，内容入缓存供他人重读。"""
        return self.acquire(BodyReadMode.SHARED_CACHE, label=label)

    def open_exclusive(self, *, label: str = "exclusive") -> BodyReader:
        """独占流模式：直读源头，不缓存，仅允许一个消费者。"""
        return self.acquire(BodyReadMode.EXCLUSIVE_STREAM, label=label)

    async def read_all(self, *, label: str = "read_all") -> bytes:
        """共享缓存模式：读取完整请求体，可重复调用。"""
        reader = self.acquire(BodyReadMode.SHARED_CACHE, label=label)
        data = b"".join([chunk async for chunk in reader])
        self._publish_body(data)
        return data

    async def replay(self, *, label: str = "replay") -> bytes:
        """受限重放模式：读取完整请求体，总次数受限。"""
        reader = self.acquire(BodyReadMode.LIMITED_REPLAY, label=label)
        data = b"".join([chunk async for chunk in reader])
        self._publish_body(data)
        return data

    # ------------------------------------------------------------------ #
    # 资源回收
    # ------------------------------------------------------------------ #

    async def aclose(self, reason: str = "completed") -> None:
        """回收全部资源（幂等）。

        取消、超时、部分读取和客户端断开后由协议层调用；
        关闭后任何读取尝试都会抛出 :class:`BodyContractViolation`。
        """
        if self._state is BodyContractState.CLOSED:
            return
        self._close_reason = reason
        try:
            self._spool.close()
        except Exception:  # 清理路径绝不向上抛错
            logger.exception("关闭请求体缓存时出错")
        for access in self._accesses:
            access.finish()
        self._active_readers = 0
        self._state = BodyContractState.CLOSED
        logger.debug(f"请求体契约已关闭（{reason}）：{self!r}")

    # ------------------------------------------------------------------ #
    # 内部实现
    # ------------------------------------------------------------------ #

    def _ensure_open(self) -> None:
        if self._state is BodyContractState.CLOSED:
            raise BodyContractViolation(
                f"请求体契约已关闭（{self._close_reason}），无法读取"
            )

    def _ensure_cache_available(self) -> None:
        if self._cache_wiped:
            raise BodyContractViolation("请求体缓存已在重放上限后清除")
        if self._state is BodyContractState.STREAMING:
            raise BodyContractViolation("请求体正被独占流消费")
        if self._state is BodyContractState.EXHAUSTED:
            raise BodyContractViolation("请求体已被独占流耗尽")
        if self._bytes_lost:
            raise BodyContractViolation(
                "部分请求体已被独占流消费，无法还原完整内容"
            )

    def _start_spooling(self) -> None:
        self._spooling = True
        if self._state in (
            BodyContractState.PRISTINE,
            BodyContractState.PARTIAL,
        ):
            self._state = BodyContractState.BUFFERING

    def _publish_body(self, data: bytes) -> None:
        """完整读取后同步到 request.body，保持传统访问方式可用。"""
        if not getattr(self._request, "body", None):
            try:
                self._request.body = data
            except AttributeError:  # pragma: no cover - 防御性
                ...

    async def _read_source_chunk(self) -> bytes | None:
        """从底层流读取下一块；无可用流时以 request.body 为一次性源。"""
        request = self._request
        stream = getattr(request, "stream", None)
        if stream is not None and getattr(stream, "request_body", None):
            read = getattr(stream, "read", None)
            if read is not None:
                chunk = await read()
                return chunk if chunk else None
        # HTTP/3 推送、测试注入等场景：请求体已被外部填充
        if not self._preset_delivered:
            self._preset_delivered = True
            body = getattr(request, "body", None)
            if body:
                return bytes(body)
        return None

    async def _pull_more(self) -> bytes | None:
        """从源头拉取下一块（单航班）；按模式决定是否入缓存。"""
        if self._source_done:
            return None
        async with self._pull_lock:
            if self._source_done:
                return None
            try:
                chunk = await self._read_source_chunk()
            except BaseException:
                # 取消/断连/协议错误：源头位置不再可信，标记失败，
                # 让后续读取者得到明确错误而不是静默的截断数据
                self._source_failed = True
                if self._state is not BodyContractState.CLOSED:
                    self._state = BodyContractState.PARTIAL
                raise
            if not chunk:
                self._source_eof = True
                self._on_source_eof()
                return None
            self._bytes_received += len(chunk)
            self._digest.update(chunk)
            if self._spooling:
                self._spool.write(chunk)
            else:
                self._bytes_lost += len(chunk)
            return chunk

    @property
    def _source_done(self) -> bool:
        return self._source_eof or self._source_failed

    def _on_source_eof(self) -> None:
        if self._state is BodyContractState.STREAMING:
            self._state = BodyContractState.EXHAUSTED
        elif self._spooling:
            self._state = BodyContractState.CACHED

    def _spool_read(self, offset: int) -> bytes:
        return self._spool.read(offset)

    def _reader_released(self, access: BodyAccess) -> None:
        access.finish()
        self._active_readers -= 1
        if (
            access.mode is BodyReadMode.LIMITED_REPLAY
            and access.completed
            and self._limited_acquires >= 1 + self._replay_limit
            and self._active_readers == 0
        ):
            # 受限重放：最后一次允许的重放完成后立即清除缓存
            self._wipe_cache()
        elif (
            not access.completed
            and not self._source_done
            and self._active_readers == 0
            and self._state is not BodyContractState.CLOSED
        ):
            self._state = BodyContractState.PARTIAL

    def _wipe_cache(self) -> None:
        self._spool.clear()
        self._cache_wiped = True
        if self._state is not BodyContractState.CLOSED:
            self._state = BodyContractState.EXHAUSTED


class BodyReader(AsyncIterator[bytes]):
    """契约读取句柄：异步迭代器 + 异步上下文管理器。

    读到源头末尾时自动释放；提前放弃时应使用 ``async with`` 或
    显式调用 :meth:`release`，以便契约准确记录部分读取。
    """

    def __init__(self, contract: BodyContract, access: BodyAccess) -> None:
        self._contract = contract
        self._access = access
        self._released = False

    @property
    def access(self) -> BodyAccess:
        """本次读取的追踪记录。"""
        return self._access

    @property
    def completed(self) -> bool:
        return self._access.completed

    async def read(self) -> bytes | None:
        """读取下一块；到达末尾返回 None。"""
        raise NotImplementedError

    def __aiter__(self) -> BodyReader:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read()
        if chunk is None:
            raise StopAsyncIteration
        return chunk

    async def __aenter__(self) -> BodyReader:
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        await self.release()
        return False

    async def release(self) -> None:
        """释放句柄（幂等）；未读完时契约会记录为部分读取。"""
        if self._released:
            return
        self._released = True
        self._contract._reader_released(self._access)

    def _finish_read(self) -> None:
        self._access.complete()


class _SharedReader(BodyReader):
    """共享缓存读取者：从缓存游标读取，不足时触发源头拉取。"""

    def __init__(self, contract: BodyContract, access: BodyAccess) -> None:
        super().__init__(contract, access)
        self._pos = 0

    async def read(self) -> bytes | None:
        if self._access.completed:
            return None
        contract = self._contract
        contract._ensure_open()
        while True:
            chunk = contract._spool_read(self._pos)
            if chunk:
                self._pos += len(chunk)
                self._access.bytes_read += len(chunk)
                return chunk
            if contract._source_failed:
                self._access.finish()
                raise BodyContractViolation(
                    "请求体源头读取失败，缓存内容不完整"
                )
            if contract._source_eof:
                self._finish_read()
                await self.release()
                return None
            await contract._pull_more()


class _ExclusiveReader(BodyReader):
    """独占流读取者：直读源头，不写入缓存。"""

    async def read(self) -> bytes | None:
        if self._access.completed:
            return None
        contract = self._contract
        contract._ensure_open()
        chunk = await contract._pull_more()
        if chunk is None:
            self._finish_read()
            await self.release()
            return None
        self._access.bytes_read += len(chunk)
        return chunk
