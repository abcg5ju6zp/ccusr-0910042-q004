import asyncio
import hashlib
import logging
import time

from collections import deque
from types import SimpleNamespace

import pytest

from sanic.exceptions import BodyContractViolation, BodyReplayExhausted
from sanic.request.body import (
    BodyContract,
    BodyContractState,
    SpoolBuffer,
)
from sanic.response import json, text


# --------------------------------------------------------------------- #
# 单元测试辅助
# --------------------------------------------------------------------- #


class FakeStream:
    """模拟协议层流：逐块返回，读完后 request_body 置空。"""

    def __init__(self, chunks, fail_at=None):
        self._chunks = deque(chunks)
        self.request_body = bool(chunks)
        self._fail_at = fail_at  # 在第 N 次 read 时抛出异常
        self._reads = 0

    async def read(self):
        self._reads += 1
        if self._fail_at is not None and self._reads == self._fail_at:
            raise asyncio.CancelledError()
        if not self._chunks:
            self.request_body = None
            return None
        return self._chunks.popleft()


def make_request(
    chunks=(), body=b"", threshold=8, replay_limit=1, fail_at=None
):
    config = SimpleNamespace(
        REQUEST_BODY_MEMORY_THRESHOLD=threshold,
        REQUEST_BODY_REPLAY_LIMIT=replay_limit,
    )
    return SimpleNamespace(
        app=SimpleNamespace(config=config),
        stream=FakeStream(chunks, fail_at=fail_at) if chunks else None,
        body=body,
    )


# --------------------------------------------------------------------- #
# SpoolBuffer：内存阈值与临时存储
# --------------------------------------------------------------------- #


def test_spool_buffer_stays_in_memory_under_threshold():
    spool = SpoolBuffer(threshold=16)
    spool.write(b"abc")
    spool.write(b"def")
    assert not spool.spilled
    assert spool.size == 6
    assert spool.read_all() == b"abcdef"
    assert spool.read(3) == b"def"
    spool.close()
    assert spool.closed


def test_spool_buffer_spills_to_temp_storage():
    spool = SpoolBuffer(threshold=8)
    spool.write(b"aaaa")
    assert not spool.spilled
    spool.write(b"bbbb")  # 8 字节，恰好不溢出
    assert not spool.spilled
    spool.write(b"cccc")  # 超过阈值，转入临时存储
    assert spool.spilled
    assert spool.size == 12
    # 溢出后内容完整可读，且可重复读
    assert spool.read_all() == b"aaaabbbbcccc"
    assert spool.read(4) == b"bbbbcccc"
    spool.write(b"dd")  # 溢出后继续追加
    assert spool.read_all() == b"aaaabbbbccccdd"
    spool.close()
    assert spool.closed
    assert spool.read_all() == b""


def test_spool_buffer_clear_releases_temp_file():
    spool = SpoolBuffer(threshold=2)
    spool.write(b"abcdef")
    assert spool.spilled
    spool.clear()
    assert not spool.spilled
    assert spool.size == 0
    # clear 后仍可复用
    spool.write(b"xy")
    assert spool.read_all() == b"xy"
    spool.close()


def test_spool_buffer_write_after_close_raises():
    spool = SpoolBuffer(threshold=2)
    spool.close()
    with pytest.raises(BodyContractViolation):
        spool.write(b"x")


# --------------------------------------------------------------------- #
# 共享缓存模式
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_shared_cache_multiple_readers():
    contract = BodyContract(make_request(chunks=[b"aaa", b"bbb", b"ccc"]))
    first = await contract.read_all(label="audit")
    second = await contract.read_all(label="handler")
    assert first == second == b"aaabbbccc"
    assert contract.bytes_received == 9
    assert contract.state is BodyContractState.CACHED
    assert contract.was_consumed
    # 两次获取都被追踪
    assert [a.label for a in contract.accesses] == ["audit", "handler"]
    assert all(a.completed for a in contract.accesses)


@pytest.mark.asyncio
async def test_shared_stream_reader_resume_after_partial():
    """部分读取后，后续读取者从缓存+源头继续获得完整内容。"""
    contract = BodyContract(make_request(chunks=[b"aa", b"bb", b"cc"]))
    reader = contract.stream_shared(label="middleware")
    assert await reader.read() == b"aa"
    await reader.release()  # 中间件只读了第一块就放弃
    assert contract.state is BodyContractState.PARTIAL
    assert not contract.was_consumed

    # 处理器随后读取，仍能拿到完整内容
    data = await contract.read_all(label="handler")
    assert data == b"aabbcc"
    assert contract.state is BodyContractState.CACHED


@pytest.mark.asyncio
async def test_shared_concurrent_readers():
    contract = BodyContract(make_request(chunks=[b"x" * 4] * 6, threshold=8))

    async def drain(label):
        return await contract.read_all(label=label)

    first, second = await asyncio.gather(drain("a"), drain("b"))
    assert first == second == b"x" * 24
    assert contract.bytes_received == 24


# --------------------------------------------------------------------- #
# 独占流模式
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_exclusive_stream_passthrough_without_cache():
    contract = BodyContract(make_request(chunks=[b"aaa", b"bbb"]))
    reader = contract.open_exclusive(label="handler")
    chunks = [chunk async for chunk in reader]
    assert chunks == [b"aaa", b"bbb"]
    assert contract.state is BodyContractState.EXHAUSTED
    assert contract.was_consumed
    assert contract.bytes_received == 6
    assert contract.bytes_cached == 0  # 独占流不缓存
    assert contract.digest == hashlib.sha256(b"aaabbb").hexdigest()
    assert contract.digest_complete


@pytest.mark.asyncio
async def test_exclusive_stream_conflicts_with_other_modes():
    contract = BodyContract(make_request(chunks=[b"aaa"]))
    contract.open_exclusive(label="handler")
    # 独占期间，共享与重放都被拒绝
    with pytest.raises(BodyContractViolation):
        contract.stream_shared(label="audit")
    with pytest.raises(BodyContractViolation):
        await contract.replay(label="audit")


@pytest.mark.asyncio
async def test_exclusive_requires_pristine():
    contract = BodyContract(make_request(chunks=[b"aaa"]))
    await contract.read_all(label="audit")
    with pytest.raises(BodyContractViolation):
        contract.open_exclusive(label="handler")


@pytest.mark.asyncio
async def test_exclusive_early_release_marks_partial():
    contract = BodyContract(make_request(chunks=[b"aa", b"bb", b"cc"]))
    async with contract.open_exclusive(label="handler") as reader:
        assert await reader.read() == b"aa"
        # 提前退出，未读完
    assert contract.state is BodyContractState.PARTIAL
    assert not contract.was_consumed
    # 独占流部分读取后内容不可恢复，后续读取被拒绝
    with pytest.raises(BodyContractViolation):
        await contract.read_all(label="late")


# --------------------------------------------------------------------- #
# 受限重放模式
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_limited_replay_within_limit():
    contract = BodyContract(
        make_request(chunks=[b"aaa", b"bbb"], replay_limit=1)
    )
    first = await contract.replay(label="audit")
    assert first == b"aaabbb"
    assert contract.replays_remaining == 1
    second = await contract.replay(label="handler")
    assert second == b"aaabbb"


@pytest.mark.asyncio
async def test_limited_replay_exhaustion_wipes_cache():
    contract = BodyContract(
        make_request(chunks=[b"aaa", b"bbb"], replay_limit=1)
    )
    await contract.replay(label="audit")
    await contract.replay(label="handler")
    # 达到上限后缓存被清除
    assert contract.bytes_cached == 0
    assert contract.state is BodyContractState.EXHAUSTED
    # 再次重放与共享读取都被拒绝
    with pytest.raises(BodyReplayExhausted):
        await contract.replay(label="intruder")
    with pytest.raises(BodyContractViolation):
        await contract.read_all(label="intruder")


@pytest.mark.asyncio
async def test_limited_replay_zero_limit():
    contract = BodyContract(make_request(chunks=[b"aaa"], replay_limit=0))
    assert await contract.replay(label="only") == b"aaa"
    with pytest.raises(BodyReplayExhausted):
        await contract.replay(label="again")


# --------------------------------------------------------------------- #
# 取消/失败与资源回收
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_source_failure_marks_partial_and_blocks_silent_truncation():
    contract = BodyContract(
        make_request(chunks=[b"aa", b"bb", b"cc"], fail_at=2)
    )
    reader = contract.stream_shared(label="handler")
    assert await reader.read() == b"aa"
    with pytest.raises(asyncio.CancelledError):
        await reader.read()
    assert contract.state is BodyContractState.PARTIAL
    assert contract.source_failed
    assert not contract.was_consumed
    # 后续读取者得到明确错误，而不是静默的截断数据
    with pytest.raises(BodyContractViolation):
        await contract.read_all(label="recovery")


@pytest.mark.asyncio
async def test_aclose_reclaims_resources_and_is_idempotent():
    contract = BodyContract(make_request(chunks=[b"x" * 32], threshold=4))
    await contract.read_all(label="handler")
    assert contract.spilled  # 小阈值下已溢出到临时存储
    await contract.aclose("completed")
    assert contract.state is BodyContractState.CLOSED
    assert contract.close_reason == "completed"
    assert contract.bytes_cached == 0
    # 幂等
    await contract.aclose("completed")
    # 关闭后读取被拒绝
    with pytest.raises(BodyContractViolation):
        await contract.read_all(label="late")


@pytest.mark.asyncio
async def test_aclose_finalizes_open_accesses():
    contract = BodyContract(make_request(chunks=[b"aa", b"bb"]))
    reader = contract.stream_shared(label="middleware")
    await reader.read()
    await contract.aclose("cancelled")
    access = contract.accesses[0]
    assert access.ended_at is not None
    assert not access.completed


# --------------------------------------------------------------------- #
# 安全日志与摘要
# --------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_safe_logging_never_contains_content():
    secret = b"super-secret-upload-content"
    contract = BodyContract(make_request(chunks=[secret]))
    await contract.read_all(label="audit")

    rep = repr(contract)
    safe = contract.log_safe()
    assert secret.decode() not in rep
    assert secret.decode() not in str(safe)
    # 只包含长度与摘要等安全信息
    expected = hashlib.sha256(secret).hexdigest()
    assert contract.digest == expected
    assert safe["body_sha256"] == expected
    assert safe["body_bytes_received"] == len(secret)
    assert safe["body_digest_complete"] is True
    assert safe["body_consumers"] == ["audit"]
    assert f"received={len(secret)}" in rep


@pytest.mark.asyncio
async def test_preset_body_source_without_stream():
    """HTTP/3 推送等场景：request.body 已被外部填充，无可用流。"""
    contract = BodyContract(make_request(body=b"pushed-body"))
    assert await contract.read_all(label="handler") == b"pushed-body"
    assert contract.was_consumed
    assert contract.digest == hashlib.sha256(b"pushed-body").hexdigest()


# --------------------------------------------------------------------- #
# 集成：中间件 / 处理器 / 异常路径
# --------------------------------------------------------------------- #


def test_audit_middleware_then_streaming_handler_gets_full_body(app):
    """审计中间件提前读取计算摘要，流式处理器仍获得完整上传内容。"""
    payload = "audit-me" * 1024

    @app.middleware("request")
    async def audit(request):
        if request.path == "/upload":
            await request.body_contract.read_all(label="audit")
            request.ctx.digest = request.body_contract.digest

    @app.post("/upload", stream=True)
    async def upload(request):
        # 中间件读完后，处理器通过契约依然拿到完整内容
        body = await request.body_contract.read_all(label="handler")
        # 传统 request.body 访问方式同样可用
        assert request.body == body
        return json(
            {
                "length": len(body),
                "digest": request.ctx.digest,
                "matches": body.decode() == payload,
            }
        )

    _, response = app.test_client.post("/upload", data=payload)
    assert response.status == 200
    assert response.json["length"] == len(payload)
    assert response.json["matches"] is True
    assert (
        response.json["digest"] == hashlib.sha256(payload.encode()).hexdigest()
    )


def test_middleware_and_handler_share_body_on_regular_route(app):
    @app.middleware("request")
    async def audit(request):
        if request.path == "/form":
            await request.body_contract.read_all(label="audit")

    @app.post("/form")
    async def form_handler(request):
        return json({"length": len(request.body), "form": request.form})

    _, response = app.test_client.post(
        "/form",
        data="field=value",
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert response.status == 200
    assert response.json["length"] == len("field=value")
    assert response.json["form"] == {"field": ["value"]}


def test_exception_handler_can_inspect_consumption_state(app):
    """异常路径能够说明内容是否已被消费、被谁消费。"""
    from sanic.exceptions import ServerError

    @app.post("/stream", stream=True)
    async def stream_handler(request):
        reader = request.body_contract.open_exclusive(label="handler")
        await reader.read()  # 只读一块就出错
        raise ServerError("boom")

    @app.exception(ServerError)
    async def on_error(request, exc):
        contract = request.body_contract
        return json(
            {
                "state": contract.state.value,
                "consumed": contract.was_consumed,
                "received": contract.bytes_received,
                "consumers": [a.label for a in contract.accesses],
            }
        )

    _, response = app.test_client.post("/stream", data="x" * 4096)
    assert response.status == 200
    assert response.json["state"] == "streaming"
    assert response.json["consumed"] is False
    assert response.json["received"] > 0
    assert response.json["consumers"] == ["handler"]


def test_contract_reclaimed_after_request(app):
    """请求完成后契约资源被协议层回收。"""
    app.config.REQUEST_BODY_MEMORY_THRESHOLD = 16

    @app.post("/upload", stream=True)
    async def upload(request):
        body = await request.body_contract.read_all(label="handler")
        app.ctx.contract = request.body_contract
        app.ctx.spilled = request.body_contract.spilled  # 关闭前捕获
        return text(f"len={len(body)}")

    _, response = app.test_client.post("/upload", data="y" * 4096)
    assert response.status == 200
    assert response.text == "len=4096"

    contract = app.ctx.contract
    assert app.ctx.spilled  # 小阈值下已安全转入临时存储
    for _ in range(200):
        if contract.state is BodyContractState.CLOSED:
            break
        time.sleep(0.01)
    assert contract.state is BodyContractState.CLOSED
    assert contract.close_reason == "completed"
    assert contract.bytes_cached == 0  # 临时存储已回收


def test_replay_limit_from_config(app):
    app.config.REQUEST_BODY_REPLAY_LIMIT = 1

    @app.post("/replay", stream=True)
    async def replay_handler(request):
        contract = request.body_contract
        first = await contract.replay(label="audit")
        second = await contract.replay(label="handler")
        try:
            await contract.replay(label="intruder")
        except BodyReplayExhausted:
            return json({"ok": True, "length": len(first + second)})
        return json({"ok": False})

    _, response = app.test_client.post("/replay", data="z" * 128)
    assert response.status == 200
    assert response.json == {"ok": True, "length": 256}


def test_unconsumed_body_log_contains_only_safe_info(app, caplog):
    """未消费请求体的日志只记录长度与摘要，不记录内容。"""
    secret = "s3cr3t-payload-content"

    @app.post("/stream", stream=True)
    async def stream_handler(request):
        reader = request.body_contract.stream_shared(label="handler")
        await reader.read()  # 只读一部分
        return text("ok")

    with caplog.at_level(logging.ERROR, logger="sanic.error"):
        _, response = app.test_client.post("/stream", data=secret * 8)
    assert response.status == 200
    assert "body not consumed" in caplog.text
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_contract_reclaimed_on_client_disconnect(app):
    """客户端断开后契约资源被回收，关闭原因可追踪。"""

    @app.post("/post", stream=True)
    async def post(request):
        app.ctx.contract = request.body_contract
        await asyncio.sleep(1.0)
        return text("unreachable")

    loop = asyncio.get_event_loop()
    task = loop.create_task(app.asgi_client.post("/post", data="chunk"))
    await asyncio.sleep(0.5)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(0.5)

    contract = app.ctx.contract
    assert contract.state is BodyContractState.CLOSED
    assert contract.close_reason == "cancelled"
