# Sanic 服务框架

本项目提供异步 HTTP 服务、路由、蓝图、中间件、信号、后台任务和工作进程管理能力。生产源码位于 `sanic/`，核心回归测试位于 `tests/`。

## 安装

`python3 -m pip install --break-system-packages --no-build-isolation -e '.[test]'`

## 测试

`python3 -m pytest -q tests/test_tasks.py tests/test_blueprints.py tests/test_routes.py`

## 构建

`python3 -m compileall -q sanic`

## 使用

应用通过 `Sanic` 创建服务，可使用本地测试客户端验证请求、响应和生命周期行为。
