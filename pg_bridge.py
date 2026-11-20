"""Page 级 PG 控制器：确认 Preview、启动固定探测；认证和业务流均留在浏览器。"""

import asyncio
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit


_PREVIEW_SELECTOR = 'iframe[title="Preview"]'
_BINDING_NAME = '__pgBootstrapSigner'
_PG_LOAD_GRACE = 30
_PG_RECOVERY_COOLDOWN = 60
_PG_RECOVERY_WINDOW = 15 * 60
_PG_RECOVERY_LIMIT = 2
_PG_SIGN_FAILURE_LIMIT = 2
_BOOTSTRAP_RECOVERABLE = frozenset({
    'PG_SIGNER_NOT_FOUND', 'PG_BOOTSTRAP_FAILED', 'PG_BOOTSTRAP_TIMEOUT',
    'PG_PREVIEW_UNAVAILABLE',
})


@dataclass
class PgState:
    """由 InstanceRecord 持有；页面/Context 重建不清零使用标记和恢复预算。"""
    enabled: bool = False
    health: dict = field(default_factory=lambda: {'phase': 'not_used', 'sampledAt': None})
    recoveries: list[tuple[float, str]] = field(default_factory=list)

    def snapshot(self):
        now = time.monotonic()
        sampled = self.health.get('sampledAt')
        return {
            **self.health,
            'enabled': self.enabled,
            'recoveriesInWindow': sum(now - at < _PG_RECOVERY_WINDOW
                                      for at, _ in self.recoveries),
            'sampleAgeSeconds': (max(0, (time.time() * 1000 - sampled) / 1000)
                                 if sampled is not None else None),
        }


class PgBridge:
    def __init__(self, page, target_url, logger, state):
        self.page = page
        self.logger = logger
        self.state = state
        self.state.health = {'phase': 'starting', 'sampledAt': None}
        self._reported = None
        self._target_url = target_url
        self._target_path = ''
        self._installed = False
        self._closed = False
        self._binding = None
        self._refresh_task = None
        self._refresh_again = False
        self._probe_task = None
        self._listeners = []

    def _on_target(self):
        if self.page.is_closed():
            return False
        url = urlsplit(self.page.url)
        return (url.scheme == 'https' and url.hostname == 'aistudio.google.com'
                and url.port in (None, 443)
                and url.path.rstrip('/') == self._target_path)

    def _current(self, binding):
        return (not self._closed and self._installed and binding is not None
                and self._binding is binding and not binding['frame'].is_detached()
                and self._on_target())

    def _task_done(self, task):
        if not task.cancelled() and task.exception():
            self.logger.warning(f'PG 后台操作失败: {type(task.exception()).__name__}')

    async def install(self):
        """仅在 Worker 新建的 about:blank Page 上、首次导航之前调用。"""
        if self._installed:
            return
        if self._closed or self.page.url != 'about:blank':
            raise RuntimeError('PG must be installed before the first Page navigation')
        target = urlsplit(self._target_url)
        if (target.scheme != 'https' or target.hostname != 'aistudio.google.com'
                or target.port not in (None, 443) or target.username or target.password
                or not target.path.startswith('/apps/') or not target.path.rstrip('/')[6:]):
            raise ValueError('PG requires an AI Studio Build application URL')
        self._target_path = target.path.rstrip('/')
        source = Path(__file__).with_suffix('.js').read_text(encoding='utf-8')
        script = source.replace('__PG_TARGET_PATH__', json.dumps(self._target_path))
        async with asyncio.timeout(5):
            await self.page.expose_binding(_BINDING_NAME, self._bootstrap_signer)
            await self.page.expose_binding('__pgMarkUsed', self._mark_used)
            await self.page.add_init_script(script=script)
            for event, callback in (
                ('framenavigated', self._on_frame_event),
                ('framedetached', self._on_frame_event),
                ('domcontentloaded', self._schedule_refresh),
                ('close', self._schedule_refresh),
            ):
                self.page.on(event, callback)
                self._listeners.append((event, callback))
        self._installed = True

    def _on_frame_event(self, frame):
        # 导航只触发确认，不等于失效。同文档导航继续使用原文档句柄和 session。
        if (frame is self.page.main_frame
                or (self._binding and frame is self._binding['frame'])
                or (self._binding is None and frame.parent_frame is self.page.main_frame)):
            self._schedule_refresh()

    def _schedule_refresh(self, *_):
        if self._closed or not self._installed:
            return
        self._refresh_again = True
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self._refresh_loop())
            self._refresh_task.add_done_callback(self._task_done)

    async def _refresh_loop(self):
        # 合并事件，不取消正在执行的浏览器命令；失败只额外确认一次，不重发业务请求。
        retry = True
        while self._refresh_again and not self._closed:
            self._refresh_again = False
            try:
                async with asyncio.timeout(5):
                    await self._refresh()
            except Exception as error:
                self.logger.warning(f'PG Preview 绑定失败: {type(error).__name__}')
                await self._clear_binding()
                if retry and not self._closed and not self.page.is_closed():
                    retry = False
                    self._refresh_again = True
                    await asyncio.sleep(0.2)

    async def _refresh(self):
        binding = self._binding
        if self._current(binding):
            try:
                if (await binding['lease'].evaluate('lease => lease.current()')
                        and await binding['preview'].evaluate('preview => preview.alive')):
                    return
            except Exception:
                pass  # 真正的文档导航会销毁句柄；不能把 Frame/WindowProxy 当成文档身份。
        await self._clear_binding()
        if self._closed or not self._on_target():
            return

        handles = []
        try:
            elements = await self.page.query_selector_all(_PREVIEW_SELECTOR)
            handles.extend(elements)
            if len(elements) != 1:
                return
            frame = await elements[0].content_frame()
            if not frame or frame.is_detached() or frame.parent_frame is not self.page.main_frame:
                return
            preview = await frame.evaluate_handle('() => window.__pgPreviewBridge')
            handles.append(preview)
            info = await preview.evaluate(
                'p => p?.alive ? {session: p.session, origin: p.origin} : null'
            )
            if not info:
                if urlsplit(frame.url).scheme == 'https':
                    raise RuntimeError('PG Preview document helper is unavailable')
                return
            if not info['origin'].startswith('https://'):
                return
            lease = await self.page.evaluate_handle(
                '({element, origin, session}) => '
                'window.__pgHostBridge?.prepare(element, origin, session)',
                {'element': elements[0], **info},
            )
            handles.append(lease)
            if not await lease.evaluate('lease => !!lease'):
                raise RuntimeError('PG host did not prepare the Preview binding')
            if self._closed or not await preview.evaluate('p => p.alive'):
                return

            # 先保存可撤销对象，再提交。任何异常由协调任务回滚；close 负责取消路径。
            self._binding = {'frame': frame, 'preview': preview,
                             'lease': lease, 'session': info['session']}
            handles.remove(preview)
            handles.remove(lease)
            if (not await lease.evaluate('lease => lease.activate()')
                    or not await preview.evaluate('p => p.alive')):
                raise RuntimeError('PG Preview binding was not committed')
        finally:
            await self._release_handles(handles)

    @staticmethod
    async def _release_handles(handles):
        if handles:
            try:
                async with asyncio.timeout(0.5):
                    await asyncio.gather(*(handle.dispose() for handle in handles),
                                         return_exceptions=True)
            except TimeoutError:
                pass

    async def _clear_binding(self):
        binding, self._binding = self._binding, None
        probe = self._probe_task
        if probe and not probe.done():
            probe.cancel()
        try:
            if binding:
                try:
                    async with asyncio.timeout(0.5):
                        await binding['lease'].evaluate('lease => lease.cancel()')
                except TimeoutError:
                    self.logger.warning('PG 绑定撤销超时；不复用该绑定')
                except Exception:
                    pass  # 旧宿主文档销毁后，不能也不需要向新文档撤销旧绑定。
            if probe and not probe.done():
                async with asyncio.timeout(0.75):
                    await asyncio.gather(probe, return_exceptions=True)
        except TimeoutError:
            self.logger.warning('PG 探测清理超时；继续撤销绑定')
        finally:
            if binding:
                await self._release_handles([binding['preview'], binding['lease']])

    def _trusted_host(self, source, session):
        binding = self._binding
        return (self._current(binding)
                and source.get('context') is self.page.context
                and source.get('page') is self.page
                and source.get('frame') is self.page.main_frame
                and session == binding['session'])

    def _mark_used(self, source, session):
        if not self._trusted_host(source, session):
            return False
        if not self.state.enabled:
            self.state.enabled = True
            self.logger.info('首次 PG 请求：启用 PG 健康监测和页面恢复后预热')
        return True

    async def _bootstrap_signer(self, source, session, remaining_ms):
        binding = self._binding
        if (not self._trusted_host(source, session)
                or type(remaining_ms) not in (int, float)
                or not math.isfinite(remaining_ms) or not 0 < remaining_ms <= 25000):
            return {'ok': False, 'code': 'PG_PREVIEW_UNAVAILABLE'}
        if self._probe_task and not self._probe_task.done():
            return {'ok': False, 'code': 'PG_BOOTSTRAP_FAILED'}

        task = asyncio.create_task(self._run_probe(binding, remaining_ms / 1000))
        self._probe_task = task
        try:
            return await task
        except asyncio.CancelledError:
            return {'ok': False, 'code': 'PG_PREVIEW_UNAVAILABLE'}
        except TimeoutError:
            return {'ok': False, 'code': 'PG_BOOTSTRAP_TIMEOUT'}
        except Exception as error:
            self.logger.warning(f'PG 初始化辅助失败: {type(error).__name__}')
            return {'ok': False, 'code': 'PG_BOOTSTRAP_FAILED'}
        finally:
            if self._probe_task is task:
                self._probe_task = None

    async def _run_probe(self, binding, budget):
        preview = binding['preview']
        deadline = asyncio.get_running_loop().time() + budget
        try:
            async with asyncio.timeout(budget):
                if (not await binding['lease'].evaluate('lease => lease.current()')
                        or not self._current(binding)):
                    return {'ok': False, 'code': 'PG_PREVIEW_UNAVAILABLE'}
                remaining_ms = max(0, (deadline - asyncio.get_running_loop().time()) * 1000)
                # 只等待一次 Promise：就绪等待与探测取消在所属文档内完成。
                return await preview.evaluate('(p, ms) => p.startProbe(ms)', remaining_ms)
        except BaseException:
            # 取消 Python 等待不会撤销已经发送的 evaluate；必须显式停止文档内探测。
            try:
                async with asyncio.timeout(0.5):
                    await preview.evaluate('p => p.stopProbe()')
            except Exception:
                pass
            raise

    def _publish_health(self, data, phase, reason=None, retry_after=0):
        self.state.health = {
            **data, 'phase': phase, 'recoveryReason': reason,
            'retryAfterSeconds': max(0, round(retry_after)),
            'sampledAt': round(time.time() * 1000),
        }
        report = (phase, reason, data.get('lastErrorCode'), data.get('lastHttpStatus'))
        if self.state.enabled and report != self._reported:
            log = (self.logger.warning if phase in ('paused', 'auth_required', 'unknown', 'degraded')
                   else self.logger.debug)
            log(f'PG 状态: {phase}; 原因: {reason or "-"}; '
                f'最近错误: {data.get("lastErrorCode") or "-"}; '
                f'最近 HTTP: {data.get("lastHttpStatus") or "-"}')
        self._reported = report

    @staticmethod
    def _recovery_reason(data):
        if not data['bound']:
            return 'PG_PREVIEW_UNAVAILABLE'
        if data['bootstrapErrorCode'] in _BOOTSTRAP_RECOVERABLE:
            return data['bootstrapErrorCode']
        if data['signFailures'] >= _PG_SIGN_FAILURE_LIMIT:
            return 'PG_SIGN_FAILED'
        return None

    async def maintain(self, can_act):
        """仅由现有保活循环调用；返回是否尝试了刷新，不重放任何业务请求。"""
        if self._closed or not self._installed:
            return False
        if self._binding is None:
            self._schedule_refresh()  # 仅修复缺失绑定，不反复检查健康文档句柄。
        host = None
        gate_touched = False
        keep_gate = False
        try:
            async with asyncio.timeout(3):
                host = await self.page.evaluate_handle('() => window.__pgHostBridge')
                data = await host.evaluate('host => host?.status()')
            if not data or not self._on_target():
                self._publish_health({}, 'unknown', 'PG_HOST_UNAVAILABLE')
                return False
            gate_touched = data['recovering']
            if not data['bound']:
                self._schedule_refresh()
            self.state.enabled = self.state.enabled or data['used']
            if not self.state.enabled:
                self._publish_health(data, 'not_used')
                return False
            if not can_act():
                self._publish_health(data, 'deferred')
                return False
            if data['settledForMs'] < _PG_LOAD_GRACE * 1000:
                self._publish_health(data, 'loading')
                return False
            if not data['authMaterialAvailable']:
                self._publish_health(data, 'auth_required', 'PG_AUTH_UNAVAILABLE')
                return False

            if data['bound'] and data['signerState'] == 'uninitialized':
                async with asyncio.timeout(3):
                    data = await host.evaluate(
                        '(host, grace) => { host.setRecovering(false); '
                        'host.warmup(grace); return host.status(); }', _PG_LOAD_GRACE * 1000,
                    )
            if data['signerState'] == 'initializing':
                self._publish_health(data, 'initializing')
                return False

            reason = self._recovery_reason(data)
            if not reason:
                phase = ('ready' if data['signerState'] == 'ready' else 'loading')
                self._publish_health(data, 'degraded' if data['signFailures'] else phase)
                return False
            now = time.monotonic()
            history = self.state.recoveries
            history[:] = [(at, code) for at, code in history if now - at < _PG_RECOVERY_WINDOW]
            resume_at = (history[0][0] + _PG_RECOVERY_WINDOW
                         if len(history) >= _PG_RECOVERY_LIMIT else 0)
            if reason == 'PG_SIGNER_NOT_FOUND':
                # 不认识新构建时，窗口内只刷新一次；按对应记录计算暂停期限。
                resume_at = max([resume_at] + [at + _PG_RECOVERY_WINDOW
                                               for at, code in history if code == reason])
            if resume_at > now:
                self._publish_health(data, 'paused', reason, resume_at - now)
                return False
            if history and now - history[-1][0] < _PG_RECOVERY_COOLDOWN:
                self._publish_health(data, 'cooldown', reason,
                                     history[-1][0] + _PG_RECOVERY_COOLDOWN - now)
                return False

            # 先关闭新 PG 请求准入，再重新检查事实，消除“检查空闲后又进来请求”的竞争。
            gate_touched = True
            async with asyncio.timeout(3):
                data = await host.evaluate('host => host.setRecovering(true)')
            if (not data or not can_act() or not data['authMaterialAvailable']
                    or data['settledForMs'] < _PG_LOAD_GRACE * 1000
                    or self._recovery_reason(data) != reason):
                self._publish_health(data or {}, 'deferred')
                return False
            if data['activeRequests']:
                keep_gate = True
                self._publish_health(data, 'draining', reason)
                return False

            # 先记预算再提交有副作用的命令；结果未知也计入，不能因导航异常无限重发。
            history.append((time.monotonic(), reason))
            self._publish_health(data, 'recovering', reason)
            self.logger.warning('PG 排空完成，刷新宿主页；同页普通 WS 请求可能短暂中断，不重放业务请求')
            try:
                async with asyncio.timeout(3):
                    accepted = await host.evaluate(
                        '(host, grace) => host.reloadIfIdle(grace)', _PG_LOAD_GRACE * 1000,
                    )
            except Exception as error:
                self.logger.warning(f'PG 刷新提交结果未确认: {type(error).__name__}；保留次数预算')
                return True
            if not accepted:
                history.pop()  # 明确拒绝、未导航，不消耗恢复次数。
                self._publish_health(data, 'deferred', reason)
                return False
            keep_gate = True
            return True
        except Exception as error:
            self._publish_health({}, 'unknown', 'PG_STATUS_UNAVAILABLE')
            self.logger.debug(f'PG 维护失败: {type(error).__name__}；不触发账号重建')
            return False
        finally:
            if host is not None:
                if gate_touched and not keep_gate:
                    try:
                        async with asyncio.timeout(0.5):
                            await host.evaluate('host => { host?.setRecovering(false); }')
                        self.state.health['recovering'] = False
                    except Exception:
                        pass  # 只向原文档解除准入限制；不修改导航后的新文档。
                await self._release_handles([host])

    async def close(self):
        """有界清理；无论是否成功，browser.py 都继续尝试关闭所属 Context。"""
        if self._closed:
            return
        self._closed = True
        self._installed = False
        for event, callback in self._listeners:
            self.page.remove_listener(event, callback)
        self._listeners.clear()
        refresh = self._refresh_task
        if refresh and not refresh.done():
            refresh.cancel()
        try:
            async with asyncio.timeout(2):
                if refresh:
                    await asyncio.gather(refresh, return_exceptions=True)
                await self._clear_binding()
        except TimeoutError:
            self.logger.warning('PG 预清理超时，继续关闭 Context')
        finally:
            if self._probe_task and not self._probe_task.done():
                self._probe_task.cancel()
            if not self.page.is_closed():
                try:
                    async with asyncio.timeout(0.5):
                        await self.page.evaluate('() => { window.__pgHostBridge?.dispose(); }')
                except Exception:
                    pass
