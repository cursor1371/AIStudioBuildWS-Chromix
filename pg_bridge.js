// Page.add_init_script：目标宿主页负责认证转发；直接子 frame 仅安装文档会话保护。
((targetPath) => {
    'use strict';

    const HOST_ORIGIN = 'https://aistudio.google.com';
    const PREVIEW_SELECTOR = 'iframe[title="Preview"]';
    const BOOTSTRAP_MS = 25000;
    // 25 秒初始化 + 20 秒业务签名，给 iframe 的 60 秒响应头超时留出余量。
    const SIGN_TIMEOUT_MS = 20000;

    if (window !== window.top) {
        if (window.parent !== window.top || location.protocol !== 'https:' ||
            window.__pgPreviewBridge) return;
        const session = crypto.randomUUID();
        let alive = true;
        let probe = null;

        function stopProbe(code = 'PG_BOOTSTRAP_FAILED') {
            // 即使启动命令尚未到达，也禁止该文档迟到的重复探测。
            if (!probe) { probe = { done: true }; return; }
            if (probe.done) return;
            probe.done = true;
            clearInterval(probe.poll);
            clearTimeout(probe.timer);
            probe.controller.abort();
            probe.resolve({ ok: !code, code });
        }

        function startProbe(remainingMs) {
            if (!alive || probe || remainingMs <= 0) {
                return Promise.resolve({ ok: false, code: 'PG_PREVIEW_UNAVAILABLE' });
            }
            const state = { done: false, started: false, controller: new AbortController() };
            probe = state;
            return new Promise(resolve => {
                state.resolve = resolve;
                const start = () => {
                    if (state.done || state.started || !window.aistudio) return;
                    state.started = true;
                    clearInterval(state.poll);
                    void (async () => {
                        try {
                            const random = crypto.getRandomValues(new Uint32Array(1))[0]
                                .toString(16).padStart(8, '0');
                            const response = await fetch(
                                'https://generativelanguage.googleapis.com/v1beta/models/' +
                                'gemini-pg-waa-probe-' + random + ':streamGenerateContent?alt=sse',
                                {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    signal: state.controller.signal,
                                    body: JSON.stringify({ contents: [{
                                        role: 'user', parts: [{ text: 'PG_WAA_DISCOVERY_PROBE' }]
                                    }] })
                                }
                            );
                            if (response.body) await response.body.cancel();
                        } catch {
                            // 探测的 HTTP/代理错误不决定原生签名是否成功。
                        } finally {
                            stopProbe(null);
                        }
                    })();
                };
                state.timer = setTimeout(() => stopProbe('PG_BOOTSTRAP_TIMEOUT'), remainingMs);
                state.poll = setInterval(start, 100);
                start();
            });
        }

        const hostTypes = new Set(['PG_INIT', 'PG_HEADERS', 'PG_CHUNK', 'PG_END', 'PG_ERROR']);
        window.addEventListener('message', event => {
            if (event.source !== window.parent || event.origin !== HOST_ORIGIN) return;
            const message = event.data;
            if (!message || typeof message !== 'object') return;
            if (message.type === '__PG_STOP_PROBE') {
                if (message.bridgeSessionId === session) stopProbe();
                event.stopImmediatePropagation();
            } else if (hostTypes.has(message.type) &&
                (!alive || message.bridgeSessionId !== session)) {
                // WindowProxy 可跨导航复用；新文档不能接受旧 PG_INIT 或旧响应。
                event.stopImmediatePropagation();
            }
        }, true);
        window.addEventListener('pagehide', () => {
            alive = false;
            stopProbe('PG_PREVIEW_UNAVAILABLE');
            window.parent.postMessage({
                type: '__PG_PREVIEW_GONE', bridgeSessionId: session
            }, HOST_ORIGIN);
        }, { once: true });
        window.__pgPreviewBridge = Object.freeze({
            session, origin: location.origin, get alive() { return alive; },
            startProbe, stopProbe
        });
        return;
    }

    const onTarget = () => location.origin === HOST_ORIGIN &&
        location.pathname.replace(/\/+$/, '') === targetPath;
    if (!onTarget() || window.__pgHostBridge) return;

    // 两个内部 RPC 使用外层 protobuf Content-Type；$ct 保持命名 JSON 正文，alt 固定 JSON 响应。
    // %24ct 是 URL 编码后的 $ct；请求对象不转换为 JSPB 数组。
    const RPC_URL = 'https://alkalimakersuite-pa.clients6.google.com/$rpc/' +
        'google.internal.alkali.applications.makersuite.v1.MakerSuiteService/' +
        'GenerateContent?%24ct=application%2Fjson&alt=json';
    const BENEFIT_RPC_URL = 'https://alkalimakersuite-pa.clients6.google.com/$rpc/' +
        'google.internal.alkali.applications.makersuite.v1.MakerSuiteService/' +
        'GetAiStudioBenefitTier?%24ct=application%2Fjson&alt=json';
    const BENEFIT_TIMEOUT_MS = 5000;
    const BENEFIT_CACHE_MS = 5 * 60 * 1000;
    const BENEFIT_RETRY_MS = 30000;
    const API_KEY = 'AIzaSyDdP816MREB3SkjZO04QXbjsigfcI0GWOs';
    const messages = {
        PG_INVALID_REQUEST: 'Invalid PG bridge request.',
        PG_RECOVERING: 'PG is preparing a page recovery; retry after recovery completes.',
        PG_PREVIEW_UNAVAILABLE: 'PG Preview binding is no longer available.',
        PG_SIGNER_NOT_FOUND: 'Cannot uniquely locate the native PG signer.',
        PG_BOOTSTRAP_FAILED: 'PG signer initialization failed.',
        PG_BOOTSTRAP_TIMEOUT: 'PG signer initialization timed out.',
        PG_AUTH_UNAVAILABLE: 'PG session authentication is unavailable.',
        PG_SIGN_FAILED: 'PG native signing failed.',
        PG_SIGN_TIMEOUT: `PG native signing timed out after ${SIGN_TIMEOUT_MS / 1000} seconds.`,
        PG_FETCH_FAILED: 'PG upstream request failed.',
        PG_READ_FAILED: 'PG upstream response could not be read.'
    };
    class PgError extends Error {
        constructor(code) { super(messages[code]); this.code = code; }
    }

    let closed = false;
    let binding = null;
    let signer = null;
    let bootstrap = null;
    let used = false;
    let recovering = false;
    let changedAt = performance.now();
    let signFailures = 0;
    let lastErrorCode = null;
    let lastErrorAt = null;
    let lastHttpStatus = null;
    let lastTransportCompletedAt = null;
    const requests = new Map();
    // 仅属于当前宿主文档；失败不永久缓存，也不复用已过期的订阅等级。
    const benefit = { tier: null, expiresAt: 0, pending: null, controller: null };
    const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
    const validToken = value => typeof value === 'string' && value.startsWith('!') && value.length > 1;

    function bound(owner) {
        return !closed && onTarget() && owner !== null && binding === owner &&
            !owner.retired && owner.element.isConnected &&
            owner.element.contentWindow === owner.window;
    }

    function active(request) {
        return !request.done && requests.get(request.id) === request && bound(request.owner);
    }

    // 调用方在本次同步调用链中检查绑定；不重复执行相同检查。
    function post(owner, type, fields = {}) {
        try {
            owner.window.postMessage({ ...fields, type, bridgeSessionId: owner.session }, owner.origin);
            return true;
        } catch { return false; }
    }

    function finish(request, type = null, fields = {}) {
        if (request.done) return;
        if (type && active(request)) {
            post(request.owner, type, { ...fields, bridgeRequestId: request.id });
        }
        request.done = true;
        if (requests.get(request.id) === request) requests.delete(request.id);
        if (type !== 'PG_END') {
            request.controller.abort();
            if (request.reader) void request.reader.cancel().catch(() => {});
        }
        request.body = null;
        request.digest = '';
    }

    function send(request, type, fields) {
        if (active(request) && post(request.owner, type, {
            ...fields, bridgeRequestId: request.id
        })) return true;
        finish(request);
        return false;
    }

    function recordFailure(code) {
        lastErrorCode = code;
        lastErrorAt = Date.now();
        if (code === 'PG_SIGN_FAILED' || code === 'PG_SIGN_TIMEOUT') signFailures++;
    }

    function sapisid() {
        const values = document.cookie.split(';').map(pair => pair.trim())
            .filter(pair => pair.startsWith('SAPISID='));
        return values.length === 1 ? values[0].slice(8) : '';
    }

    function status() {
        return {
            used, bound: bound(binding), recovering,
            signerState: signer ? 'ready' : bootstrap
                ? (bootstrap.state === 'pending' ? 'initializing' : 'failed') : 'uninitialized',
            bootstrapErrorCode: bootstrap?.errorCode ?? null,
            signFailures, authMaterialAvailable: !!sapisid(),
            activeRequests: requests.size,
            settledForMs: Math.max(0, performance.now() - changedAt),
            lastErrorCode, lastErrorAt, lastHttpStatus, lastTransportCompletedAt
        };
    }

    function warmup(minSettledMs) {
        if (!bound(binding) || recovering || signer || bootstrap || !sapisid() ||
            performance.now() - changedAt < minSettledMs) return;
        // 只由已启用 PG 的实例调用；与业务请求共享同一个初始化 Promise。
        used = true;
        void ensureSigner(binding).catch(() => {});
    }

    function setRecovering(value) {
        if (closed || !onTarget()) return null;
        recovering = value === true;
        return status();
    }

    function reloadIfIdle(minSettledMs) {
        if (closed || !onTarget() || !recovering || requests.size !== 0 || !sapisid() ||
            performance.now() - changedAt < minSettledMs) return false;
        // 在持有此对象的原宿主文档中执行，不让迟到 Python 命令刷新新的文档。
        location.reload();
        return true;
    }

    function invalidate(owner = binding) {
        if (!owner || owner.retired) return;
        for (const request of requests.values()) {
            if (request.owner === owner) finish(request, 'PG_ERROR', {
                code: 'PG_PREVIEW_UNAVAILABLE', message: messages.PG_PREVIEW_UNAVAILABLE
            });
        }
        owner.retired = true;
        if (binding === owner) {
            binding = null;
            changedAt = performance.now();
        }
        if (bootstrap && bootstrap.owner === owner && bootstrap.state === 'pending') {
            bootstrap.finish('PG_PREVIEW_UNAVAILABLE');
        }
    }

    function prepare(element, origin, session) {
        const unique = () => {
            const frames = document.querySelectorAll(PREVIEW_SELECTOR);
            return frames.length === 1 && frames[0] === element && element.isConnected;
        };
        if (closed || !onTarget() || !unique() || !element.contentWindow ||
            typeof session !== 'string' || !session) return null;
        const url = new URL(origin);
        if (url.protocol !== 'https:' || url.origin !== origin) return null;
        const owner = { element, window: element.contentWindow, origin, session, retired: false };
        // prepare 无授权副作用；Python 持有文档限定的句柄，再提交或撤销本次绑定。
        // 撤销是永久的，迟到的 activate 不能复活它，也不能撤销后来的绑定。
        return Object.freeze({
            activate() {
                if (closed || owner.retired || !onTarget() || !unique() ||
                    element.contentWindow !== owner.window) return false;
                if (binding !== owner) {
                    invalidate();
                    binding = owner;
                    changedAt = performance.now();
                }
                if (post(owner, 'PG_INIT')) return true;
                invalidate(owner);
                return false;
            },
            current: () => bound(owner) && unique(),
            cancel: () => invalidate(owner)
        });
    }

    function ensureSigner(owner) {
        if (signer) return Promise.resolve();
        if (bootstrap) return bootstrap.promise;
        const state = {
            owner, state: 'pending', observed: false, errorCode: null,
            deadline: performance.now() + BOOTSTRAP_MS, promise: null, finish: null
        };
        bootstrap = state;
        state.promise = new Promise((resolve, reject) => {
            let module, key, original, wrapper, interval, timer;
            state.finish = (code = null, captured = null) => {
                if (state.state !== 'pending') return;
                if (!code && !bound(owner)) code = 'PG_PREVIEW_UNAVAILABLE';
                if (!code && performance.now() >= state.deadline) code = 'PG_BOOTSTRAP_TIMEOUT';
                state.state = code ? 'failed' : 'ready';
                clearInterval(interval);
                clearTimeout(timer);
                if (module && module[key] === wrapper) module[key] = original;
                // 事件通知探测退出；Python 不再轮询签名状态。
                post(owner, '__PG_STOP_PROBE');
                if (code) {
                    // 每次初始化只记录一次，不按共享 Promise 的业务等待者重复计数。
                    state.errorCode = code;
                    recordFailure(code);
                    reject(new PgError(code));
                } else {
                    signer = captured;
                    signFailures = 0;
                    resolve();
                }
            };

            const discover = () => {
                if (state.state !== 'pending') return;
                try {
                    if (!bound(owner)) { state.finish('PG_PREVIEW_UNAVAILABLE'); return; }
                    const ms = window.default_MakerSuite;
                    if (!ms || Object.keys(ms).length <= 100) return;
                    const keys = Object.keys(ms).filter(name => {
                        if (typeof ms[name] !== 'function') return false;
                        const source = Function.prototype.toString.call(ms[name]);
                        return source.includes('.snapshot({') && source.includes('content');
                    });
                    if (keys.length !== 1) { state.finish('PG_SIGNER_NOT_FOUND'); return; }
                    clearInterval(interval);
                    module = ms;
                    key = keys[0];
                    original = ms[key];
                    wrapper = function (...args) {
                        const observe = state.state === 'pending' && !state.observed;
                        if (observe) state.observed = true;
                        let result;
                        try { result = Reflect.apply(original, this, args); }
                        catch (error) {
                            if (observe) state.finish('PG_BOOTSTRAP_FAILED');
                            throw error;
                        }
                        if (observe) {
                            const captured = { fn: original, receiver: this, service: args[0] };
                            void Promise.resolve(result).then(
                                token => state.finish(captured.service && validToken(token)
                                    ? null : 'PG_BOOTSTRAP_FAILED', captured),
                                () => state.finish('PG_BOOTSTRAP_FAILED')
                            );
                        }
                        return result;
                    };
                    ms[key] = wrapper;
                    void Promise.resolve(window.__pgBootstrapSigner(
                        owner.session, Math.max(0, state.deadline - performance.now())
                    )).then(result => {
                        if (state.state !== 'pending') return;
                        if (!result?.ok) {
                            state.finish(Object.hasOwn(messages, result?.code)
                                ? result.code : 'PG_BOOTSTRAP_FAILED');
                        } else if (!state.observed) {
                            state.finish('PG_BOOTSTRAP_FAILED');
                        }
                        // 探测结束且原生签名尚未完成：等待签名结果，不以 404 判失败。
                    }, () => state.finish('PG_BOOTSTRAP_FAILED'));
                } catch { state.finish('PG_BOOTSTRAP_FAILED'); }
            };
            timer = setTimeout(() => state.finish('PG_BOOTSTRAP_TIMEOUT'), BOOTSTRAP_MS);
            interval = setInterval(discover, 100);
            discover();
        });
        return state.promise;
    }

    // 只结束本地等待；Promise.race 接管原生调用的迟到完成/拒绝，不让其继续 run。
    async function signRequest(request) {
        const signal = request.controller.signal;
        let timer, onAbort;
        const interrupted = new Promise((_, reject) => {
            onAbort = () => reject(signal.reason);
            signal.addEventListener('abort', onAbort, { once: true });
            timer = setTimeout(() => reject(new PgError('PG_SIGN_TIMEOUT')), SIGN_TIMEOUT_MS);
        });
        try {
            return await Promise.race([
                interrupted,
                Promise.resolve().then(() => {
                    signal.throwIfAborted();
                    return Reflect.apply(
                        signer.fn, signer.receiver, [signer.service, request.digest]
                    );
                })
            ]);
        } finally {
            clearTimeout(timer);
            signal.removeEventListener('abort', onAbort);
        }
    }

    async function authorization() {
        const cookie = sapisid();
        if (!cookie) throw new PgError('PG_AUTH_UNAVAILABLE');
        const timestamp = Math.floor(Date.now() / 1000);
        const material = `${timestamp} ${cookie} ${HOST_ORIGIN}`;
        const hash = await crypto.subtle.digest('SHA-1', new TextEncoder().encode(material));
        const hex = Array.from(new Uint8Array(hash))
            .map(value => value.toString(16).padStart(2, '0')).join('');
        return `SAPISIDHASH ${timestamp}_${hex}`;
    }

    function subscriptionTier() {
        if (closed || !onTarget()) return Promise.resolve(null);
        if (benefit.pending) return benefit.pending;
        if (performance.now() < benefit.expiresAt) return Promise.resolve(benefit.tier);

        const controller = new AbortController();
        benefit.controller = controller;
        benefit.pending = (async () => {
            let succeeded = false;
            let tier = null;
            const timer = setTimeout(() => controller.abort(), BENEFIT_TIMEOUT_MS);
            try {
                const auth = await authorization();
                controller.signal.throwIfAborted();
                const response = await fetch(BENEFIT_RPC_URL, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json+protobuf',
                        'Authorization': auth,
                        'X-Goog-Api-Key': API_KEY
                    },
                    credentials: 'include', redirect: 'error',
                    signal: controller.signal,
                    body: '{}'
                });
                if (response.ok) {
                    const data = await response.json();
                    controller.signal.throwIfAborted();
                    if (!closed && isObject(data) && !('error' in data)) {
                        succeeded = true;
                        if (data.benefitTier === 'TIER1' || data.benefitTier === 'TIER2') {
                            tier = data.benefitTier;
                        }
                    }
                } else if (response.body) {
                    void response.body.cancel().catch(() => {});
                }
            } catch {
                // 权益查询失败只降级为不带订阅头，不计签名故障，不重试业务请求。
            } finally {
                clearTimeout(timer);
                controller.abort();
                if (!closed) {
                    benefit.tier = tier;
                    benefit.expiresAt = performance.now() +
                        (succeeded ? BENEFIT_CACHE_MS : BENEFIT_RETRY_MS);
                }
                benefit.pending = null;
                benefit.controller = null;
            }
            return closed ? null : tier;
        })();
        // 文档级共享查询不跟随单条请求取消，避免中断其他并发等待者。
        return benefit.pending;
    }

    async function run(request) {
        let stage = 'PG_BOOTSTRAP_FAILED';
        let reader = null;
        try {
            // 查询与初始化并行；查询最多增加 5 秒等待，签名仍在查询完成后生成。
            const [, tier] = await Promise.all([
                ensureSigner(request.owner), subscriptionTier()
            ]);
            if (!active(request)) return;
            stage = 'PG_SIGN_FAILED';
            const snapshot = await signRequest(request);
            if (!active(request)) return;
            if (!validToken(snapshot)) throw new PgError(stage);
            signFailures = 0;
            stage = 'PG_AUTH_UNAVAILABLE';
            const auth = await authorization();
            if (!active(request)) return;
            stage = 'PG_FETCH_FAILED';
            const headers = {
                'Content-Type': 'application/json+protobuf',
                'Authorization': auth,
                'X-Goog-Api-Key': API_KEY
            };
            if (tier === 'TIER1' || tier === 'TIER2') {
                headers['X-AIStudio-G1-Tier'] = tier;
            }
            const response = await fetch(RPC_URL, {
                method: 'POST',
                headers,
                credentials: 'include', redirect: 'error',
                signal: request.controller.signal,
                body: JSON.stringify({ ...request.body, requestSnapshot: snapshot })
            });
            if (!active(request)) {
                if (response.body) void response.body.cancel().catch(() => {});
                return;
            }
            lastHttpStatus = response.status;
            if (!send(request, 'PG_HEADERS', {
                status: response.status, headers: Object.fromEntries(response.headers.entries())
            })) return;
            stage = 'PG_READ_FAILED';
            if (response.body) {
                reader = response.body.getReader();
                request.reader = reader;
                const decoder = new TextDecoder('utf-8', { fatal: true });
                while (true) {
                    const { done, value } = await reader.read();
                    if (!active(request)) return;
                    if (done) break;
                    const chunk = decoder.decode(value, { stream: true });
                    if (chunk && !send(request, 'PG_CHUNK', { chunk })) return;
                }
                const tail = decoder.decode();
                if (!active(request)) return;
                if (tail && !send(request, 'PG_CHUNK', { chunk: tail })) return;
            }
            // 只表示传输完整结束，不把 HTTP 200 或 EOF 当成模型语义成功。
            lastTransportCompletedAt = Date.now();
            finish(request, 'PG_END');
        } catch (error) {
            if (active(request)) {
                const code = error instanceof PgError ? error.code : stage;
                if (stage !== 'PG_BOOTSTRAP_FAILED') recordFailure(code);
                finish(request, 'PG_ERROR', { code, message: messages[code] });
            }
        } finally {
            if (!request.done) finish(request);
            if (reader) { try { reader.releaseLock(); } catch { /* 已取消或释放。 */ } }
            request.reader = null;
        }
    }

    function onMessage(event) {
        const owner = binding;
        if (!owner || event.source !== owner.window || event.origin !== owner.origin ||
            !isObject(event.data)) return;
        const message = event.data;
        if (message.type === '__PG_PREVIEW_GONE' && message.bridgeSessionId === owner.session) {
            invalidate(owner);
            return;
        }
        if (!bound(owner)) return;
        if (message.type === 'PG_READY') { post(owner, 'PG_INIT'); return; }
        if (message.bridgeSessionId !== owner.session ||
            typeof message.bridgeRequestId !== 'string' || !message.bridgeRequestId) return;
        const id = message.bridgeRequestId;
        if (message.type === 'PG_ABORT') {
            const request = requests.get(id);
            if (request && request.owner === owner) finish(request);
            return;
        }
        if (message.type !== 'PG_REQUEST' || requests.has(id)) return;
        const body = message.requestBody;
        if (!isObject(body) || typeof body.model !== 'string' ||
            !/^models\/[A-Za-z0-9][A-Za-z0-9._-]*$/.test(body.model) ||
            body.model.endsWith('-pg') || 'requestSnapshot' in body || 'request_snapshot' in body ||
            typeof message.signDigest !== 'string' || !/^[a-fA-F0-9]{64}$/.test(message.signDigest)) {
            post(owner, 'PG_ERROR', {
                bridgeRequestId: id, code: 'PG_INVALID_REQUEST', message: messages.PG_INVALID_REQUEST
            });
            return;
        }
        if (!used) {
            used = true;
            // 只通知首次合法业务使用；健康采样同时保留 used 作为后备观测。
            void window.__pgMarkUsed(owner.session).catch(() => {});
        }
        if (recovering) {
            post(owner, 'PG_ERROR', {
                bridgeRequestId: id, code: 'PG_RECOVERING', message: messages.PG_RECOVERING
            });
            return;
        }
        const request = {
            id, owner, body, digest: message.signDigest,
            controller: new AbortController(), reader: null, done: false
        };
        requests.set(id, request);
        void run(request);
    }

    function dispose() {
        if (closed) return;
        invalidate();
        closed = true;
        benefit.controller?.abort();
        benefit.tier = null;
        benefit.expiresAt = 0;
        signer = null;
        bootstrap = null;
        window.removeEventListener('message', onMessage);
        window.removeEventListener('pagehide', dispose);
    }

    window.__pgHostBridge = Object.freeze({
        prepare, dispose, status, warmup, setRecovering, reloadIfIdle
    });
    window.addEventListener('message', onMessage);
    window.addEventListener('pagehide', dispose);
})(__PG_TARGET_PATH__);
