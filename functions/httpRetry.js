import axios from 'axios';

// Retry wrapper for outbound API calls that are on the realtime critical path.
//
// axios.defaults.timeout (see httpAgent.js) caps how long any single attempt can hang,
// but a timeout on its own just fails faster — the caller still loses the data. This
// retries the failures that are worth retrying, which in practice is nearly all of them:
// a dropped connection to partner.shopeemobile.com almost always succeeds on the next try.
const RETRYABLE_CODES = new Set([
    'ETIMEDOUT',      // TCP connect never answered
    'ECONNABORTED',   // our own axios timeout fired
    'ECONNRESET',     // peer dropped it, often a stale socket from the keep-alive pool
    'EPIPE',
    'EAI_AGAIN',      // transient DNS failure
    'ENETUNREACH',
]);

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// 4xx is deliberately NOT retryable: a bad token or a bad signature will not fix itself,
// and retrying just burns quota against the partner app.
function isRetryable(e) {
    if (RETRYABLE_CODES.has(e.code)) return true;
    const status = e.response?.status;
    return status === 429 || (status >= 500 && status <= 599);
}

export async function requestWithRetry(config, { attempts = 3, label = '' } = {}) {
    let lastError;

    for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
            return await axios(config);
        } catch (e) {
            lastError = e;

            if (attempt === attempts || !isRetryable(e)) throw e;

            // 400ms then 1200ms. Deliberately short - this is a realtime dashboard, and
            // the common case is a dead pooled socket that reconnects immediately.
            const wait = 400 * Math.pow(3, attempt - 1);
            const reason = e.code || e.response?.status;
            console.log(`[RETRY] ${label} attempt ${attempt}/${attempts} failed (${reason}), retrying in ${wait}ms`);
            await sleep(wait);
        }
    }

    throw lastError;
}
