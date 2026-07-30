import http from 'node:http';
import https from 'node:https';
import axios from 'axios';

// Shared keep-alive connection pool for every outbound axios call in this process.
//
// WHY: we run on node:18-slim, where http(s).globalAgent defaults to keepAlive: false
// (keep-alive only became the default in Node 19). Without it, every single request
// opens a brand-new TCP + TLS connection. partner.shopeemobile.com resolves to ONE IP
// (147.136.145.192, behind an NLB), and a full /realtime-sync tick fires ~300 requests
// at it from 22 Shopee shops inside a ~60s window. That volume of fresh handshakes to a
// single destination endpoint exhausts NAT source ports / trips SYN throttling, which
// surfaces as "connect ETIMEDOUT 147.136.145.192:443".
//
// With pooling, those ~300 requests reuse ~16-20 warm sockets instead.
const options = {
    keepAlive: true,
    keepAliveMsecs: 30_000, // TCP keep-alive probe interval on idle sockets
    maxSockets: 30,         // ceiling PER ORIGIN (Shopee, Shopify, TikTok each get their own 30)
    maxFreeSockets: 20,     // idle sockets retained per origin, so they stay warm across brands
    scheduling: 'lifo',     // reuse the hottest socket and let cold ones age out
    timeout: 60_000,        // reap sockets idle for 60s (e.g. between scheduler ticks)
};

export const httpAgent = new http.Agent(options);
export const httpsAgent = new https.Agent(options);

// Applied as a module side effect rather than by the importer: every file in this repo
// does `import axios from 'axios'`, which is the same singleton, so setting the defaults
// here covers all ~55 call sites. Doing it at module-evaluation time also guarantees the
// pool is in place before any processor module can fire a request.
axios.defaults.httpAgent = httpAgent;
axios.defaults.httpsAgent = httpsAgent;

// Snapshot of pool usage per origin. Handy for confirming sockets are actually being
// reused rather than recreated.
export function poolStatus() {
    const count = (bucket) => Object.fromEntries(
        Object.entries(bucket).map(([origin, entries]) => [origin, entries.length])
    );

    const snapshot = (agent) => ({
        active: count(agent.sockets),   // sockets currently serving a request
        free: count(agent.freeSockets), // warm sockets parked for reuse
        queued: count(agent.requests),  // requests waiting because maxSockets is saturated
    });

    return { https: snapshot(httpsAgent), http: snapshot(httpAgent) };
}
