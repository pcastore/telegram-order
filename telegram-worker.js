/**
 * PCA Game Store — Standalone Telegram Approval Worker
 * ================================================
 * A self-contained Cloudflare Worker. Does not touch, call, or depend on
 * any other bot/worker in the project.
 *
 * What it does, fully server-side (admin panel does NOT need to be open):
 *
 *   1. CRON (every 1 min): scans Firestore for new "Pending" orders and
 *      walletTopups that haven't been sent to Telegram yet, and sends them
 *      with inline Confirm/Reject buttons.
 *
 *   2. WEBHOOK (/telegram-webhook): receives the button press the instant
 *      it happens and updates Firestore directly via the REST API.
 *
 *   3. CRON (same tick, after the sweep above): auto-fulfills any Pending
 *      order paid straight from the customer's wallet (paidFromWallet:
 *      true). These NEVER go to Telegram — the customer already paid, so
 *      there is nothing for a human to approve:
 *        - G2Bulk-linked package  -> place the order with G2Bulk's API
 *        - JCPlays-linked package -> place the order with JCPlays' API
 *        - neither (manual game)  -> move straight to "Processing" so it
 *          shows up in the admin panel's normal Processing queue (nothing
 *          can auto-fulfill a manual game — there's no API for it)
 *      Reads the G2Bulk/JCPlays API keys AND (for G2Bulk) the webhook
 *      URL/secret straight from the settings/g2bulk and settings/jcplays
 *      Firestore docs — the exact same docs admin.html's G2Bulk/JCPlays
 *      Settings modals already write to, so there is only ONE place to
 *      configure any of this (the admin panel), not two.
 *      If auto-fulfillment ultimately fails (5 attempts, see
 *      MAX_WALLET_FULFILL_ATTEMPTS) the customer's wallet is refunded
 *      automatically in the same write that marks the order Fail — see
 *      finalizeWalletOrderFailure(). This mirrors the refund logic in the
 *      companion g2bulk-function Worker (for a FAILED webhook callback) and
 *      in admin.html's markOrderFailedWithRefund() (for a manual "Mark
 *      Fail" or a manual "Check G2Bulk Status" that comes back FAILED).
 *
 *   4. CRON (same tick): sends a text-only Daily Order Summary to Telegram
 *      at exactly 00:00 Yangon time — counts by status, total sales, then
 *      the full itemised order list for the day that just ended. Mirrors
 *      admin.html's sendDailySummaryToTelegram() message-for-message; see
 *      checkDailySummarySchedule() below for the Yangon-time + dedupe logic
 *      (guards against a double-send via the same settings/dailySummary doc
 *      admin.html used to write to for its own, now-disabled, browser-tab
 *      version of this).
 *
 * Status mapping for receipt-paid orders confirmed via Telegram:
 *   Order   Confirm -> G2Bulk-linked game     : placed with G2Bulk's API right
 *                       away (fulfillG2BulkOrder), same as "Auto-Fulfill via
 *                       G2Bulk" in the admin panel. Lands on "Processing"
 *                       (finishes via the G2Bulk webhook/companion Worker) or
 *                       "Fail" if G2Bulk rejects it.
 *                     JCPlays-linked game     : placed with JCPlays' API right
 *                       away (fulfillJCPlaysOrder) - JCPlays confirms
 *                       synchronously, so this resolves straight to "Success"
 *                       or "Fail".
 *                     G2Bulk voucher/giftcard : purchased right away
 *                       (fulfillG2BulkVoucherOrder). Resolves instantly to
 *                       "Success" if G2Bulk had the codes ready, or
 *                       "Processing" with a saved poll_url if not - in which
 *                       case pollPendingVoucherOrders() (run every cron tick,
 *                       ~1 min) keeps checking server-side until it resolves,
 *                       since this side of the G2Bulk API has no webhook.
 *                     Manual/unlinked game    : status "Processing", same
 *                       meaning as "Start Processing" - an admin finishes it
 *                       by hand.
 *   Order   Reject  -> status "Fail"
 *   Topup   Confirm -> status "Completed"  + wallet credited (users/{uid}.walletBalance)
 *   Topup   Reject  -> status "Rejected"
 *
 * ---------------- SETUP ----------------
 * See SETUP.md alongside this file for the full step-by-step deploy guide.
 */

const FIRESTORE_SCOPE = 'https://www.googleapis.com/auth/datastore';
// JCPlays proxy URL: taken from settings/jcplays.proxyUrl (saved in the admin panel's JCPlays Settings),
// or the JCPLAYS_PROXY_URL Worker variable. No hard-coded URL.
function jcplaysProxyUrl(env, jcplays) {
  return (jcplays && jcplays.proxyUrl) || env.JCPLAYS_PROXY_URL || '';
}
const FAZERCARD_BASE_URL = 'https://api.fzr.cards/api/v2';
// The customer-facing bot (pcagamestore_bot / @pcagamestore_bot), a separate
// Worker (gamestore-auth-worker.js) — this file only ever talks to it via
// its /notify-topup-status endpoint, to DM a customer back in *that* bot's
// chat after a human resolves their wallet top-up here. See
// notifyTopupCustomer() below.
const AUTH_WORKER_URL = 'https://pcastore.pca-game-store.workers.dev';
// Both Workers live on the same account's workers.dev subdomain, and Cloudflare
// blocks a plain fetch() from one to the other's public URL (error 1042 - it
// answers 404/HTML instead of running the target Worker). That is why the
// customer DMs never arrived even though calling the URL from outside (curl)
// worked fine. The fix is a Service Binding: Cloudflare dashboard ->
// this Worker -> Settings -> Bindings -> Add -> Service binding, variable name
// AUTH_WORKER, service = the gamestore-auth-worker Worker (pcastore).
// Falls back to a plain fetch() when the binding isn't set up yet, so nothing
// breaks before the binding exists.
function callAuthWorker(env, path, init) {
  const url = `${env.AUTH_WORKER_URL || AUTH_WORKER_URL}${path}`;
  return env.AUTH_WORKER && typeof env.AUTH_WORKER.fetch === 'function'
    ? env.AUTH_WORKER.fetch(url, init)
    : fetch(url, init);
}
const MAX_WALLET_FULFILL_ATTEMPTS = 5; // after this many failures, give up and mark the order Fail

let cachedToken = null; // module-scope cache, lives for the isolate's lifetime

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runCronSweepLocked(env));
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get('Origin');
    // Which website list applies to this path: customer endpoints -> the store, admin helpers -> the
    // admin panel. A browser on any other website is refused. (Server-to-server calls, e.g. the
    // customer bot's /ping, send no Origin header and are not affected.)
    const allowed = STORE_PATHS.has(url.pathname) ? originList(env.STORE_ORIGINS, STORE_ORIGINS)
      : ADMIN_PATHS.has(url.pathname) ? originList(env.ADMIN_ORIGINS, null)
      : null;
    if (allowed && origin && !allowed.includes(origin)) {
      return new Response('forbidden', { status: 403 });
    }
    const res = await routeRequest(request, env, ctx, url);
    return allowed ? applyOriginCors(res, origin, allowed) : res;
  }
};

// Origin lists can be changed WITHOUT editing code: set STORE_ORIGINS / ADMIN_ORIGINS in the Worker's
// variables as a comma-separated list (e.g. "https://pcagameshop.com,https://www.pcagameshop.com").
// ADMIN_ORIGINS unset = admin helpers keep answering any website (they still need a real admin token).
function originList(fromEnv, fallback) {
  const list = String(fromEnv || '').split(',').map(x => x.trim().replace(/\/$/, '')).filter(Boolean);
  return list.length ? list : fallback;
}

// The ONLY websites allowed to call the customer endpoints from a browser.
// Add the new domain here when the store moves (www. variants are listed separately).
const STORE_ORIGINS = [
  'https://pcagamestoremm.vercel.app'
];
const STORE_PATHS = new Set(['/ping', '/register-user', '/create-order', '/create-topup', '/update-profile', '/my-state']);
const ADMIN_PATHS = new Set(['/admin-login-lookup', '/admin-sync-claims', '/cf-usage', '/firestore-usage', '/admin-backfill-google-email']);

function applyOriginCors(res, origin, allowed) {
  const headers = new Headers(res.headers);
  if (origin && allowed.includes(origin)) headers.set('Access-Control-Allow-Origin', origin);
  else headers.delete('Access-Control-Allow-Origin');
  headers.append('Vary', 'Origin');
  return new Response(res.body, { status: res.status, headers });
}

async function routeRequest(request, env, ctx, url) {
  {
    if (request.method === 'POST' && url.pathname === '/telegram-webhook') {
      return handleTelegramWebhook(request, env, ctx);
    }

    // Instant "wake up now" ping from Store.html the moment an order / wallet
    // top-up is saved, so it reaches Telegram (or auto-fulfils) right away
    // instead of waiting for the next cron tick. See handlePing() below.
    if (url.pathname === '/ping') {
      if (request.method === 'OPTIONS') return corsPreflight();
      if (request.method === 'POST') return handlePing(request, env, ctx);
    }

    // Customer registration: the ONLY place a customer ID is ever issued. See
    // handleRegisterUser() near the bottom of this file.
    if (url.pathname === '/register-user') {
      if (request.method === 'OPTIONS') return corsPreflight();
      if (request.method === 'POST') {
        try {
          return await handleRegisterUser(request, env, ctx);
        } catch (err) {
          console.error('/register-user failed', err);
          return corsJson({ ok: false, error: 'Server error' }, 500);
        }
      }
    }

    // Customer data is written ONLY here: orders, wallet top-up requests and profile
    // edits. The browser sends its Firebase ID token; the Worker verifies it, finds the
    // customer, validates the fields and writes with the service account. See
    // "Customer writes" near the bottom of this file.
    if (url.pathname === '/create-order' || url.pathname === '/create-topup' || url.pathname === '/update-profile' || url.pathname === '/my-state') {
      if (request.method === 'OPTIONS') return corsPreflight();
      if (request.method === 'POST') {
        try {
          if (url.pathname === '/create-order') return await handleCreateOrder(request, env, ctx);
          if (url.pathname === '/create-topup') return await handleCreateTopup(request, env, ctx);
          if (url.pathname === '/my-state') return await handleMyState(request, env);
          return await handleUpdateProfile(request, env);
        } catch (err) {
          console.error(url.pathname + ' failed', err);
          return corsJson({ ok: false, error: 'Server error' }, 500);
        }
      }
    }

    // Admin-panel auth helpers (Phase 1 Firestore-rules hardening). See the
    // "Firebase Auth: ID-token verification + admin custom claims" section at
    // the bottom of this file.
    if (url.pathname === '/admin-login-lookup' || url.pathname === '/admin-sync-claims') {
      if (request.method === 'OPTIONS') return corsPreflightAdmin();
      if (request.method === 'POST') {
        try {
          return url.pathname === '/admin-login-lookup'
            ? await handleAdminLoginLookup(request, env)
            : await handleAdminSyncClaims(request, env, url);
        } catch (err) {
          console.error(url.pathname + ' failed', err);
          return corsJsonAdmin({ ok: false, error: 'Server error' }, 500);
        }
      }
    }

    // Public, edge-cached "is the admin online?" status for the storefront badge.
    // Replaces the storefront's live Firestore listener on settings/presence (every
    // admin heartbeat write was billed as one read per open storefront). See
    // handleAdminStatus() at the bottom of this file.
    if (url.pathname === '/admin-status') {
      if (request.method === 'OPTIONS') return adminStatusPreflight();
      if (request.method === 'GET') {
        try {
          return await handleAdminStatus(env);
        } catch (err) {
          console.error('/admin-status failed', err);
          return adminStatusJson({ ok: false, error: 'Server error' }, 500, 0);
        }
      }
    }

    // Real Cloudflare Workers usage (all Workers on the account) for the admin
    // Dashboard card. Admin-only; see handleCfUsage() at the bottom of this file.
    if (url.pathname === '/cf-usage') {
      if (request.method === 'OPTIONS') return corsPreflightAdmin();
      if (request.method === 'POST') {
        try {
          return await handleCfUsage(request, env);
        } catch (err) {
          console.error('/cf-usage failed', err);
          return corsJsonAdmin({ ok: false, error: 'Server error' }, 500);
        }
      }
    }

    // Real Firestore reads/writes/deletes for the whole project (storefront +
    // this Worker + admin panel) from Google Cloud Monitoring, for the admin
    // Dashboard card. Admin-only; see handleFirestoreUsage() at the bottom.
    if (url.pathname === '/firestore-usage') {
      if (request.method === 'OPTIONS') return corsPreflightAdmin();
      if (request.method === 'POST') {
        try {
          return await handleFirestoreUsage(request, env);
        } catch (err) {
          console.error('/firestore-usage failed', err);
          return corsJsonAdmin({ ok: false, error: 'Server error' }, 500);
        }
      }
    }

    // One-time helper: copies each linked Google account's REAL sign-in email
    // (from Firebase Auth) onto users/{guestId}.googleEmail where it is missing,
    // so wallet orders can prove who owns a wallet. See handleBackfillGoogleEmail().
    if (url.pathname === '/admin-backfill-google-email') {
      if (request.method === 'OPTIONS') return corsPreflightAdmin();
      if (request.method === 'POST') {
        try {
          return await handleBackfillGoogleEmail(request, env, url);
        } catch (err) {
          console.error('/admin-backfill-google-email failed', err);
          return corsJsonAdmin({ ok: false, error: 'Server error' }, 500);
        }
      }
    }

    // One-time: copy old imgbb pictures into D1 and rewrite the links in Firestore
    // (see handleMigrateImages() at the bottom of this file).
    if (request.method === 'GET' && url.pathname === '/migrate-images') {
      return await handleMigrateImages(env, url);
    }

    // Manual trigger for testing without waiting for the cron tick.
    // Guarded by the same secret so randoms can't spam your Telegram chat.
    // ?force=backup|weekly|daily|cost runs just that one time-gated sweep
    // immediately, bypassing its day/hour + dedupe check - useful for
    // testing the weekly summary/backup without waiting for an actual
    // Sunday/Monday 00:00 Yangon. No ?force= (or an unrecognized value)
    // runs the full normal cron sweep instead.
    if (request.method === 'GET' && url.pathname === '/run-sweep') {
      // Its own secret (SWEEP_SECRET), never the Telegram webhook secret. Send it as the
      // X-Sweep-Secret header; ?secret= still works so a phone browser can trigger it.
      // If SWEEP_SECRET is not set the endpoint stays switched off.
      const given = request.headers.get('X-Sweep-Secret') || url.searchParams.get('secret') || '';
      if (!env.SWEEP_SECRET || !timingSafeEqualStr(given, env.SWEEP_SECRET)) {
        return new Response('forbidden', { status: 403 });
      }
      try {
        const force = url.searchParams.get('force');
        if (force === 'backup') {
          await runFirestoreBackupSweep(env, true);
        } else if (force === 'weekly') {
          await checkWeeklySummarySchedule(env, true);
        } else if (force === 'daily') {
          await checkDailySummarySchedule(env, true);
        } else if (force === 'cost') {
          // Forget every saved supplier fingerprint so the next passes re-check (and re-apply) every catalogue;
          // call /run-sweep?force=cost again to speed it along (each call checks a few more catalogues).
          await patchDoc(env, COST_WATCH_DOC, { builtAt: 0, sources: [], retry: [] }, ['builtAt', 'sources', 'retry']);
          await runAutoCostUpdateSweep(env);
        } else {
          await runCronSweep(env);
        }
        return new Response('swept ok');
      } catch (err) {
        console.error('manual sweep failed', err);
        return new Response('sweep failed: ' + err.message, { status: 500 });
      }
    }

    return new Response('PCA Game Store Telegram Worker is running.');
  }
}

// ---------------------------------------------------------------------------
// Firebase service-account auth (RS256 JWT -> OAuth2 access token)
// ---------------------------------------------------------------------------

function base64url(input) {
  const str = typeof input === 'string' ? btoa(input) : btoa(String.fromCharCode(...new Uint8Array(input)));
  return str.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

async function importPrivateKey(pem) {
  const cleaned = pem.replace(/\\n/g, '\n');
  const body = cleaned
    .replace('-----BEGIN PRIVATE KEY-----', '')
    .replace('-----END PRIVATE KEY-----', '')
    .replace(/\s+/g, '');
  const der = Uint8Array.from(atob(body), c => c.charCodeAt(0));
  return crypto.subtle.importKey(
    'pkcs8', der.buffer, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']
  );
}

async function getAccessToken(env) {
  const now = Math.floor(Date.now() / 1000);
  if (cachedToken && cachedToken.expires > now + 60) return cachedToken.token;

  const header = { alg: 'RS256', typ: 'JWT' };
  const claim = {
    iss: env.FIREBASE_CLIENT_EMAIL,
    scope: FIRESTORE_SCOPE,
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600
  };
  const signingInput = `${base64url(JSON.stringify(header))}.${base64url(JSON.stringify(claim))}`;
  const key = await importPrivateKey(env.FIREBASE_PRIVATE_KEY);
  const signature = await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5', key, new TextEncoder().encode(signingInput)
  );
  const jwt = `${signingInput}.${base64url(signature)}`;

  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Firestore auth failed: ' + JSON.stringify(data));
  cachedToken = { token: data.access_token, expires: now + (data.expires_in || 3600) };
  return cachedToken.token;
}

// ---------------------------------------------------------------------------
// Firestore REST helpers
// ---------------------------------------------------------------------------

function firestoreDocsBase(env) {
  return `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents`;
}

function docName(env, path) {
  return `projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents/${path}`;
}

function encodeValue(v) {
  if (v === null || v === undefined) return { nullValue: null };
  if (typeof v === 'string') return { stringValue: v };
  if (typeof v === 'boolean') return { booleanValue: v };
  if (typeof v === 'number') return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  if (v instanceof Date) return { timestampValue: v.toISOString() };
  if (Array.isArray(v)) return { arrayValue: { values: v.map(encodeValue) } };
  if (typeof v === 'object') {
    const fields = {};
    for (const k in v) fields[k] = encodeValue(v[k]);
    return { mapValue: { fields } };
  }
  return { stringValue: String(v) };
}

function decodeValue(v) {
  if (!v) return null;
  const key = Object.keys(v)[0];
  switch (key) {
    case 'stringValue': return v.stringValue;
    case 'integerValue': return parseInt(v.integerValue, 10);
    case 'doubleValue': return v.doubleValue;
    case 'booleanValue': return v.booleanValue;
    case 'timestampValue': return v.timestampValue;
    case 'nullValue': return null;
    case 'mapValue': return decodeFields(v.mapValue.fields || {});
    case 'arrayValue': return (v.arrayValue.values || []).map(decodeValue);
    default: return null;
  }
}

function decodeFields(fields) {
  const out = {};
  for (const k in fields) out[k] = decodeValue(fields[k]);
  return out;
}

async function getDoc(env, path) {
  const token = await getAccessToken(env);
  const res = await fetch(`${firestoreDocsBase(env)}/${path}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  if (res.status === 404) return null;
  if (!res.ok) {
    console.error('getDoc failed', path, res.status, await res.text());
    return null;
  }
  const data = await res.json();
  if (!data.fields) return null;
  return { name: data.name, updateTime: data.updateTime, fields: decodeFields(data.fields) };
}

async function patchDoc(env, path, fields, updateMaskFields) {
  const token = await getAccessToken(env);
  const encoded = {};
  for (const k in fields) encoded[k] = encodeValue(fields[k]);
  const mask = updateMaskFields.map(f => `updateMask.fieldPaths=${encodeURIComponent(f)}`).join('&');
  const res = await fetch(`${firestoreDocsBase(env)}/${path}?${mask}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ fields: encoded })
  });
  const data = await res.json();
  if (!res.ok) console.error('patchDoc failed', path, res.status, JSON.stringify(data));
  return data;
}

async function deleteFirestoreDoc(env, path) {
  const token = await getAccessToken(env);
  const res = await fetch(`${firestoreDocsBase(env)}/${path}`, {
    method: 'DELETE',
    headers: { Authorization: `Bearer ${token}` }
  });
  if (!res.ok && res.status !== 404) {
    console.error('deleteFirestoreDoc failed', path, res.status, await res.text().catch(() => ''));
  }
}

// ---------------------------------------------------------------------------
// Lightweight rate limiting (Firestore-backed, fixed window) — for public
// endpoints that have no signed-in admin/customer to key off yet, so they
// can't rely on Firestore rules. No KV/D1 binding needed: reuses the same
// atomic-increment pattern as walletIncrementWrite (no read-modify-write
// race — see commit below). Fails OPEN on a Firestore/network error (never
// let a monitoring hiccup lock out real users), fails CLOSED once the real
// count is over the limit.
//
// Housekeeping: this writes one small doc per ip+bucket+window to
// rateLimits/. It's server-only traffic (service-account calls bypass
// Firestore rules, same as every other Worker write in this file), but it
// will grow forever unless you add a TTL policy: Firebase Console ->
// Firestore -> TTL -> collection group "rateLimits" -> field "expiresAt".
async function rateLimitHit(env, bucket, ip, limit, windowSeconds, failClosed = false) {
  try {
    const windowId = Math.floor(Date.now() / (windowSeconds * 1000));
    const safeIp = String(ip || 'unknown').replace(/[^a-zA-Z0-9:.]/g, '_').slice(0, 64);
    const path = `rateLimits/${bucket}_${safeIp}_${windowId}`;
    const expiresAt = new Date(Date.now() + windowSeconds * 2 * 1000);
    const data = await commitWrites(env, [{
      update: { name: docName(env, path), fields: { at: encodeValue(new Date()), expiresAt: encodeValue(expiresAt) } },
      updateMask: { fieldPaths: ['at', 'expiresAt'] },
      updateTransforms: [{ fieldPath: 'count', increment: { integerValue: '1' } }]
    }]);
    const tr = data && data.writeResults && data.writeResults[0] && data.writeResults[0].transformResults;
    const count = (tr && tr[0] && tr[0].integerValue) ? parseInt(tr[0].integerValue, 10) : 1;
    return count > limit;
  } catch (e) {
    // Money-related buckets fail CLOSED: if the counter can't be written, refuse instead of allowing unlimited.
    console.error('rateLimitHit failed (' + (failClosed ? 'failing closed' : 'failing open') + ')', bucket, e.message);
    return failClosed;
  }
}

async function commitWrites(env, writes) {
  const token = await getAccessToken(env);
  const res = await fetch(
    `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:commit`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ writes })
    }
  );
  const data = await res.json();
  if (!res.ok) console.error('commitWrites failed', res.status, JSON.stringify(data));
  return data;
}

// ---------------------------------------------------------------------------
// Status rules (single source of truth for what a Telegram button / sweep may do)
// ---------------------------------------------------------------------------
// Order:  Pending -> Processing -> Success | Fail      (Pending may also go straight to Success | Fail)
// Top up: Pending -> Processing -> Completed | Rejected (Pending may also go straight to Completed | Rejected)
// Success / Fail / Completed / Rejected are final - nothing moves out of them.
const STATUS_TRANSITIONS = {
  order: { Pending: ['Processing', 'Success', 'Fail'], Processing: ['Success', 'Fail'], Success: [], Fail: [] },
  topup: { Pending: ['Processing', 'Completed', 'Rejected'], Processing: ['Completed', 'Rejected'], Completed: [], Rejected: [] }
};
function canTransition(kind, from, to) {
  const t = STATUS_TRANSITIONS[kind] || {};
  return Array.isArray(t[from]) && t[from].includes(to);
}
// commitWrites() never throws - Firestore reports a failed precondition (the doc changed since we read it)
// in the response body. A caller that ignores it tells the admin "done" when nothing was written.
function commitFailed(result) { return !result || !!result.error; }

// ---------------------------------------------------------------------------
// Order status alerts -> inboxes only (no push notifications any more)
// ---------------------------------------------------------------------------
// Customer inbox = the customer's Telegram chat with the store bot (the store's in-app
// inbox is built from the customer's own orders). Admin inbox = the admin Telegram chat
// card + the admin panel. notifyOrderStatus() is called after every order that this Worker
// resolves to Success/Fail; notifyOrderDm() (below) de-duplicates by order status, so a
// customer never gets the same message twice.
async function notifyOrderStatus(env, orderId, status, preloadedOrderDoc = null) {
  try {
    const orderDoc = preloadedOrderDoc || await getDoc(env, `orders/${orderId}`);
    if (!orderDoc) return;
    // The admin card in Telegram still says "Processing" after the Worker resolves the order -
    // flip it to Success / Failed right now (the cron reconcile below is only the safety net).
    try { await syncTelegramCard(env, orderId, { ...orderDoc.fields, status }); } catch (e) { console.error('card sync failed', orderId, e); }
    if (!orderDoc.fields.userId) return;
    await notifyOrderDm(env, orderId, orderDoc.fields, status);
  } catch (err) {
    console.error('notifyOrderStatus error', orderId, status, err);
  }
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Keeps the admin's Telegram card in step with the order. telegramCardStatus = the status the card
// currently shows ('Processing' is written when an admin taps Confirm, see markCardStatus).
// ---------------------------------------------------------------------------
const CARD_FINAL_LABEL = { Success: '✅ SUCCESS', Fail: '❌ FAILED' };
async function markCardStatus(env, orderId, status) {
  try { await patchDoc(env, `orders/${orderId}`, { telegramCardStatus: status }, ['telegramCardStatus']); } catch (e) { /* non-critical */ }
}
async function syncTelegramCard(env, orderId, fields) {
  const status = fields.status;
  if (!CARD_FINAL_LABEL[status] || !fields.telegramMessageId || fields.telegramCardStatus === status) return;
  await editResolvedMessage(env, orderId, fields, buildOrderMessage, CARD_FINAL_LABEL[status]);
  await markCardStatus(env, orderId, status);
}
// Safety net for orders finished by something other than this Worker's own poll (G2Bulk webhook Worker,
// admin panel, ...): cards still marked 'Processing' whose order is now Success/Fail get corrected.
async function reconcileTelegramCards(env) {
  const rows = await runQueryPaginated(env, (limit) => ({
    from: [{ collectionId: 'orders' }],
    where: { compositeFilter: { op: 'AND', filters: [
      { fieldFilter: { field: { fieldPath: 'telegramCardStatus' }, op: 'EQUAL', value: { stringValue: 'Processing' } } },
      { fieldFilter: { field: { fieldPath: 'status' }, op: 'IN', value: { arrayValue: { values: [{ stringValue: 'Success' }, { stringValue: 'Fail' }] } } } }
    ] } },
    orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
    limit
  }), 'orders', 'reconcileTelegramCards', 6);
  for (const d of rows.slice(0, 6)) {
    try { await syncTelegramCard(env, d.id, d.fields); } catch (e) { console.error('card reconcile failed', d.id, e); }
  }
}

// ---------------------------------------------------------------------------
// CORS helpers for /ping — the one endpoint on this Worker meant to be
// called directly from the browser (Store.html).
// ---------------------------------------------------------------------------

function corsJson(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    }
  });
}

function corsPreflight() {
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    }
  });
}

// Shared cursor-pagination engine for every query* helper below. A single
// runQuery call is capped at whatever `limit` is passed in, so once a
// collection has more matching docs than that cap, the old per-function
// implementations would silently drop the rest (e.g. a busy day with 30
// Pending orders would only ever see the first 25 — the other 5 would never
// reach Telegram until something else nudged their `status` field).
//
// This instead pages through with a `__name__` tiebreaker cursor and keeps
// requesting the next page until Firestore returns fewer docs than a full
// page, so every matching doc is eventually collected regardless of how
// many there are — bounded only by `hardCap` as a sane ceiling so a
// pathological collection can't make a single cron tick run forever.
//
// `buildStructuredQuery(pageSize)` must return a FRESH structuredQuery object
// each call (no `startAt`, this function adds that) whose `orderBy` always
// ends with a `__name__` entry — that's what makes the cursor unambiguous
// when multiple docs share the same value on the real sort field(s).
async function runQueryPaginated(env, buildStructuredQuery, collectionId, logLabel, hardCap = 2000) {
  const token = await getAccessToken(env);
  const pageSize = 300;
  const out = [];
  let lastDoc = null; // raw {name, fields} (Firestore wire format) of the previous page's last doc
  try {
    for (;;) {
      const structuredQuery = buildStructuredQuery(pageSize);
      if (lastDoc) {
        structuredQuery.startAt = {
          before: false,
          values: structuredQuery.orderBy.map(o =>
            o.field.fieldPath === '__name__' ? { referenceValue: lastDoc.name } : lastDoc.fields[o.field.fieldPath]
          )
        };
      }
      const res = await fetch(
        `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
          body: JSON.stringify({ structuredQuery })
        }
      );
      const rows = await res.json();
      if (!res.ok || !Array.isArray(rows)) {
        console.error(`${logLabel} failed`, collectionId, res.status, JSON.stringify(rows));
        break;
      }
      const docs = rows.filter(r => r.document);
      for (const r of docs) {
        out.push({ id: r.document.name.split('/').pop(), fields: decodeFields(r.document.fields || {}) });
      }
      if (docs.length < pageSize || out.length >= hardCap) break;
      lastDoc = docs[docs.length - 1].document;
    }
  } catch (err) {
    console.error(`${logLabel} threw`, collectionId, err);
  }
  return out;
}

// Queries a collection for status == "Pending", paging through every match
// (see runQueryPaginated above) so a busy sweep never leaves later Pending
// docs unseen just because there were more than one page's worth of them.
//
// Memoised per cron tick (see runCronSweep): sweepCollection('orders') and
// fulfillWalletOrders() both list Pending orders back-to-back every minute,
// and every doc returned is a billed read - so the second call reuses the
// first one's result. fulfillWalletOrders re-fetches each order fresh before
// claiming it, so a result a few seconds old is safe. Outside a cron tick
// (_pendingQueryCache === null) nothing is cached.
let _pendingQueryCache = null;
async function queryPending(env, collectionId, hardCap = 2000) {
  if (_pendingQueryCache && _pendingQueryCache.has(collectionId)) return _pendingQueryCache.get(collectionId);
  const rows = await runQueryPaginated(env, (limit) => ({
    from: [{ collectionId }],
    where: {
      fieldFilter: { field: { fieldPath: 'status' }, op: 'EQUAL', value: { stringValue: 'Pending' } }
    },
    orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
    limit
  }), collectionId, 'queryPending', hardCap);
  if (_pendingQueryCache) _pendingQueryCache.set(collectionId, rows);
  return rows;
}

// Queries a collection for docs whose `timestamp` field falls within
// [startIso, endIso). Used by the daily summary to pull "yesterday"'s
// orders — pages through every match so a big sales day can't get an
// undercounted summary just because it crossed the old single-page cap.
async function queryByTimestampRange(env, collectionId, startIso, endIso, hardCap = 5000) {
  return runQueryPaginated(env, (limit) => ({
    from: [{ collectionId }],
    where: {
      compositeFilter: {
        op: 'AND',
        filters: [
          { fieldFilter: { field: { fieldPath: 'timestamp' }, op: 'GREATER_THAN_OR_EQUAL', value: { timestampValue: startIso } } },
          { fieldFilter: { field: { fieldPath: 'timestamp' }, op: 'LESS_THAN', value: { timestampValue: endIso } } }
        ]
      }
    },
    orderBy: [
      { field: { fieldPath: 'timestamp' }, direction: 'ASCENDING' },
      { field: { fieldPath: '__name__' }, direction: 'ASCENDING' }
    ],
    limit
  }), collectionId, 'queryByTimestampRange', hardCap);
}

// Queries a collection for docs whose `fieldPath` timestamp is strictly
// after `sinceIso`, oldest-first. Used by the cron sweeps to find
// docs created since the last cron tick. Pages through every match, same
// as the other helpers here.
async function queryDocsSince(env, collectionId, fieldPath, sinceIso, hardCap = 2000) {
  return runQueryPaginated(env, (limit) => ({
    from: [{ collectionId }],
    where: {
      fieldFilter: { field: { fieldPath }, op: 'GREATER_THAN', value: { timestampValue: sinceIso } }
    },
    orderBy: [
      { field: { fieldPath }, direction: 'ASCENDING' },
      { field: { fieldPath: '__name__' }, direction: 'ASCENDING' }
    ],
    limit
  }), collectionId, 'queryDocsSince', hardCap);
}

// Queries a collection for a single field == a string value. Same
// pagination as queryPending, just not hardcoded to status=="Pending" -
// used for the admin-link lookups and the push-notification sweep
// (orders status=="Success"/"Fail").
async function queryEqualString(env, collectionId, fieldPath, value, hardCap = 2000) {
  return runQueryPaginated(env, (limit) => ({
    from: [{ collectionId }],
    where: { fieldFilter: { field: { fieldPath }, op: 'EQUAL', value: { stringValue: value } } },
    orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
    limit
  }), collectionId, 'queryEqualString', hardCap);
}

// Queries an entire collection with no filter (used where there's no
// pending/status field to narrow on, e.g. "give me every user doc so I can
// check which ones have an fcmToken"). Same fail-soft behavior as the other
// query helpers. limit defaults high since this is meant to cover the whole
// collection, not a page of it.
async function queryAllDocs(env, collectionId, limit = 1000) {
  try {
    const token = await getAccessToken(env);
    const body = { structuredQuery: { from: [{ collectionId }], limit } };
    const res = await fetch(
      `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`,
      { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }, body: JSON.stringify(body) }
    );
    const rows = await res.json();
    if (!res.ok || !Array.isArray(rows)) {
      console.error('queryAllDocs failed', collectionId, res.status, JSON.stringify(rows));
      return [];
    }
    return rows.filter(r => r.document).map(r => ({ id: r.document.name.split('/').pop(), fields: decodeFields(r.document.fields || {}) }));
  } catch (err) {
    console.error('queryAllDocs threw', collectionId, err);
    return [];
  }
}

// Creates a new doc with an auto-generated ID in the given collection path
// (e.g. "auditLog"). Unlike patchDoc, this is a POST to
// the collection itself, not a specific document.
async function createDoc(env, collectionPath, fields) {
  const token = await getAccessToken(env);
  const encoded = {};
  for (const k in fields) encoded[k] = encodeValue(fields[k]);
  const res = await fetch(`${firestoreDocsBase(env)}/${collectionPath}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ fields: encoded })
  });
  const data = await res.json();
  if (!res.ok) console.error('createDoc failed', collectionPath, res.status, JSON.stringify(data));
  return data;
}

// ---------------------------------------------------------------------------
// Per-admin Telegram linking — lets each admin connect their own Telegram
// account (adminUsernames/{username}.telegramChatId). Linking happens from admin.html (Settings >
// Admin Accounts > your own card > "Link Telegram"), which writes a
// short-lived 6-digit code onto that admin's own doc; the admin then DMs
// this bot with /link CODE (or taps a t.me/<bot>?start=link_CODE deep link,
// which Telegram delivers as "/start link_CODE") and it's redeemed below.
// The doc id in adminUsernames IS the username (admin.html writes it that
// way - doc(db,"adminUsernames",username)), so a resolved admin's `.id` is
// already the username, no separate lookup needed.
// ---------------------------------------------------------------------------

// Finds the adminUsernames/{username} doc carrying this still-valid code.
// Expiry is re-checked here (not just trusted from admin.html's countdown
// display) since this is the actual security boundary - a stale code must
// not be redeemable just because a client-side timer hasn't caught up.
async function findAdminByLinkCode(env, code) {
  const rows = await queryEqualString(env, 'adminUsernames', 'telegramLinkCode', code, 50);
  const now = Date.now();
  return rows.find(r => {
    const exp = r.fields.telegramLinkCodeExpiresAt;
    return exp && new Date(exp).getTime() > now;
  }) || null;
}

// Handles "/link 482913", the "/start link_482913" a deep-link tap sends,
// and a bare "/start"/"/help" with onboarding instructions. Returns true if
// it handled the message, false if it's unrelated.
async function handleTelegramLinkCommand(env, msg) {
  const text = (msg.text || '').trim();
  let code = null;
  const linkMatch = text.match(/^\/link(?:@\w+)?\s+(\d{4,8})$/i);
  const startMatch = text.match(/^\/start(?:@\w+)?(?:\s+link_(\d{4,8}))?$/i);
  if (linkMatch) code = linkMatch[1];
  else if (startMatch && startMatch[1]) code = startMatch[1];
  else if (startMatch || /^\/help(?:@\w+)?$/i.test(text)) {
    await tgApi(env, 'sendMessage', {
      chat_id: msg.chat.id, parse_mode: 'HTML',
      text: 'Hi! To connect your admin account: open the Admin Panel → Settings → Admin Accounts → your own card → "Link Telegram", then send me the 6-digit code shown there as:\n<code>/link 123456</code>'
    });
    return true;
  } else {
    return false; // not a /link or /start command - let the caller check if it's a reply instead
  }

  if (!code) {
    await tgApi(env, 'sendMessage', { chat_id: msg.chat.id, text: 'Send the code like this: /link 123456' });
    return true;
  }
  if (await rateLimitHit(env, 'tgLink', String(msg.from && msg.from.id || 'x'), 8, 600, true)) {
    await tgApi(env, 'sendMessage', { chat_id: msg.chat.id, text: 'Too many attempts. Please wait 10 minutes and try again.' });
    return true;
  }
  const admin = await findAdminByLinkCode(env, code);
  if (!admin) {
    await tgApi(env, 'sendMessage', { chat_id: msg.chat.id, text: '❌ That code is invalid or has expired. Generate a new one from Admin Panel → Settings → Admin Accounts.' });
    return true;
  }
  const username = admin.id;
  const displayName = [msg.from.first_name, msg.from.last_name].filter(Boolean).join(' ') || 'Telegram user';
  await patchDoc(
    env, `adminUsernames/${username}`,
    {
      telegramChatId: msg.chat.id, telegramName: displayName, telegramUsername: msg.from.username || null,
      telegramLinkedAt: new Date(), telegramLinkCode: null, telegramLinkCodeExpiresAt: null
    },
    ['telegramChatId', 'telegramName', 'telegramUsername', 'telegramLinkedAt', 'telegramLinkCode', 'telegramLinkCodeExpiresAt']
  );
  await tgApi(env, 'sendMessage', {
    chat_id: msg.chat.id, parse_mode: 'HTML',
    text: `✅ Linked as <b>${esc(username)}</b>. Your admin account is now connected to this chat.`
  });
  return true;
}

// ---------------------------------------------------------------------------
// AUTO COST UPDATE — follows the suppliers, not the clock
// ---------------------------------------------------------------------------
//  * settings/costWatch keeps one entry per supplier catalogue (G2Bulk game / JCPlays product /
//    G2Bulk voucher category / FazerCards category): which games use it, and a FINGERPRINT of the
//    last cost list it saw. The list is rebuilt from the games collection every 3 hours.
//  * Every 1-minute cron tick looks at only a few catalogues (COST_SOURCES_PER_TICK, round-robin by the
//    clock - no cursor writes). One request per catalogue; comparing fingerprints costs no Firestore reads.
//  * Only when a catalogue's list really changed are the games that use it read and updated - so the
//    update lands within one pass (a few minutes) of the supplier changing, and a quiet supplier costs nothing.
//  * Cost ALWAYS follows the supplier. Per game (games/{id}): autoUpdate === false -> Price is left alone
//    (only Cost changes); autoPriceMarginPct -> its own price margin (else settings/pricing.resyncMarginPct, else 3%).
//    Price is never left below Cost (see applyLiveCosts).
//  * Changed games wait a moment (until a tick brings no new change, 20 games, or 3 min) and are then
//    announced in ONE Telegram message with a button per game; a button sends that game's table.
//  * Tiny subrequest budget per tick (COST_TICK_BUDGET) - Cloudflare Workers Free allows 50 per invocation
//    and the order/top-up sweeps run first in the same tick.
// ---------------------------------------------------------------------------

const COST_WATCH_DOC = 'settings/costWatch';
const DEFAULT_RESYNC_MARGIN_PCT = 3;   // used until the admin's margin has been saved to settings/pricing (or on the game)
const COST_SOURCES_PER_TICK = 4;       // supplier catalogues checked per cron tick
const COST_REBUILD_MS = 3 * 60 * 60 * 1000;
const COST_FLUSH_BATCH = 20;           // announce at once when this many games are waiting
const COST_FLUSH_MAX_WAIT_MS = 3 * 60 * 1000;
let _costCfgMemo = null;               // { at, cfg } - settings docs reused for 5 minutes inside one isolate

// Same maths as admin.html's autoPriceForCost(): Cost + margin, always rounded UP (never down) to a tiered step:
// Price under 10,000 -> next multiple of 10; 10,000 to 59,999 -> next multiple of 50; 60,000 and up -> next multiple of 100.
function priceStepFor(raw) { return raw >= 60000 ? 100 : raw >= 10000 ? 50 : 10; }
function autoPriceForCost(cost, marginPct) {
  if (!cost || cost <= 0) return 0;
  marginPct = Math.max(0, Number(marginPct) || 0);   // never below cost
  const raw = cost * (1 + marginPct / 100);
  const step = priceStepFor(raw);
  return Math.ceil(raw / step) * step;
}
const COST_TICK_BUDGET = 10;   // max subrequests this sweep may spend per cron tick (raise it on a paid plan)
const COST_RESERVE = 2;        // kept back for the state save + a Telegram message
const COST_BATCH_GAMES = 8;    // games read with ONE batchGet call
const COST_DETAIL_CHUNK = 40;     // games per costUpdates/{cycle}_{k} doc (keeps each doc far below 1 MB)
const COST_BUTTONS_PER_MSG = 60;  // game buttons per index message (Telegram allows 100)
const COST_MAX_ROWS = 200;        // safety cap on queued game tables (they now wait in settings/costCycle until the cycle ends; keeps that doc under 1 MB)

function makeSubrequestBudget(total) {
  let used = 0;
  return { spend(n = 1) { used += n; }, left() { return total - used; }, used() { return used; } };
}

// Race-safe write of games/{id}.packages. Re-reads the game (or reuses `opts.preloaded` for the
// first try), lets mutate(packages, fields) re-apply the change, and commits with the doc's
// updateTime as a precondition so a save made by the admin in between is never overwritten.
// Returns the saved changes, [] when nothing needed saving, or null when the doc is gone /
// contention never cleared. opts.budget (optional) is charged for every request made.
async function updateGamePackagesMerged(env, gameId, mutate, opts = {}) {
  const budget = opts.budget;
  for (let attempt = 0; attempt < 5; attempt++) {
    let fresh;
    if (attempt === 0 && opts.preloaded) fresh = opts.preloaded;
    else { fresh = await getDoc(env, `games/${gameId}`); if (budget) budget.spend(1); }
    if (!fresh) return null;
    const packages = (Array.isArray(fresh.fields.packages) ? fresh.fields.packages : []).map(p => ({ ...p }));
    const ups = mutate(packages, fresh.fields) || [];
    if (!ups.length) return [];
    const result = await commitWrites(env, [{
      update: { name: docName(env, `games/${gameId}`), fields: { packages: encodeValue(packages) } },
      updateMask: { fieldPaths: ['packages'] },
      currentDocument: { updateTime: fresh.updateTime }
    }]);
    if (budget) budget.spend(1);
    if (!result.error) return ups;
    await new Promise(r => setTimeout(r, 150 + Math.random() * 250)); // admin saved in between - re-read and re-apply
  }
  console.error('updateGamePackagesMerged: gave up after retries for game', gameId);
  return null;
}

// ONE request that reads many docs (settings docs + a batch of games).
async function batchGetDocs(env, paths) {
  const token = await getAccessToken(env);
  const res = await fetch(`${firestoreDocsBase(env)}:batchGet`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ documents: paths.map(p => docName(env, p)) })
  });
  const rows = await res.json();
  if (!res.ok || !Array.isArray(rows)) throw new Error('batchGet failed: ' + res.status);
  const out = new Map();
  for (const r of rows) {
    if (!r.found) continue;
    const path = r.found.name.split('/documents/')[1];
    out.set(path, { name: r.found.name, updateTime: r.found.updateTime, fields: decodeFields(r.found.fields || {}) });
  }
  return out;
}

function buildCostCfg(docs) {
  const f = p => (docs.get(p) && docs.get(p).fields) || {};
  const g2bulk = f('settings/g2bulk'), jcplays = f('settings/jcplays'), fazercard = f('settings/fazercard');
  const pricing = f('settings/pricing');
  const mPct = Number(pricing.resyncMarginPct);
  return {
    autoPrice: pricing.autoPriceOnCost !== false,
    marginPct: Number.isFinite(mPct) && mPct >= 0 && mPct <= 100 ? mPct : DEFAULT_RESYNC_MARGIN_PCT,
    g2bulkOff: g2bulk.enabled === false, // only G2Bulk games/vouchers are skipped - other providers still update
    rate: Number(g2bulk.usdRate) || 4500,
    jcplays,
    jcplaysOk: !!(jcplays.uid && jcplays.email && jcplays.mKey),
    fazercard,
    fazerOk: !!(fazercard.apiKey && fazercard.enabled !== false),
    fazerRate: Number(fazercard.usdRate) || 4500 // FazerCards has its own USD->MMK rate
  };
}

// Which supplier catalogues a game needs this run (same linking rules as before).
function costSourcesForGame(f, cfg) {
  const s = [];
  if (f.g2bulkCode && !cfg.g2bulkOff) s.push({ kind: 'g2bulk', key: f.g2bulkCode, keyField: 'g2bulk' });
  if (f.jcplaysProduct && cfg.jcplaysOk) s.push({ kind: 'jcplays', key: f.jcplaysProduct, keyField: 'jcplays' });
  if (!cfg.g2bulkOff && f.fulfillProvider === 'g2bulk_voucher' && f.g2bulkVoucherCategory) s.push({ kind: 'voucher', key: f.g2bulkVoucherCategory, keyField: 'voucherId' });
  if (f.fazercardCategoryId && cfg.fazerOk) s.push({ kind: 'fazer', key: f.fazercardCategoryId, keyField: 'fazercardOfferId' });
  return s;
}

// Fetches one supplier catalogue as Map(packageKey -> live cost in MMK). Cached for the tick, so
// games sharing a catalogue cost one subrequest in total. null = could not be read this time.
async function fetchLiveCostMap(env, src, cfg, cache, budget) {
  const ck = src.kind + ':' + src.key;
  if (cache.has(ck)) return cache.get(ck);
  budget.spend(1);
  let map = null;
  try {
    if (src.kind === 'g2bulk') {
      const res = await fetch(`https://api.g2bulk.com/v1/games/${encodeURIComponent(src.key)}/catalogue`);
      const data = await res.json();
      if (data.success) map = new Map((data.catalogues || []).map(c => [c.name, Math.round(c.amount * cfg.rate)]));
    } else if (src.kind === 'jcplays') {
      const proxyUrl = jcplaysProxyUrl(env, cfg.jcplays);
      if (!proxyUrl) throw new Error('JCPlays proxy URL not set');
      const res = await fetch(proxyUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          endpointPath: 'productlist',
          extraParams: { product: src.key },
          uid: cfg.jcplays.uid, email: cfg.jcplays.email, mKey: cfg.jcplays.mKey,
          baseUrl: cfg.jcplays.baseUrl || 'https://jcplays.com/smilecoin/api'
        })
      });
      const data = await res.json();
      const list = (data && data.status === 200 && data.data && data.data.product) || null;
      if (list) map = new Map(list.map(r => [String(r.id), Math.round(Number(r.price) * cfg.rate)]));
    } else if (src.kind === 'voucher') {
      const res = await fetch(`https://api.g2bulk.com/v1/category/${encodeURIComponent(src.key)}`);
      const data = await res.json();
      if (data && data.success) {
        const products = data.products || (data.category ? data.category.products : []) || [];
        map = new Map(products.map(p => [String(p.id), Math.round(p.unit_price * cfg.rate)]));
      }
    } else if (src.kind === 'fazer') {
      const res = await fetch(`${FAZERCARD_BASE_URL}/topups/offers?category_id=${encodeURIComponent(src.key)}`, { headers: { 'X-API-Key': cfg.fazercard.apiKey } });
      const data = await res.json();
      if (data && data.ok && Array.isArray(data.offers)) map = new Map(data.offers.map(o => [String(o.offer_id), Math.round(Number(o.price_usd) * cfg.fazerRate)]));
    }
  } catch (err) { console.error('Cost update - supplier fetch error', ck, err); }
  cache.set(ck, map);
  return map;
}

// Writes the live Costs onto the packages. When a Cost moves, the Price follows it to Cost + the game's
// margin (up or down). Safety: whatever the settings, a changed package is never left with Price below Cost.
// gameFields: the game doc (autoPriceMarginPct = this game's own margin %).
function gameMarginPct(gameFields, cfg) {
  const g = gameFields && gameFields.autoPriceMarginPct;
  const v = (g === '' || g === null || g === undefined) ? NaN : Number(g);
  return Number.isFinite(v) && v >= 0 && v <= 100 ? v : cfg.marginPct;
}
function applyLiveCosts(packages, sources, maps, gameName, cfg, gameFields) {
  const ups = [];
  const margin = gameMarginPct(gameFields, cfg);
  const priceOn = !(gameFields && gameFields.autoUpdate === false);   // per-game switch: Cost always follows the supplier, Price only when on
  packages.forEach(p => {
    sources.forEach((s, i) => {
      const liveByKey = maps[i];
      if (!liveByKey) return;
      const key = String(p[s.keyField] || '').trim();
      if (!key || !liveByKey.has(key)) return;
      const liveCost = liveByKey.get(key);
      const savedCost = Number(p.cost) || 0;
      if (liveCost === savedCost) return;
      p.cost = liveCost;
      const oldPrice = Number(p.price) || 0;
      let newPrice = oldPrice;
      if (priceOn && liveCost > 0 && (cfg.autoPrice || oldPrice < liveCost)) {
        const target = autoPriceForCost(liveCost, margin);
        if (target > 0) { newPrice = target; p.price = target; }
      }
      ups.push({ gameName, packageLabel: p.amount || key, oldCost: savedCost, newCost: liveCost, oldPrice, newPrice });
    });
  });
  return ups;
}

// Coloured arrows (Telegram has no text colour, so the colour is the emoji):
//   Cost  up = 🔴⬆️ (margin squeezed)   down = 🟢⬇️ (cheaper for us)
//   Price up = 🟢⬆️ (customers pay more) down = 🟡⬇️
const ARROW_COST_UP = '🔴⬆️', ARROW_COST_DOWN = '🟢⬇️', ARROW_PRICE_UP = '🟢⬆️', ARROW_PRICE_DOWN = '🟡⬇️';
function pctChange(oldV, newV) {
  if (!oldV) return '';
  const p = (newV - oldV) / oldV * 100;
  return ` (${p >= 0 ? '+' : ''}${p.toFixed(1)}%)`;
}
// ONE Telegram message per game: the game name is the title, then every package of that game
// whose Cost moved. Split into several messages only if it would pass Telegram's length limit.
// Monospace table (<pre>) so the columns line up on every Telegram client:
//   Currency (package) | Old | Now | Change.  ▲ = went up, ▼ = went down.
function costTableText(rows) {
  const W = { name: 14, old: 8, now: 8 };
  const fit = (v, w) => String(v).length > w ? String(v).slice(0, w - 1) + '…' : String(v);
  const head = fit('Currency', W.name).padEnd(W.name) + ' ' + 'Old'.padStart(W.old) + ' ' + 'Now'.padStart(W.now) + '  Chg';
  const lines = rows.map(r => {
    const pct = r.oldV ? ((r.newV - r.oldV) / r.oldV * 100) : 0;
    const arrow = r.newV > r.oldV ? '▲' : r.newV < r.oldV ? '▼' : '=';
    const chg = r.oldV ? `${arrow}${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%` : arrow;
    return fit(r.name, W.name).padEnd(W.name) + ' ' + fit(r.oldV.toLocaleString(), W.old).padStart(W.old) + ' ' + fit(r.newV.toLocaleString(), W.now).padStart(W.now) + '  ' + chg;
  });
  return '<pre>' + esc([head, '-'.repeat(head.length), ...lines].join('\n')) + '</pre>';
}
// Native Bot API 10.3 rich-message table (sendRichMessage, InputRichBlockTable) like a real
// spreadsheet table. Direct block text is literal (no HTML escaping needed).
function richCostTable(rows, up, down) {
  const hc = (text, align) => ({ text, is_header: true, align, valign: 'middle' });
  const c = (text, align) => ({ text: String(text), align, valign: 'middle' });
  const body = rows.map(r => {
    const pct = r.oldV ? ((r.newV - r.oldV) / r.oldV * 100) : 0;
    const arrow = r.newV > r.oldV ? up : r.newV < r.oldV ? down : '=';
    const chg = r.oldV ? `${arrow} ${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%` : arrow;
    return [c(r.name, 'left'), c(r.oldV.toLocaleString(), 'right'), c(r.newV.toLocaleString(), 'right'), c(chg, 'right')];
  });
  return { type: 'table', is_bordered: true, is_striped: true,
    cells: [[hc('Currency', 'left'), hc('Old', 'right'), hc('Now', 'right'), hc('Change', 'right')], ...body] };
}
// ONE Telegram message per game: the game name is the title, then a Cost table (and a Price table
// if any selling price moved). Each row is stored as 'RICH1:' + JSON {rich, html}: sent as a native
// rich-message table, with the monospace <pre> version as fallback if Telegram rejects it.
function buildGameCostMessages(gameName, ups) {
  const title = `🎮 <b>${esc(gameName)}</b>`;
  const PER_MSG = 25;
  const out = [];
  for (let i = 0; i < ups.length; i += PER_MSG) {
    const chunk = ups.slice(i, i + PER_MSG);
    const suffix = ups.length > PER_MSG ? ` (${Math.floor(i / PER_MSG) + 1}/${Math.ceil(ups.length / PER_MSG)})` : '';
    const costRows = chunk.map(u => ({ name: u.packageLabel, oldV: u.oldCost, newV: u.newCost }));
    const priceRows = chunk.filter(u => u.newPrice !== u.oldPrice).map(u => ({ name: u.packageLabel, oldV: u.oldPrice, newV: u.newPrice }));
    const html = [`${title}${suffix}`, '💱 <b>Cost updated</b> (MMK)', costTableText(costRows)];
    const blocks = [
      { type: 'heading', size: 2, text: `🎮 ${gameName}${suffix}` },
      { type: 'paragraph', text: '💱 Cost updated (MMK)' },
      richCostTable(costRows, ARROW_COST_UP, ARROW_COST_DOWN)
    ];
    if (priceRows.length) {
      html.push('🏷 <b>Price updated</b> (MMK)', costTableText(priceRows));
      blocks.push({ type: 'paragraph', text: '🏷 Price updated (MMK)' }, richCostTable(priceRows, ARROW_PRICE_UP, ARROW_PRICE_DOWN));
    }
    // Attention list: price now below cost (selling at a loss), and packages whose Price was left alone
    // (auto price off for the game, or a margin that still covers the new Cost) so the admin knows nothing moved there.
    const lossNames = chunk.filter(u => u.newPrice > 0 && u.newPrice < u.newCost).map(u => u.packageLabel);
    const keptNames = chunk.filter(u => u.newPrice === u.oldPrice && !(u.newPrice > 0 && u.newPrice < u.newCost)).map(u => u.packageLabel);
    if (lossNames.length) {
      html.push('⚠️ <b>Price is BELOW cost</b>: ' + esc(lossNames.join(', ')));
      blocks.push({ type: 'paragraph', text: '⚠️ Price is BELOW cost: ' + lossNames.join(', ') });
    }
    if (keptNames.length) {
      html.push('🔒 Price unchanged: ' + esc(keptNames.join(', ')));
      blocks.push({ type: 'paragraph', text: '🔒 Price unchanged: ' + keptNames.join(', ') });
    }
    out.push('RICH1:' + JSON.stringify({ rich: { blocks }, html: html.join('\n'), name: gameName + suffix, n: chunk.length }));
  }
  return out;
}
// Sends one queued cost row; returns how many Telegram calls it used. Legacy plain-HTML rows
// (queued by an older deploy) still work.
async function sendCostRow(env, row, chatId = env.TELEGRAM_CHAT_ID, threadId = undefined) {
  const to = threadId ? { chat_id: chatId, message_thread_id: threadId } : { chat_id: chatId };
  if (typeof row === 'string' && row.startsWith('RICH1:')) {
    let p = null;
    try { p = JSON.parse(row.slice(6)); } catch (e) { /* fall through to plain */ }
    if (p) {
      const r = await tgApi(env, 'sendRichMessage', { ...to, rich_message: p.rich });
      if (r && r.ok) return 1;
      console.error('sendRichMessage failed, falling back to <pre> table', JSON.stringify(r));
      await tgApi(env, 'sendMessage', { ...to, parse_mode: 'HTML', text: p.html });
      return 2;
    }
  }
  await tgApi(env, 'sendMessage', { ...to, parse_mode: 'HTML', text: row });
  return 1;
}

function packCostRows(rows, limit = 3800) {
  const groups = [];
  let cur = [], len = 0;
  for (const row of rows) {
    if (cur.length && len + row.length + 2 > limit) { groups.push(cur); cur = []; len = 0; }
    cur.push(row); len += row.length + 2;
  }
  if (cur.length) groups.push(cur);
  return groups;
}

// A short, stable fingerprint of one catalogue's cost list (key -> cost). Compared with the saved one to
// learn "did the supplier change anything?" without reading a single Firestore document.
function costFingerprint(map) {
  const keys = [...map.keys()].sort();
  let h = 5381;
  for (const k of keys) {
    const str = k + ':' + map.get(k) + ';';
    for (let i = 0; i < str.length; i++) h = ((h * 33) ^ str.charCodeAt(i)) >>> 0;
  }
  return keys.length + ':' + h.toString(36);
}

// Rebuilds the catalogue list from the games collection (one query). A catalogue keeps its saved fingerprint only while the set of games using it is unchanged.
async function rebuildCostWatch(env, prevSources, cfg, budget) {
  const games = await queryAllDocs(env, 'games');
  budget.spend(1);
  const bySrc = new Map();
  for (const g of games) {
    for (const s of costSourcesForGame(g.fields, cfg)) {
      const k = s.kind + ':' + s.key;
      if (!bySrc.has(k)) bySrc.set(k, { k, kind: s.kind, key: String(s.key), g: [], fp: '' });
      bySrc.get(k).g.push(g.id);
    }
  }
  const old = new Map((prevSources || []).map(s => [s.k, s]));
  const sources = [...bySrc.values()].sort((a, b) => a.k.localeCompare(b.k));
  sources.forEach(s => {
    s.g.sort();
    const o = old.get(s.k);
    if (o && Array.isArray(o.g) && [...o.g].sort().join(',') === s.g.join(',')) s.fp = o.fp || '';
  });
  return sources;
}

// The supplier changed this catalogue: update every game that uses it. Returns 'ok', 'defer' (out of
// budget - try again next tick) or 'fail' (supplier unreadable / write contention - retried next pass).
async function applyCostChangeForSource(env, src, cfg, cache, budget, state, counters) {
  for (let i = 0; i < src.g.length; i += COST_BATCH_GAMES) {
    if (budget.left() < COST_RESERVE + 2) return 'defer';
    const chunk = src.g.slice(i, i + COST_BATCH_GAMES);
    const docs = await batchGetDocs(env, chunk.map(id => `games/${id}`));
    budget.spend(1);
    for (const id of chunk) {
      const gd = docs.get(`games/${id}`);
      if (!gd) continue;
      const sources = costSourcesForGame(gd.fields, cfg);
      const uncached = sources.filter(s => !cache.has(s.kind + ':' + s.key)).length;
      if (budget.left() - COST_RESERVE < uncached + 2) return 'defer';
      const maps = [];
      for (const s of sources) maps.push(await fetchLiveCostMap(env, s, cfg, cache, budget));
      if (sources.length && maps.every(m => m === null)) return 'fail';
      const gameName = gd.fields.name || id;
      const saved = await updateGamePackagesMerged(env, id,
        (pk, fields) => applyLiveCosts(pk, sources, maps, gameName, cfg, fields),
        { preloaded: gd, budget });
      if (saved === null) return 'fail';
      if (saved.length) {
        counters.added++;
        state.rowsGames++; state.rowsPkgs += saved.length;
        if (state.rows.length < COST_MAX_ROWS) state.rows.push(...buildGameCostMessages(gameName, saved));
      }
    }
  }
  return 'ok';
}

// ONE Telegram message (or a few if there are many games) listing every waiting game as a button;
// the tables themselves are saved to costUpdates/{flushId}_{k} and sent when a button is tapped.
async function flushCostRows(env, state, budget) {
  const items = state.rows;
  const flushId = String(Math.floor(Date.now() / 1000));
  const chunks = Math.ceil(items.length / COST_DETAIL_CHUNK);
  const parts = Math.ceil(items.length / COST_BUTTONS_PER_MSG);
  if (budget.left() < chunks + parts + 1) return false;
  for (let k = 0; k < chunks; k++) {
    await patchDoc(env, `costUpdates/${flushId}_${k}`, { items: items.slice(k * COST_DETAIL_CHUNK, (k + 1) * COST_DETAIL_CHUNK), createdAt: new Date().toISOString() }, ['items', 'createdAt']);
    budget.spend(1);
  }
  const labelOf = (row, i) => {
    if (typeof row === 'string' && row.startsWith('RICH1:')) {
      try { const p = JSON.parse(row.slice(6)); if (p && p.name) return `🎮 ${p.name}${p.n ? ' (' + p.n + ')' : ''}`; } catch (e) { /* fall through */ }
    }
    return `🎮 Game ${i + 1}`;
  };
  const buttons = items.map((row, i) => ({ text: labelOf(row, i).slice(0, 40), callback_data: `cost:${flushId}:${i}` }));
  const y = yangonNow();
  const hhmm = `${String(y.getUTCHours()).padStart(2, '0')}:${String(y.getUTCMinutes()).padStart(2, '0')}`;
  for (let m = 0; m < parts; m++) {
    const slice = buttons.slice(m * COST_BUTTONS_PER_MSG, (m + 1) * COST_BUTTONS_PER_MSG);
    const kb = [];
    for (let i = 0; i < slice.length; i += 2) kb.push(slice.slice(i, i + 2));
    const head = `💱 <b>Cost updated</b> (${hhmm}) — ${state.rowsPkgs} package(s) in ${state.rowsGames} game(s)` +
      (parts > 1 ? ` · ${m + 1}/${parts}` : '') + '\n👇 Tap a game to see its table';
    const r = await tgApi(env, 'sendMessage', { chat_id: env.TELEGRAM_CHAT_ID, parse_mode: 'HTML', text: head, reply_markup: { inline_keyboard: kb } });
    budget.spend(1);
    if (!(r && r.ok)) { console.error('Cost update index message failed', JSON.stringify(r)); return false; }
  }
  return true;
}

async function runAutoCostUpdateSweep(env) {
  const budget = makeSubrequestBudget(COST_TICK_BUDGET);
  const stDoc = await getDoc(env, COST_WATCH_DOC);
  budget.spend(1);
  const prev = (stDoc && stDoc.fields) || {};
  const state = {
    sources: Array.isArray(prev.sources) ? prev.sources.map(s => ({ ...s, g: Array.isArray(s.g) ? s.g : [] })) : [],
    builtAt: Number(prev.builtAt) || 0,
    retry: Array.isArray(prev.retry) ? prev.retry.slice() : [],
    rows: Array.isArray(prev.rows) ? prev.rows.slice() : [],
    rowsSince: prev.rowsSince || '',
    rowsPkgs: Number(prev.rowsPkgs) || 0,
    rowsGames: Number(prev.rowsGames) || 0
  };
  const dirty = new Set();
  const counters = { added: 0 };

  let cfg;
  if (_costCfgMemo && Date.now() - _costCfgMemo.at < 5 * 60 * 1000) cfg = _costCfgMemo.cfg;
  else {
    const docs = await batchGetDocs(env, ['settings/g2bulk', 'settings/jcplays', 'settings/fazercard', 'settings/pricing']);
    budget.spend(1);
    cfg = buildCostCfg(docs);
    _costCfgMemo = { at: Date.now(), cfg };
  }

  // ---- 1. refresh the catalogue list now and then ----
  if (Date.now() - state.builtAt > COST_REBUILD_MS) {
    state.sources = await rebuildCostWatch(env, state.sources, cfg, budget);
    state.builtAt = Date.now();
    state.retry = state.retry.filter(k => state.sources.some(s => s.k === k));
    ['sources', 'builtAt', 'retry'].forEach(k => dirty.add(k));
  }

  // ---- 2. look at a few catalogues; act only on the ones whose fingerprint changed ----
  const order = [...state.retry];
  const n = state.sources.length;
  if (n) {
    const slot = Math.floor(Date.now() / 60000);
    const start = (slot * COST_SOURCES_PER_TICK) % n;
    for (let i = 0; i < Math.min(COST_SOURCES_PER_TICK, n); i++) order.push(state.sources[(start + i) % n].k);
  }
  const cache = new Map();
  const seen = new Set();
  const retryNext = [];
  for (let idx = 0; idx < order.length; idx++) {
    const k = order[idx];
    if (seen.has(k)) continue;
    seen.add(k);
    const src = state.sources.find(s => s.k === k);
    if (!src) continue;
    if (budget.left() < COST_RESERVE + 3) {
      if (state.retry.includes(k)) retryNext.push(k);
      continue;
    }
    const map = await fetchLiveCostMap(env, src, cfg, cache, budget);
    if (!map) continue;                                             // supplier unreachable - next pass
    const fp = costFingerprint(map);
    if (fp === src.fp) continue;                                    // nothing changed at the supplier
    const res = await applyCostChangeForSource(env, src, cfg, cache, budget, state, counters);
    if (res === 'ok') { src.fp = fp; dirty.add('sources'); }
    else if (res === 'defer') retryNext.push(k);
  }
  if (JSON.stringify(retryNext) !== JSON.stringify(state.retry)) { state.retry = retryNext; dirty.add('retry'); }

  // ---- 3. announce: one message with a button per game, once the burst of changes has settled ----
  if (counters.added) {
    if (!state.rowsSince) state.rowsSince = new Date().toISOString();
    ['rows', 'rowsSince', 'rowsPkgs', 'rowsGames'].forEach(k => dirty.add(k));
  }
  if (state.rows.length) {
    const since = Date.parse(state.rowsSince) || Date.now();
    const settled = counters.added === 0;
    if ((settled || state.rows.length >= COST_FLUSH_BATCH || Date.now() - since > COST_FLUSH_MAX_WAIT_MS) && budget.left() > 3) {
      try {
        if (await flushCostRows(env, state, budget)) {
          state.rows = []; state.rowsSince = ''; state.rowsPkgs = 0; state.rowsGames = 0;
          ['rows', 'rowsSince', 'rowsPkgs', 'rowsGames'].forEach(k => dirty.add(k));
        }
      } catch (e) { console.error('Cost update announce failed:', e); }
    }
  }

  // ---- save only what changed ----
  if (dirty.size) {
    const fields = {};
    dirty.forEach(k => { fields[k] = state[k]; });
    await patchDoc(env, COST_WATCH_DOC, fields, Object.keys(fields));
  }
}



async function tgApi(env, method, payload) {
  const call = async (pl) => {
    const res = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(pl)
    });
    return res.json();
  };
  let out = await call(payload);
  // <tg-time> (Bot API 9.5 date/time) is a newer tag: if Telegram can't parse it, resend the
  // same message with the plain fallback text so a card is never lost because of it.
  const hasTime = (v) => typeof v === 'string' && v.includes('<tg-time');
  if (out && out.ok === false && /parse|entit/i.test(out.description || '') && (hasTime(payload.text) || hasTime(payload.caption))) {
    const strip = (v) => (typeof v === 'string' ? v.replace(/<tg-time\b[^>]*>([\s\S]*?)<\/tg-time>/g, '$1') : v);
    const pl = { ...payload };
    if ('text' in pl) pl.text = strip(pl.text);
    if ('caption' in pl) pl.caption = strip(pl.caption);
    out = await call(pl);
  }
  return out;
}

// Date/time shown in every viewer's own time zone (Bot API 9.5). Fallback text is Yangon time.
function tgTime(iso) {
  const ms = Date.parse(iso);
  if (!ms) return '';
  const fallback = new Date(ms).toLocaleString('en-GB', { timeZone: 'Asia/Yangon', day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false });
  return `<tg-time unix="${Math.floor(ms / 1000)}" format="wDT">${esc(fallback)}</tg-time>`;
}

// Long notices: in a group, an ephemeral message (Bot API 10.3) visible only to the admin who
// tapped; in a private chat (or if Telegram refuses it) the usual alert popup.
async function answerCallbackLong(env, cb, text) {
  const chat = cb && cb.message && cb.message.chat;
  if (chat && (chat.type === 'group' || chat.type === 'supergroup') && cb.from && cb.from.id) {
    try {
      const r = await tgApi(env, 'sendMessage', { chat_id: chat.id, text, ephemeral_message_parameters: { receiver_user_id: cb.from.id, callback_query_id: cb.id } });
      if (r && r.ok) { await answerCallback(env, cb.id); return; }
    } catch (e) { /* fall back to the popup below */ }
  }
  await answerCallback(env, cb.id, String(text).slice(0, 200), true);
}

async function answerCallback(env, id, text, showAlert = false) {
  try { await tgApi(env, 'answerCallbackQuery', { callback_query_id: id, text, show_alert: showAlert }); } catch (e) { /* non-critical */ }
}

// Live G2Bulk balance (no caching - always fresh). Used to show the balance
// on the Telegram order card and to block auto-placement when it can't
// cover the order (see resolveOrder below) - G2Bulk accepts an order into
// its queue even when the balance can't cover it and never auto-fails it
// back, so without this check a confirmed order would just sit
// "Processing" forever with no further signal.
async function fetchG2BulkBalance(g2bulk) {
  if (!g2bulk || !g2bulk.apiKey) return null;
  try {
    const res = await fetch('https://api.g2bulk.com/api/v2', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key: g2bulk.apiKey, action: 'balance' })
    });
    const data = await res.json();
    if (data.error) return null;
    const bal = Number(data.balance);
    return Number.isFinite(bal) ? bal : null; // missing/garbled balance = unknown, not 0 (0 would falsely block orders)
  } catch (err) {
    return null;
  }
}

function isG2BulkOrVoucherOrder(f) {
  return !!(f.g2bulkGameCode && f.g2bulkCatalogueName) || !!f.g2bulkVoucherProductId;
}

// balanceInfo: { balanceUsd, mmkEquivalent, insufficient } or null - built
// by sweepCollection() below from a live fetchG2BulkBalance() call, only
// for G2Bulk/voucher-linked orders.
async function buildBalanceInfo(g2bulk, priceMmk) {
  const liveBalanceUsd = await fetchG2BulkBalance(g2bulk);
  if (liveBalanceUsd === null) return null;
  const mmkEquivalent = liveBalanceUsd * (Number(g2bulk.usdRate) || 4500);
  return { balanceUsd: liveBalanceUsd, mmkEquivalent, insufficient: mmkEquivalent < Number(priceMmk || 0) };
}

function esc(s) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Icon + bold-label rows meant to sit inside a <blockquote> (order/topup
// cards) - replaces the old padEnd-aligned "Label  -  Value" layout, which
// only lined up in a monospace font; Telegram's blockquote text is
// proportional, so the padding just added uneven gaps instead of real
// alignment. An emoji per row plus a bold label reads at a glance instead.
function buildFieldList(rows) {
  return rows.map(([icon, label, value]) => `${icon} <b>${label}</b>  ${value}`).join('\n');
}

// Prefer the name resolved by looking the order/topup's userId up in the
// users collection (resolvedCustomerName, set by resolveCustomerName below
// and persisted onto the doc alongside telegramMessageId) over the
// possibly-stale/unset userName field the client wrote at checkout time.
function resolvedCustomerLabel(f) {
  return (f.resolvedCustomerName && f.resolvedCustomerName.trim())
    || (f.userName && f.userName.trim())
    || 'Guest';
}

function buildOrderMessage(id, f, balanceInfo) {
  const rows = [
    ['🎮', 'Game', esc(f.game) + (f.serverRegion ? ` (${esc(f.serverRegion)})` : '')],
    ['📦', 'Package', esc(f.itemAmount) + (Number(f.quantity) > 1 ? ` <b>×${Number(f.quantity)}</b>` : '') + (f.packageTab ? ` · ${esc(f.packageTab)}` : '')],
    ['💵', 'Price', '<b>' + Number(f.price || 0).toLocaleString() + ' MMK</b>'],
    ['🆔', 'Player ID', esc(f.playerId || 'N/A')],
    f.inGameName ? ['🏷️', 'In-Game Name', esc(f.inGameName)] : null,
    f.serverId ? ['🌐', 'Server ID', esc(f.serverId)] : null,
    ['💳', 'Payment', esc(f.paymentMethod || 'N/A')],
    ['👤', 'Customer', esc(resolvedCustomerLabel(f)) + (f.userPhone ? ' · ' + esc(f.userPhone) : '')],
    f.timestamp && tgTime(f.timestamp) ? ['🕐', 'Time', tgTime(f.timestamp)] : null
  ].filter(Boolean);
  // Live balance snapshot (G2Bulk/voucher-linked orders only) - shown right
  // on the order card so the admin can judge fulfillability before tapping
  // Confirm, without opening the app.
  const balanceLine = !balanceInfo ? null
    : balanceInfo.insufficient
      ? `⚠️ <b>G2Bulk Balance LOW:</b> ${balanceInfo.balanceUsd.toFixed(2)} USD (~${Math.round(balanceInfo.mmkEquivalent).toLocaleString()} MMK) — not enough to cover this order`
      : `💰 G2Bulk Balance: ${balanceInfo.balanceUsd.toFixed(2)} USD (~${Math.round(balanceInfo.mmkEquivalent).toLocaleString()} MMK)`;
  const text = [
    '🛒 <b>New Order</b> ✨',
    `<blockquote>${buildFieldList(rows)}</blockquote>`,
    balanceLine,
    `🔖 Order ID: <code>${esc(id)}</code>`
  ].filter(Boolean).join('\n');
  const buttons = [
    { text: '✅ Confirm', style: 'success', callback_data: `confirm:${id}` },
    { text: '❌ Reject', style: 'danger', callback_data: `reject:${id}` }
  ];
  const pid = String(f.playerId || '').trim().slice(0, 256);
  const copyRow = pid ? [{ text: '📋 Copy Player ID', copy_text: { text: pid } }] : [];
  return { text, buttons, copyRow, photo: f.receiptUrl || null, rich: buildOrderRich(id, f, balanceInfo) };
}

// Same card as a native bordered table (Bot API rich message): one "label | value" row per detail.
// Block text is literal, so no HTML escaping here. Sent by sweepCollection() (with the same buttons)
// and re-rendered by editResolvedMessage() when the order is confirmed / finished.
const ORDER_STATUS_LABEL = { Processing: '⏳ Processing', Success: '✅ Success', Fail: '❌ Failed' };
function buildOrderRich(id, f, balanceInfo) {
  const qty = Number(f.quantity) > 1 ? ` ×${Number(f.quantity)}` : '';
  const when = f.timestamp ? new Date(Date.parse(f.timestamp)).toLocaleString('en-GB', { timeZone: 'Asia/Yangon', day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false }) : '';
  const rows = [
    ['🎮 Game', String(f.game || '') + (f.serverRegion ? ` (${f.serverRegion})` : '')],
    ['📦 Package', String(f.itemAmount || '') + qty + (f.packageTab ? ` · ${f.packageTab}` : '')],
    ['💵 Price', Number(f.price || 0).toLocaleString() + ' MMK'],
    ['🆔 Player ID', String(f.playerId || 'N/A')],
    f.inGameName ? ['🏷️ In-Game Name', String(f.inGameName)] : null,
    f.serverId ? ['🌐 Server ID', String(f.serverId)] : null,
    ['💳 Payment', String(f.paymentMethod || 'N/A')],
    ['👤 Customer', resolvedCustomerLabel(f) + (f.userPhone ? ' · ' + f.userPhone : '')],
    when && when !== 'Invalid Date' ? ['🕐 Time', when] : null,
    ORDER_STATUS_LABEL[f.status] ? ['📌 Status', ORDER_STATUS_LABEL[f.status]] : null
  ].filter(Boolean);
  const cell = (text, header) => ({ text: String(text), is_header: !!header, align: 'left', valign: 'middle' });
  const blocks = [
    { type: 'heading', size: 2, text: f.status && f.status !== 'Pending' ? '🛒 Order' : '🛒 New Order ✨' },
    { type: 'table', is_bordered: true, cells: rows.map(([k, v]) => [cell(k, true), cell(v, false)]) }
  ];
  if (balanceInfo) {
    blocks.push({ type: 'paragraph', text: balanceInfo.insufficient
      ? `⚠️ G2Bulk Balance LOW: ${balanceInfo.balanceUsd.toFixed(2)} USD (~${Math.round(balanceInfo.mmkEquivalent).toLocaleString()} MMK) — not enough to cover this order`
      : `💰 G2Bulk Balance: ${balanceInfo.balanceUsd.toFixed(2)} USD (~${Math.round(balanceInfo.mmkEquivalent).toLocaleString()} MMK)` });
  }
  blocks.push({ type: 'paragraph', text: `🔖 Order ID: ${id}` });
  return { blocks };
}

function buildTopupMessage(id, f) {
  const rows = [
    ['💵', 'Amount', '<b>' + Number(f.amount || 0).toLocaleString() + ' MMK</b>'],
    ['💳', 'Payment', esc(f.paymentMethod || 'N/A')],
    ['👤', 'Customer', esc(resolvedCustomerLabel(f)) + (f.userPhone ? ' · ' + esc(f.userPhone) : '')],
    ...(f.timestamp && tgTime(f.timestamp) ? [['🕐', 'Time', tgTime(f.timestamp)]] : [])
  ];
  const text = [
    '💰 <b>Wallet Top Up Request</b>',
    `<blockquote>${buildFieldList(rows)}</blockquote>`,
    `🔖 Request ID: <code>${esc(id)}</code>`
  ].join('\n');
  const buttons = [
    { text: '✅ Confirm & Credit', style: 'success', callback_data: `wtopup_confirm:${id}` },
    { text: '❌ Reject', style: 'danger', callback_data: `wtopup_reject:${id}` }
  ];
  return { text, buttons, photo: f.receiptUrl || null };
}

// ---------------------------------------------------------------------------
// Cron sweep — send new Pending items that haven't been sent yet
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Cron overlap guard. runCronSweep() does dozens of sequential network calls
// (Telegram sends, provider polling, Firestore reads/writes) and can
// occasionally run past the 1-minute cron interval. Without a guard, the
// next tick starts while the previous one is still mid-flight, and both can
// read the same order's status before either has written pushNotifiedAt —
// causing the same customer to get the same push notification twice.
// This uses a single Firestore doc as a simple mutex: a tick only proceeds
// if the lock is free or stale (previous run crashed/hung), and always
// releases the lock when done, success or failure.
const CRON_LOCK_STALE_MS = 55 * 1000; // shorter than the 60s cron interval

async function runCronSweepLocked(env) {
  const gotLock = await acquireCronLock(env);
  if (!gotLock) {
    console.log('Cron sweep skipped — previous run still in progress');
    return;
  }
  try {
    await runCronSweep(env);
  } finally {
    await releaseCronLock(env);
  }
}

async function acquireCronLock(env) {
  try {
    const lockDoc = await getDoc(env, 'settings/cronLock');
    const lockedAt = lockDoc && lockDoc.fields && lockDoc.fields.lockedAt;
    if (lockedAt) {
      const age = Date.now() - new Date(lockedAt).getTime();
      if (age < CRON_LOCK_STALE_MS) return false; // another run is genuinely still active
    }
    await patchDoc(env, 'settings/cronLock', { lockedAt: new Date().toISOString() }, ['lockedAt']);
    return true;
  } catch (err) {
    console.error('acquireCronLock error', err);
    return true; // fail open — a missed lock is better than notifications never sending at all
  }
}

async function releaseCronLock(env) {
  try {
    await patchDoc(env, 'settings/cronLock', { lockedAt: null }, ['lockedAt']);
  } catch (err) {
    console.error('releaseCronLock error', err);
  }
}

// Separate, SHORT lock just for sending order / top-up cards and wallet
// auto-fulfil. Before, the instant ping (a customer just placed an order) had
// to wait for the same lock the whole cron tick holds - and a tick also polls
// G2Bulk / FazerCards, refreshes caches, runs backups and cost updates - so a
// new order could sit 10-60 seconds behind that. Now the heavy steps hold
// only the cron lock; card sending holds this one, so pings never queue
// behind them. Cron and ping still share THIS lock, so a card is never sent twice.
const SEND_LOCK_STALE_MS = 40 * 1000;

async function acquireSendLock(env) {
  try {
    const lockDoc = await getDoc(env, 'settings/sendLock');
    const lockedAt = lockDoc && lockDoc.fields && lockDoc.fields.lockedAt;
    if (lockedAt) {
      const age = Date.now() - new Date(lockedAt).getTime();
      if (age < SEND_LOCK_STALE_MS) return false;
    }
    await patchDoc(env, 'settings/sendLock', { lockedAt: new Date().toISOString() }, ['lockedAt']);
    return true;
  } catch (err) {
    console.error('acquireSendLock error', err);
    return true; // fail open, same as the cron lock
  }
}

async function releaseSendLock(env) {
  try {
    await patchDoc(env, 'settings/sendLock', { lockedAt: null }, ['lockedAt']);
  } catch (err) {
    console.error('releaseSendLock error', err);
  }
}

async function runCronSweep(env) {
  _pendingQueryCache = new Map(); // per-tick memo for queryPending (cleared in the finally below)
  try {
    await runCronSweepBody(env);
  } finally {
    _pendingQueryCache = null;
  }
}

async function runCronSweepBody(env) {
  try {
    await runCronSweepSteps(env);
  } finally {
    // Runs even if an earlier step threw, so the 4-hourly Cost update can't be skipped by an unrelated failure.
    // It is last in the tick and only spends a small subrequest budget (see COST_TICK_BUDGET).
    try { await runAutoCostUpdateSweep(env); } catch (e) { console.error('runAutoCostUpdateSweep error', e); }
  }
}

async function runCronSweepSteps(env) {
  // Cards + wallet auto-fulfil first, under the short send lock. If a ping
  // already holds it, that ping is sending right now - skip, don't wait.
  if (await acquireSendLock(env)) {
    try {
      await sweepCollection(env, 'orders', buildOrderMessage);
      await sweepCollection(env, 'walletTopups', buildTopupMessage);
      await fulfillWalletOrders(env);
    } catch (e) {
      console.error('cron send steps failed', e); // don't let this block the polling steps below
    } finally {
      await releaseSendLock(env);
    }
  }
  await reconcileWalletDebits(env);
  await pollPendingVoucherOrders(env);
  try { await refreshG2BulkCache(env); } catch (e) { console.error('g2bulk cache refresh failed', e); }
  await pollPendingFazerCardOrders(env);
  try { await pollPendingG2BulkDirectOrders(env); } catch (e) { console.error('direct G2Bulk poll failed', e); }
  try { await reconcileTelegramCards(env); } catch (e) { console.error('card reconcile failed', e); }
  await checkDailySummarySchedule(env);
  await checkWeeklySummarySchedule(env);
  await runFirestoreBackupSweep(env);
}

// ---------------------------------------------------------------------------
// Instant ping - POST /ping  { type: 'order' | 'topup', id }
// ---------------------------------------------------------------------------
// Called by Store.html right after it saves an order / wallet top-up. It runs
// the exact same sweep functions the cron tick runs for that item type
// (Telegram card for receipt orders/top-ups, auto-fulfil for wallet orders),
// so the ~1 minute cron wait is gone for the main customer flow. The cron is
// kept only as a safety net for anything a ping missed (browser closed right
// after submit, Worker briefly down, etc).
//
// The browser can't hold a secret, so this route is public. It stays cheap and
// harmless to spam because it (1) only accepts a real, still-Pending doc id,
// (2) does nothing for docs the cron would skip anyway, (3) de-dupes the same
// id per isolate, and (4) reuses the cron lock so it can never overlap a run.
const PING_RECENT = new Map(); // "type:id" -> last accepted ms (per isolate)
const PING_DEDUPE_MS = 15 * 1000;

async function handlePing(request, env, ctx) {
  const pingIp = request.headers.get('CF-Connecting-IP') || 'unknown';
  if (await rateLimitHit(env, 'ping', pingIp, 120, 60)) return corsJson({ ok: false, error: 'Too many requests' }, 429);
  let body;
  try { body = await request.json(); } catch (e) { return corsJson({ ok: false, error: 'Bad request' }, 400); }
  const type = String((body && body.type) || '');
  const id = String((body && body.id) || '');
  if ((type !== 'order' && type !== 'topup') || !/^[A-Za-z0-9_-]{1,64}$/.test(id)) {
    return corsJson({ ok: false, error: 'Bad request' }, 400);
  }

  const key = `${type}:${id}`;
  const now = Date.now();
  if (now - (PING_RECENT.get(key) || 0) < PING_DEDUPE_MS) return corsJson({ ok: true, skipped: 'recent' });
  PING_RECENT.set(key, now);
  if (PING_RECENT.size > 500) {
    for (const [k, t] of PING_RECENT) if (now - t > PING_DEDUPE_MS) PING_RECENT.delete(k);
  }

  const doc = await getDoc(env, type === 'order' ? `orders/${id}` : `walletTopups/${id}`);
  if (!doc || doc.fields.status !== 'Pending') return corsJson({ ok: true, skipped: 'not pending' });
  const f = doc.fields;
  const needsWork = type === 'order'
    ? (f.paidFromWallet === true || !f.telegramMessageId)
    : (!f.telegramMessageId && !f.fazercardPaymentId);
  if (!needsWork) return corsJson({ ok: true, skipped: 'already handled' });

  ctx.waitUntil(runPingSweep(env, type));
  return corsJson({ ok: true, accepted: true }, 202);
}

async function runPingSweep(env, type) {
  // If the cron's send step (or another ping) holds the send lock, wait ~1.5s and try
  // again (up to ~18s). A burst of pings coalesces naturally: the first one to get the lock
  // sweeps EVERY pending item, so the later ones usually find nothing left.
  for (let attempt = 0; attempt < 12; attempt++) {
    if (await acquireSendLock(env)) {
      _pendingQueryCache = new Map();
      try {
        if (type === 'order') {
          await sweepCollection(env, 'orders', buildOrderMessage);
          await fulfillWalletOrders(env);
        } else {
          await sweepCollection(env, 'walletTopups', buildTopupMessage);
        }
      } catch (err) {
        console.error('ping sweep failed', type, err);
      } finally {
        _pendingQueryCache = null;
        await releaseSendLock(env);
      }
      return;
    }
    await new Promise(r => setTimeout(r, 1500));
  }
  console.log('ping: cron lock stayed busy - leaving it to the next cron tick');
}

// Looks the order/topup's userId up in the users collection so the
// Telegram card can show the account's real name (users/{uid}.name),
// instead of just the userName string the client happened to send at
// checkout (which can be blank, stale, or spoofed). Falls back to
// guestId/uid. Cached per sweep
// call so the same customer isn't re-fetched for every pending doc.
async function resolveCustomerName(env, userId, cache) {
  if (!userId) return null;
  if (cache.has(userId)) return cache.get(userId);
  let name = null;
  try {
    const userDoc = await getDoc(env, `users/${userId}`);
    if (userDoc && userDoc.fields) {
      name = (userDoc.fields.name && userDoc.fields.name.trim())
        ? userDoc.fields.name.trim()
        : (userDoc.fields.guestId || userId);
    }
  } catch (err) {
    console.error('resolveCustomerName failed', userId, err);
  }
  cache.set(userId, name);
  return name;
}

async function sweepCollection(env, collectionId, buildMessage) {
  const docs = await queryPending(env, collectionId);
  let g2bulkSettings = null; // fetched lazily, once per sweep call, only if a G2Bulk/voucher order is actually seen
  const customerNameCache = new Map(); // userId -> resolved name, shared across this sweep call
  for (const d of docs) {
    if (d.fields.telegramMessageId) continue; // already sent, skip

    // Wallet-paid orders skip Telegram entirely - the customer already paid
    // out of their wallet balance, so there's nothing for the admin to
    // review/approve. fulfillWalletOrders() (called right after this sweep)
    // handles placing/completing these automatically.
    if (collectionId === 'orders' && d.fields.paidFromWallet === true) continue;

    // Crypto top-ups (created by the fazercards-functions Worker's
    // /fazercard-payment-create route) also skip Telegram entirely - they
    // are only ever supposed to move to "Completed" once FazerCards' own
    // payment status confirms it (see that Worker's /fazercard-payment-status
    // route), never via a human tapping Confirm. If this were sent to
    // Telegram, an admin could accidentally credit an invoice nobody has
    // actually paid yet. resolveTopupConfirm() below also refuses these as a
    // second layer of defense, in case a doc from before this fix still has
    // a stale telegramMessageId-less state.
    if (collectionId === 'walletTopups' && d.fields.fazercardPaymentId) continue;

    let balanceInfo = null;
    if (collectionId === 'orders' && isG2BulkOrVoucherOrder(d.fields)) {
      if (g2bulkSettings === null) {
        const g2bulkDoc = await getDoc(env, 'settings/g2bulk');
        g2bulkSettings = (g2bulkDoc && g2bulkDoc.fields) || {};
      }
      balanceInfo = await buildBalanceInfo(g2bulkSettings, d.fields.price);
    }

    const resolvedCustomerName = await resolveCustomerName(env, d.fields.userId, customerNameCache);
    if (resolvedCustomerName) d.fields.resolvedCustomerName = resolvedCustomerName;

    const { text, buttons, photo, copyRow, rich } = buildMessage(d.id, d.fields, balanceInfo);

    // Second row: one-tap "Copy Player ID" (native copy button) on order cards.
    const kb = (withCopy) => ({ inline_keyboard: [buttons, ...(withCopy && copyRow && copyRow.length ? [copyRow] : [])] });
    // Orders: bordered table card (receipt photo, if any, goes first as its own small message).
    // Anything Telegram refuses falls through to the classic text/photo card below, so a card is never lost.
    if (collectionId === 'orders' && rich) {
      let receiptMsgId = null;
      if (photo) {
        const pr = await tgApi(env, 'sendPhoto', { chat_id: env.TELEGRAM_CHAT_ID, photo, caption: `🧾 Receipt · ${d.id.slice(0, 8)}` });
        if (pr && pr.ok) receiptMsgId = pr.result.message_id;
      }
      const richPayload = (withCopy) => ({ chat_id: env.TELEGRAM_CHAT_ID, rich_message: rich, reply_markup: kb(withCopy), ...(receiptMsgId ? { reply_parameters: { message_id: receiptMsgId, allow_sending_without_reply: true } } : {}) });
      let rr = await tgApi(env, 'sendRichMessage', richPayload(true));
      if (!(rr && rr.ok) && copyRow && copyRow.length && /button|markup|copy/i.test((rr && rr.description) || '')) rr = await tgApi(env, 'sendRichMessage', richPayload(false));
      if (rr && rr.ok) {
        const pf = { telegramMessageId: rr.result.message_id, telegramChatId: rr.result.chat.id, telegramMessageIsPhoto: false, telegramIsRich: true };
        const pm = ['telegramMessageId', 'telegramChatId', 'telegramMessageIsPhoto', 'telegramIsRich'];
        if (resolvedCustomerName) { pf.resolvedCustomerName = resolvedCustomerName; pm.push('resolvedCustomerName'); }
        await patchDoc(env, `${collectionId}/${d.id}`, pf, pm);
        continue;
      }
      console.error('rich order card failed, using classic card', d.id, JSON.stringify(rr));
    }
    const payload = { chat_id: env.TELEGRAM_CHAT_ID, parse_mode: 'HTML', reply_markup: kb(true) };
    const sendCard = () => photo
      ? tgApi(env, 'sendPhoto', { ...payload, photo, caption: text })
      : tgApi(env, 'sendMessage', { ...payload, text });
    let result = await sendCard();
    // If Telegram refuses the keyboard (e.g. the copy button), resend without the copy row so the
    // order card itself is never lost.
    if (!result.ok && copyRow && copyRow.length && /button|markup|copy/i.test(result.description || '')) {
      payload.reply_markup = kb(false);
      result = await sendCard();
    }
    // Whether the message Telegram actually created is a photo message
    // (caption) or a plain text message - starts matching the attempt
    // above, but gets corrected to false if the sendPhoto attempt fails
    // and we fall back to text below. This is persisted on the doc
    // (telegramMessageIsPhoto) so editResolvedMessage() later knows which
    // Telegram API method (editMessageCaption vs editMessageText) actually
    // applies to this message, instead of re-guessing from receiptUrl.
    let sentAsPhoto = !!photo && result.ok;

    // sendPhoto can fail on its own (bad/unreachable receipt URL, oversized
    // image, Telegram rejecting the host, etc.) even though the order itself
    // is completely fine. Without a fallback, that failure used to leave the
    // doc stuck in an infinite retry loop every cron tick - telegramMessageId
    // never gets set, so the admin NEVER sees a "New Order" card for it (the
    // order still shows up in the Daily Summary later, which pulls straight
    // from Firestore, not from this send-loop - that's why it can look like
    // only the summary "knows" about the order). If the photo send fails,
    // retry once as a plain text message so the order is never silently lost
    // - the admin can still open the order in the admin panel to see the
    // receipt image itself.
    if (!result.ok && photo) {
      console.error('sendPhoto failed, falling back to text', collectionId, d.id, JSON.stringify(result));
      const fallbackText = `${text}\n⚠️ <i>Receipt photo failed to load - check the order in the admin panel.</i>`;
      result = await tgApi(env, 'sendMessage', { ...payload, text: fallbackText });
      sentAsPhoto = false;
    }

    if (result.ok) {
      const patchFields = { telegramMessageId: result.result.message_id, telegramChatId: result.result.chat.id, telegramMessageIsPhoto: sentAsPhoto };
      const patchMask = ['telegramMessageId', 'telegramChatId', 'telegramMessageIsPhoto'];
      // Persist the resolved name onto the doc itself (same as
      // telegramMessageId) so that later reads - e.g. resolveOrder/
      // resolveTopup re-reading the doc before editResolvedMessage - still
      // have it and the Confirmed/Rejected message keeps showing the real
      // name instead of falling back to userName/Guest.
      if (resolvedCustomerName) {
        patchFields.resolvedCustomerName = resolvedCustomerName;
        patchMask.push('resolvedCustomerName');
      }
      await patchDoc(env, `${collectionId}/${d.id}`, patchFields, patchMask);
    } else {
      console.error('Telegram send failed', collectionId, d.id, JSON.stringify(result));
    }
  }
}

// ---------------------------------------------------------------------------
// Wallet order auto-fulfillment — no admin panel needed
// ---------------------------------------------------------------------------
// Wallet-paid orders (paidFromWallet: true) have nothing left to review -
// the customer already paid out of their wallet balance at checkout. This
// mirrors what clicking "Start Processing" in the admin panel does:
//   - G2Bulk-linked package  -> place the order with G2Bulk's API
//   - JCPlays-linked package -> place the order with JCPlays' API
//   - neither (manual game)  -> just move straight to "Processing"
// Reads the same settings/g2bulk and settings/jcplays Firestore docs the
// admin panel already writes to, so nothing needs configuring twice.
// Completion for G2Bulk orders (Processing -> Success/Fail) still comes
// through your existing G2Bulk webhook Worker, same as always - this only
// handles the initial placement, which used to require someone to have the
// admin panel open.

// ---------------------------------------------------------------------------
// Daily Order Summary — sent at exactly midnight (Yangon time), even with
// the admin panel fully closed
// ---------------------------------------------------------------------------
// Mirrors admin.html's sendDailySummaryToTelegram()/checkDailySummarySchedule()
// exactly (same message format, same settings/dailySummary.lastRunDate guard
// so the two never double-send), but runs off this Worker's 1-minute cron
// instead of a browser tab being open during the midnight hour — so "today's
// orders get sent the instant it turns 00:00" is guaranteed rather than
// best-effort. admin.html's own automatic scheduler has been turned off in
// favor of this (its manual "Send Today's Summary Now" test button still
// works, and doesn't touch the lastRunDate guard, same as before).

const YANGON_OFFSET_MINUTES = 6 * 60 + 30; // Asia/Yangon is a fixed +6:30, no DST

function yangonNow() {
  return new Date(Date.now() + YANGON_OFFSET_MINUTES * 60 * 1000);
}

function yangonDateStr(yangonShiftedDate) {
  // yangonShiftedDate is a Date object already shifted by +6:30 (see
  // yangonNow()/above) — read its UTC getters so the shift isn't re-applied.
  const y = yangonShiftedDate.getUTCFullYear();
  const m = String(yangonShiftedDate.getUTCMonth() + 1).padStart(2, '0');
  const d = String(yangonShiftedDate.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

// Real UTC instant for 00:00 Yangon time on the given Yangon calendar day.
function yangonMidnightUtcIso(yangonDateStrValue) {
  const [y, m, d] = yangonDateStrValue.split('-').map(Number);
  const utcMs = Date.UTC(y, m - 1, d) - YANGON_OFFSET_MINUTES * 60 * 1000;
  return new Date(utcMs).toISOString();
}

// Best-effort cost lookup for an order, mirroring admin.html's
// resolveOrderCost() exactly (see that function's comment): orders don't
// store a cost snapshot, so match the order back to its game's current
// package list (exact price match first, falling back to matching the
// package's amount label inside the order's itemAmount text). Returns null
// when no package can be matched, so that order can be excluded from the
// profit total instead of silently counting as 0 cost (which would inflate
// profit).
function resolveOrderCostWorker(games, o) {
  const g = games.find(x => x.fields.name === o.fields.game);
  if (!g || !Array.isArray(g.fields.packages)) return null;
  let p = g.fields.packages.find(pkg => Number(pkg.price) === Number(o.fields.price));
  if (!p && o.fields.itemAmount) {
    p = g.fields.packages.find(pkg => String(o.fields.itemAmount).includes(String(pkg.amount)));
  }
  return p ? (Number(p.cost) || 0) : null;
}

// Groups a period's Success orders by game and computes Sales/Cost/Profit/
// Margin% per game, using the same resolveOrderCostWorker() package-cost
// lookup the aggregate totals above use. Sorted worst-margin-first so a
// thinning or negative margin on any one game surfaces at the top instead
// of getting buried under high-volume winners.
function buildPerGameProfitReport(games, periodOrders) {
  const bySuccess = periodOrders.filter(o => o.fields.status === 'Success');
  const map = new Map();
  for (const o of bySuccess) {
    const name = o.fields.game || 'Unknown';
    if (!map.has(name)) map.set(name, { sales: 0, cost: 0, count: 0, unmatched: 0 });
    const entry = map.get(name);
    entry.count++;
    entry.sales += Number(o.fields.price || 0);
    const cost = resolveOrderCostWorker(games, o);
    if (cost === null) { entry.unmatched++; continue; }
    entry.cost += cost;
  }
  return [...map.entries()]
    .map(([game, v]) => ({
      game, count: v.count, sales: v.sales, cost: v.cost,
      profit: v.sales - v.cost,
      marginPct: v.sales > 0 ? ((v.sales - v.cost) / v.sales) * 100 : 0,
      unmatched: v.unmatched
    }))
    .sort((a, b) => a.marginPct - b.marginPct);
}

// Posts the per-game breakdown as its own message (chunked under Telegram's
// 4096-char limit), separate from the aggregate summary above so the
// aggregate stays short/glanceable and this stays a detail view under it.
async function sendGameProfitBreakdownToTelegram(env, games, periodOrders) {
  const report = buildPerGameProfitReport(games, periodOrders);
  if (!report.length) return;

  const rows = report.map((r, i) => {
    const marginFlag = r.marginPct < 0 ? '🔴' : r.marginPct < 5 ? '🟡' : '🟢';
    const unmatchedNote = r.unmatched ? ` (${r.unmatched} unmatched)` : '';
    return `${marginFlag} ${esc(r.game)} — ${r.count}x — Sales ${r.sales.toLocaleString()} — Profit ${r.profit.toLocaleString()} — ${r.marginPct.toFixed(1)}%${unmatchedNote}`;
  });

  const CHUNK = 12;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const text = i === 0
      ? `📦 <b>Per-Game Profit</b> (worst margin first)\n<blockquote>${chunk.join('\n')}</blockquote>`
      : `<blockquote>${chunk.join('\n')}</blockquote>`;
    await tgApi(env, 'sendMessage', { chat_id: env.TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' });
  }
}

// Daily/Weekly summary as ONE native rich message: stats table, per-game profit table and a
// collapsible order list (times shown in each viewer's own time zone). Returns 'full' when
// everything was delivered, 'partial' when only stats + per-game went out (too many orders for
// one message), or false when Telegram refused it -> caller falls back to the old messages.
const ORDERS_IN_RICH = 250;
async function trySendRichSummary(env, { periodLabel, periodStr, statsRows, unmatchedCount, games, periodOrders }) {
  try {
    const hc = (text, align) => ({ text, is_header: true, align, valign: 'middle' });
    const c = (text, align = 'left') => ({ text, align, valign: 'middle' });
    const blocks = [
      { type: 'heading', size: 2, text: `📊 ${periodLabel} — ${periodStr}` },
      { type: 'table', is_bordered: true, is_striped: true, is_compact: true,
        cells: statsRows.map(([icon, label, value]) => [c(`${icon} ${label}`), c(String(value), 'right')]) }
    ];
    if (unmatchedCount > 0) blocks.push({ type: 'paragraph', text: `⚠️ ${unmatchedCount} order(s) couldn't be matched to a package cost — excluded from Cost/Profit above.` });
    const report = buildPerGameProfitReport(games, periodOrders).slice(0, 100);
    if (report.length) {
      blocks.push({ type: 'heading', size: 3, text: '📦 Per-Game Profit (worst margin first)' });
      blocks.push({ type: 'table', is_bordered: true, is_striped: true, is_compact: true, cells: [
        [hc('Game', 'left'), hc('Orders', 'right'), hc('Sales', 'right'), hc('Profit', 'right'), hc('Margin', 'right')],
        ...report.map(r => {
          const flag = r.marginPct < 0 ? '🔴' : r.marginPct < 5 ? '🟡' : '🟢';
          return [c(r.game + (r.unmatched ? ` (${r.unmatched} unmatched)` : '')), c(String(r.count), 'right'), c(r.sales.toLocaleString(), 'right'), c(r.profit.toLocaleString(), 'right'), c(`${flag} ${r.marginPct.toFixed(1)}%`, 'right')];
        })
      ] });
    }
    const withOrders = periodOrders.length > 0 && periodOrders.length <= ORDERS_IN_RICH;
    if (withOrders) {
      const sorted = periodOrders.slice().sort((a, b) => (a.fields.timestamp ? Date.parse(a.fields.timestamp) : 0) - (b.fields.timestamp ? Date.parse(b.fields.timestamp) : 0));
      const rows = sorted.map((o, i) => {
        const ms = o.fields.timestamp ? Date.parse(o.fields.timestamp) : 0;
        const fb = ms ? new Date(ms).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Yangon' }) : '--:--';
        const timeCell = ms ? { type: 'date_time', text: fb, unix_time: Math.floor(ms / 1000), date_time_format: 't' } : fb;
        return [c(String(i + 1), 'right'), c(timeCell), c(String(o.fields.game || '')), c(String(o.fields.itemAmount || '')), c(Number(o.fields.price || 0).toLocaleString(), 'right'), c(String(o.fields.status || ''))];
      });
      blocks.push({ type: 'details', summary: `🧾 Orders (${sorted.length})`, is_open: false, blocks: [
        { type: 'table', is_bordered: true, is_striped: true, is_compact: true, cells: [
          [hc('#', 'right'), hc('Time', 'left'), hc('Game', 'left'), hc('Package', 'left'), hc('Price', 'right'), hc('Status', 'left')], ...rows] }
      ] });
    }
    const r = await tgApi(env, 'sendRichMessage', { chat_id: env.TELEGRAM_CHAT_ID, rich_message: { blocks } });
    if (!(r && r.ok)) { console.error('rich summary failed, using classic messages', JSON.stringify(r)); return false; }
    return periodOrders.length <= ORDERS_IN_RICH ? 'full' : 'partial';
  } catch (e) {
    console.error('rich summary error', e);
    return false;
  }
}

async function sendPeriodSummaryToTelegram(env, periodLabel, periodStr, periodOrders) {
  const bySuccess = periodOrders.filter(o => o.fields.status === 'Success');
  const byFail = periodOrders.filter(o => o.fields.status === 'Fail');
  const byPending = periodOrders.filter(o => o.fields.status === 'Pending');
  const byProcessing = periodOrders.filter(o => o.fields.status === 'Processing');
  const totalSales = bySuccess.reduce((sum, o) => sum + Number(o.fields.price || 0), 0);

  // Split Total Sales into Cost (what was paid to the fulfillment provider)
  // and Profit (what's actually kept), same package-cost lookup the
  // Dashboard's "Today's Profit" stat card uses - previously this summary
  // only ever showed the raw sale total, with cost and profit mixed
  // together and no way to tell them apart.
  const games = await queryAllDocs(env, 'games');
  let totalCost = 0, matchedSales = 0, unmatchedCount = 0;
  for (const o of bySuccess) {
    const cost = resolveOrderCostWorker(games, o);
    if (cost === null) { unmatchedCount++; continue; }
    totalCost += cost;
    matchedSales += Number(o.fields.price || 0);
  }
  const totalProfit = matchedSales - totalCost;

  const statsRows = [
    ['📦', 'Total Orders', String(periodOrders.length)],
    ['✅', 'Success', String(bySuccess.length)],
    ['⏳', 'Processing', String(byProcessing.length)],
    ['🕓', 'Pending', String(byPending.length)],
    ['❌', 'Fail', String(byFail.length)],
    ['💰', 'Total Sales', totalSales.toLocaleString() + ' MMK'],
    ['💵', 'Total Cost', totalCost.toLocaleString() + ' MMK'],
    ['📈', 'Net Profit', totalProfit.toLocaleString() + ' MMK']
  ];
  const caveat = unmatchedCount > 0
    ? `\n⚠️ <i>${unmatchedCount} order(s) couldn't be matched to a package cost — excluded from Cost/Profit above.</i>`
    : '';
  const headerText = [
    `📊 <b>${periodLabel}</b> — ${periodStr}`,
    `<blockquote>${buildFieldList(statsRows)}</blockquote>${caveat}`
  ].join('\n');

  const richRes = await trySendRichSummary(env, { periodLabel, periodStr, statsRows, unmatchedCount, games, periodOrders });
  if (richRes === 'full') return;

  if (richRes !== 'partial') await tgApi(env, 'sendMessage', { chat_id: env.TELEGRAM_CHAT_ID, text: headerText, parse_mode: 'HTML' });

  if (!periodOrders.length) return;

  if (richRes !== 'partial') await sendGameProfitBreakdownToTelegram(env, games, periodOrders);

  const sorted = periodOrders.slice().sort((a, b) => {
    const ta = a.fields.timestamp ? Date.parse(a.fields.timestamp) : 0;
    const tb = b.fields.timestamp ? Date.parse(b.fields.timestamp) : 0;
    return ta - tb;
  });
  const CHUNK = 10; // keep each Telegram message well under the 4096 char limit
  for (let i = 0; i < sorted.length; i += CHUNK) {
    const chunk = sorted.slice(i, i + CHUNK);
    const rows = chunk.map((o, idx) => {
      const timeStr = o.fields.timestamp
        ? new Date(o.fields.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Yangon' })
        : '--:--';
      return `${i + idx + 1}. ${timeStr} — ${esc(o.fields.game)} (${esc(o.fields.itemAmount)}) — ${Number(o.fields.price || 0).toLocaleString()} MMK — ${o.fields.status}`;
    });
    const text = `<blockquote>${rows.join('\n')}</blockquote>`;
    await tgApi(env, 'sendMessage', { chat_id: env.TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' });
  }
}

async function checkDailySummarySchedule(env, force = false) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) return;

  const now = yangonNow();
  if (!force && now.getUTCHours() !== 0) return; // only act during the 00:00–00:59 Yangon window
  const todayStr = yangonDateStr(now);

  if (!force) {
    const settingsDoc = await getDoc(env, 'settings/dailySummary');
    if (settingsDoc && settingsDoc.fields.lastRunDate === todayStr) return; // already sent today
  }

  const yesterdayStr = yangonDateStr(new Date(now.getTime() - 24 * 60 * 60 * 1000));
  const startIso = yangonMidnightUtcIso(yesterdayStr);
  const endIso = yangonMidnightUtcIso(todayStr);
  const dayOrders = await queryByTimestampRange(env, 'orders', startIso, endIso);

  await sendPeriodSummaryToTelegram(env, 'Daily Order Summary', yesterdayStr, dayOrders);
  await patchDoc(env, 'settings/dailySummary', { lastRunDate: todayStr }, ['lastRunDate']);
}

// Same dedupe pattern as checkDailySummarySchedule, gated to the Monday
// 00:00-00:59 Yangon window instead of every day, covering the 7 days just
// ended (last Monday 00:00 -> this Monday 00:00 Yangon). Dedup key is the
// ISO week's Monday date string, stored separately from
// settings/dailySummary so the two schedules never interfere.
// force=true (from /run-sweep?force=weekly) skips both the day/hour gate
// and the dedupe check, for testing without waiting for an actual Monday.
async function checkWeeklySummarySchedule(env, force = false) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) return;

  const now = yangonNow();
  if (!force && (now.getUTCHours() !== 0 || now.getUTCDay() !== 1)) return; // only Monday 00:00-00:59 Yangon
  const thisMondayStr = yangonDateStr(now);

  if (!force) {
    const settingsDoc = await getDoc(env, 'settings/weeklySummary');
    if (settingsDoc && settingsDoc.fields.lastRunWeekStart === thisMondayStr) return; // already sent this week
  }

  const lastMondayStr = yangonDateStr(new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000));
  const startIso = yangonMidnightUtcIso(lastMondayStr);
  const endIso = yangonMidnightUtcIso(thisMondayStr);
  const weekOrders = await queryByTimestampRange(env, 'orders', startIso, endIso);

  await sendPeriodSummaryToTelegram(env, 'Weekly Order Summary', `${lastMondayStr} → ${thisMondayStr}`, weekOrders);
  await patchDoc(env, 'settings/weeklySummary', { lastRunWeekStart: thisMondayStr }, ['lastRunWeekStart']);
}

// ---------------------------------------------------------------------------
// Weekly Firestore backup — dumps every collection below to one JSON file
// (gzip'd) and sends it to the admin Telegram chat as a document. No GCS/R2
// setup needed since it reuses the Bot API token this Worker already has.
// Telegram's Bot API caps uploads at 50MB; past that this sweep should move
// to per-collection files or a Cloudflare R2 upload instead (see the
// oversize guard in runFirestoreBackupSweep below).
// ---------------------------------------------------------------------------

const BACKUP_COLLECTIONS = [
  'orders', 'ordersArchive', 'users', 'userStats', 'games', 'walletTopups',
  'notifications', 'paymentMethods',
  'announcements', 'adminUsernames', 'googleAccounts', 'settings'
];

async function sendTelegramDocument(env, filename, bytes, caption) {
  const form = new FormData();
  form.append('chat_id', env.TELEGRAM_CHAT_ID);
  if (caption) form.append('caption', caption);
  form.append('document', new Blob([bytes], { type: 'application/gzip' }), filename);
  const res = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendDocument`, {
    method: 'POST',
    body: form
  });
  return res.json();
}

async function runFirestoreBackupSweep(env, force = false) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) return;

  const now = yangonNow();
  if (!force && (now.getUTCHours() !== 0 || now.getUTCDay() !== 0)) return; // Sunday 00:00-00:59 Yangon only
  const todayStr = yangonDateStr(now);

  if (!force) {
    const settingsDoc = await getDoc(env, 'settings/firestoreBackup');
    if (settingsDoc && settingsDoc.fields.lastRunDate === todayStr) return; // already ran today
  }

  // Mark the run BEFORE doing the (slow, many-collection) export so a
  // second overlapping cron tick can't start a duplicate backup while this
  // one is still in flight - same guard pattern as the other schedule
  // checks above.
  await patchDoc(env, 'settings/firestoreBackup', { lastRunDate: todayStr }, ['lastRunDate']);

  const dump = {};
  let totalDocs = 0;
  for (const collectionId of BACKUP_COLLECTIONS) {
    try {
      const docs = await queryAllDocs(env, collectionId, 5000);
      dump[collectionId] = docs.map(d => ({ id: d.id, fields: d.fields }));
      totalDocs += docs.length;
    } catch (err) {
      console.error('runFirestoreBackupSweep: failed to dump', collectionId, err);
      dump[collectionId] = { error: String(err) };
    }
  }

  const json = JSON.stringify(dump);
  const rawBytes = new TextEncoder().encode(json);

  // Gzip before upload - JSON compresses heavily (mostly repeated field
  // names), which buys a lot of headroom under Telegram's 50MB cap as the
  // store's data grows.
  let uploadBytes = rawBytes;
  let filename = `pcastore-backup-${todayStr}.json`;
  try {
    const cs = new CompressionStream('gzip');
    const compressedStream = new Blob([rawBytes]).stream().pipeThrough(cs);
    uploadBytes = new Uint8Array(await new Response(compressedStream).arrayBuffer());
    filename += '.gz';
  } catch (err) {
    console.error('runFirestoreBackupSweep: gzip failed, uploading raw JSON', err);
  }

  const sizeMb = (uploadBytes.byteLength / (1024 * 1024)).toFixed(2);
  const caption = `🗄 Weekly Firestore backup — ${todayStr}\n${totalDocs} docs across ${BACKUP_COLLECTIONS.length} collections — ${sizeMb} MB`;

  if (uploadBytes.byteLength > 49 * 1024 * 1024) {
    await tgApi(env, 'sendMessage', {
      chat_id: env.TELEGRAM_CHAT_ID,
      text: `⚠️ Firestore backup for ${todayStr} is ${sizeMb} MB — over Telegram's 50MB upload limit, so it wasn't sent. Time to move backups to Cloudflare R2.`
    });
    return;
  }

  const result = await sendTelegramDocument(env, filename, uploadBytes, caption);
  if (!result.ok) console.error('runFirestoreBackupSweep: Telegram upload failed', JSON.stringify(result));
}

async function fulfillWalletOrders(env) {
  const pending = (await queryPending(env, 'orders')).filter(d => d.fields.paidFromWallet === true);
  if (!pending.length) return;

  const g2bulkDoc = await getDoc(env, 'settings/g2bulk');
  const jcplaysDoc = await getDoc(env, 'settings/jcplays');
  const fazercardDoc = await getDoc(env, 'settings/fazercard');
  const g2bulk = (g2bulkDoc && g2bulkDoc.fields) || {};
  const jcplays = (jcplaysDoc && jcplaysDoc.fields) || {};
  const fazercard = (fazercardDoc && fazercardDoc.fields) || {};

  for (const { id } of pending) {
    try {
      // Re-fetch fresh (not the possibly-slightly-stale query result) and
      // atomically claim it before doing anything that costs real money.
      // If this loses the race - another overlapping sweep, or the admin
      // clicked Start Processing at the same instant - the precondition
      // fails, commitWrites comes back with an error, and we just skip it
      // this tick instead of risking a double G2Bulk/JCPlays placement.
      const doc = await getDoc(env, `orders/${id}`);
      if (!doc || doc.fields.status !== 'Pending') continue;
      const claimed = await claimOrderForFulfillment(env, id, doc.updateTime);
      if (!claimed) continue;

      // walletFlow 2: prove owner + price + balance BEFORE any provider is
      // called; the wallet is only debited afterwards (chargeWalletAfterProvider).
      let charge = null;
      if (doc.fields.walletFlow === WALLET_FLOW) {
        const gate = await gateWalletOrder(env, doc.fields);
        if (!gate.ok) {
          if (gate.retry) await recordFulfillFailure(env, id, Number(doc.fields.walletFulfillAttempts) || 0, gate.reason);
          else await finalizeWalletOrderFailure(env, id, 'Not charged: ' + gate.reason);
          continue;
        }
        charge = gate;
        // Safety net: remember that this order MUST be charged. If the Worker dies after the provider
        // accepted the order but before the debit below, reconcileWalletDebits() charges it on a later tick.
        await patchDoc(env, `orders/${id}`, { walletChargeDue: true }, ['walletChargeDue']);
      }

      if (doc.fields.g2bulkGameCode && doc.fields.g2bulkCatalogueName) {
        await fulfillG2BulkOrder(env, id, doc.fields, g2bulk);
      } else if (doc.fields.jcplaysProductId) {
        await fulfillJCPlaysOrder(env, id, doc.fields, jcplays);
      } else if (doc.fields.g2bulkVoucherProductId) {
        await fulfillG2BulkVoucherOrder(env, id, doc.fields, g2bulk);
      } else if (doc.fields.fazercardCategoryId && doc.fields.fazercardOfferId) {
        await fulfillFazerCardOrder(env, id, doc.fields, fazercard);
      } else {
        // No provider linked - nothing can auto-deliver this, so it just
        // moves into the normal Processing queue for an admin to finish
        // by hand (still no Telegram message, still no "Confirm" needed).
        await patchDoc(env, `orders/${id}`, { status: 'Processing' }, ['status']);
      }
      if (charge) await chargeWalletAfterProvider(env, id, charge.chargeAmount, charge.cost);
    } catch (err) {
      console.error('fulfillWalletOrders error for', id, err);
    }
  }
}

// ---------------------------------------------------------------------------
// Wallet payments: PROVIDER FIRST, WALLET SECOND  (order.walletFlow === 2)
// ---------------------------------------------------------------------------
// The browser no longer touches wallet money at all. It only writes a Pending
// order (rules require a real Google sign-in and stamp authUid = the caller's
// uid). This Worker then, per order:
//   1. proves who owns the wallet   (Firebase Auth email == users/{id}.googleEmail)
//   2. re-derives the price from the games catalogue (never trusts the browser)
//   3. checks the wallet can cover it
//   4. places the order with the provider (G2Bulk / JCPlays / ... or manual queue)
//   5. ONLY THEN debits the wallet (atomic increment) and stamps walletDebited.
// Failure before step 5 costs the customer nothing; a later provider failure
// refunds exactly what was debited (see finalizeWalletOrderFailure).
// Orders WITHOUT walletFlow 2 are legacy ones already debited by the old
// browser code, and keep their old behaviour (no gate, refund on failure).
const WALLET_FLOW = 2;
let _gamesCache = null; // { at, docs } - short-lived, so a sweep of many orders reads the catalogue once

async function getGamesCached(env) {
  if (_gamesCache && Date.now() - _gamesCache.at < 30000) return _gamesCache.docs;
  const docs = await queryAllDocs(env, 'games');
  _gamesCache = { at: Date.now(), docs };
  return docs;
}

const wStr = v => (v === null || v === undefined) ? '' : String(v);
const wLower = v => wStr(v).trim().toLowerCase();

// True when the customer's wallet has really been charged for this order (or,
// for legacy orders, was charged up-front by the old browser code).
function walletWasCharged(fields) {
  return fields.walletFlow !== WALLET_FLOW || fields.walletDebited === true;
}

// Firestore write that adds `amount` (may be negative) to users/{id}.walletBalance
// atomically on the server - no read-modify-write race.
function walletIncrementWrite(env, userPath, amount) {
  // A real (harmless) field sits in the mask on purpose: a transform-only write
  // with an EMPTY mask is easy to get wrong and could overwrite the whole doc.
  return {
    update: { name: docName(env, userPath), fields: { walletUpdatedAt: encodeValue(new Date()) } },
    updateMask: { fieldPaths: ['walletUpdatedAt'] },
    updateTransforms: [{ fieldPath: 'walletBalance', increment: { integerValue: String(Math.round(amount)) } }]
  };
}

// Who owns this wallet order? Not the browser's word for it: the order carries
// authUid (rules force it to equal the real signed-in uid), and that account's
// Firebase Auth e-mail must match the e-mail stored on the wallet's users doc.
async function verifyWalletOwner(env, f) {
  if (!f.authUid || !f.userId) return { ok: false, reason: 'Missing account identity.' };
  const uid = String(f.authUid);
  const user = await getDoc(env, `users/${f.userId}`);
  if (!user) return { ok: false, reason: 'Wallet account not found.' };
  if (user.fields.banned) return { ok: false, reason: 'Your account has been restricted.' };

  // Telegram login: Firebase custom-token uid is `tg_<telegramId>` and the
  // store links it in telegramAccounts/{uid}.guestId (same uid as doc id).
  // There is no e-mail on these accounts, so the link doc IS the proof.
  if (uid.startsWith('tg_')) {
    const tgLink = await getDoc(env, `telegramAccounts/${uid}`);
    if (tgLink && tgLink.fields.guestId === f.userId) return { ok: true, user };
    // Fallback: users doc already claimed by this Telegram uid.
    if (wStr(user.fields.authUid) === uid) return { ok: true, user };
    return { ok: false, reason: 'This Telegram sign-in is not linked to that wallet.' };
  }

  // Google / Facebook login: link doc must point at this wallet AND the real
  // Firebase Auth e-mail must match the e-mail stored on the wallet's user doc.
  const [gLink, fbLink] = await Promise.all([
    getDoc(env, `googleAccounts/${uid}`),
    getDoc(env, `facebookAccounts/${uid}`)
  ]);
  // Proof of link = the googleAccounts/facebookAccounts doc points at this
  // wallet, OR the wallet doc itself was claimed by this exact uid (authUid is
  // write-once and rules-protected). The e-mail check below still applies to
  // both, so the fallback never weakens ownership - it only covers accounts
  // whose link doc was never written (e.g. the client write was denied).
  const claimedByUid = wStr(user.fields.authUid) === uid;
  let viaGoogle = !!(gLink && gLink.fields.guestId === f.userId);
  let viaFacebook = !viaGoogle && !!(fbLink && fbLink.fields.guestId === f.userId);
  if (!viaGoogle && !viaFacebook && claimedByUid) {
    if (wLower(user.fields.googleEmail)) viaGoogle = true;
    else if (wLower(user.fields.facebookEmail)) viaFacebook = true;
  }
  if (!viaGoogle && !viaFacebook) return { ok: false, reason: 'This sign-in is not linked to that wallet.' };

  let authEmail = '';
  try {
    const res = await identityToolkit(env, 'accounts:lookup', { localId: [uid] });
    const u = (res.users || [])[0];
    if (u && u.emailVerified !== false) authEmail = wLower(u.email);
  } catch (e) {
    return { ok: false, retry: true, reason: 'Could not verify sign-in: ' + e.message };
  }
  const storedEmail = wLower(viaGoogle ? user.fields.googleEmail : user.fields.facebookEmail);
  if (!authEmail || storedEmail !== authEmail) {
    return { ok: false, reason: 'This wallet is not verified for your sign-in. Sign out and back in, then try again.' };
  }
  return { ok: true, user };
}

// The authoritative price. Finds the catalogue package this order is for and
// insists the order's price equals the catalogue's own list price.
async function verifyWalletPrice(env, f) {
  const price = Number(f.price);
  if (!Number.isInteger(price) || price < 0) return { ok: false, reason: 'Invalid price.' };
  const changed = { ok: false, reason: 'The price of this item has changed. Please reopen it and try again.' };

  const games = await getGamesCached(env);
  let best = null;
  for (const g of games) {
    const gf = g.fields;
    if (wStr(gf.name) !== wStr(f.game) || wStr(gf.serverName) !== wStr(f.serverRegion)) continue;
    // Any provider code the order names must be this game's own.
    if (f.g2bulkGameCode && wStr(f.g2bulkGameCode) !== wStr(gf.g2bulkCode)) continue;
    if (f.jcplaysProduct && wStr(f.jcplaysProduct) !== wStr(gf.jcplaysProduct)) continue;
    if (f.fazercardCategoryId && wStr(f.fazercardCategoryId) !== wStr(gf.fazercardCategoryId)) continue;
    if (f.fazercardGiftCategoryId && wStr(f.fazercardGiftCategoryId) !== wStr(gf.fazercardGiftCategoryId)) continue;
    for (const p of (Array.isArray(gf.packages) ? gf.packages : [])) {
      if (!p || typeof p !== 'object' || wStr(p.amount) !== wStr(f.itemAmount)) continue;
      if (f.g2bulkCatalogueName && wStr(f.g2bulkCatalogueName) !== wStr(p.g2bulk)) continue;
      if (f.jcplaysProductId && wStr(f.jcplaysProductId) !== wStr(p.jcplays)) continue;
      if (f.g2bulkVoucherProductId && wStr(f.g2bulkVoucherProductId) !== wStr(p.voucherId)) continue;
      if (f.fazercardOfferId && wStr(f.fazercardOfferId) !== wStr(p.fazercardOfferId)) continue;
      if (f.fazercardGiftCardId && wStr(f.fazercardGiftCardId) !== wStr(p.fazercardGiftCardId)) continue;
      const pp = Number(p.price) || 0;
      if (!best || pp < best.price) best = { price: pp, cost: Number(p.cost) || 0 };
    }
  }
  if (!best) return { ok: false, reason: 'This item is no longer available.' };

  // Voucher orders buy `quantity` codes in one call: total = unit price x quantity.
  const qty = f.g2bulkVoucherProductId ? Math.max(1, Math.min(MAX_VOUCHER_QTY, parseInt(f.quantity, 10) || 1)) : 1;
  const expected = best.price * qty;
  if (price !== expected) return changed;
  return { ok: true, price, cost: best.cost * qty };
}

// Everything that must be true BEFORE any provider is called.
async function gateWalletOrder(env, f) {
  const owner = await verifyWalletOwner(env, f);
  if (!owner.ok) return owner;
  const priced = await verifyWalletPrice(env, f);
  if (!priced.ok) return priced;
  const balance = Number(owner.user.fields.walletBalance) || 0;
  if (balance < priced.price) return { ok: false, reason: 'Insufficient wallet balance.' };
  return { ok: true, chargeAmount: priced.price, cost: priced.cost };
}

// Step 5: the provider has accepted the order (status is now Processing /
// Success) - take the money. Idempotent through walletDebited + the order-doc
// updateTime precondition, and atomic on the balance.
async function chargeWalletAfterProvider(env, orderId, chargeAmount, cost, quiet = false) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const doc = await getDoc(env, `orders/${orderId}`);
    if (!doc) return;
    const f = doc.fields;
    if (f.walletDebited === true) return;
    if (f.status === 'Pending') return;                       // provider call didn't go through - retried next tick, nothing to charge
    if (f.status === 'Fail' && !f.walletRefunded) return;     // failed cleanly - customer was never charged
    const result = await commitWrites(env, [
      {
        update: {
          name: docName(env, `orders/${orderId}`),
          fields: {
            walletDebited: encodeValue(true),
            walletDebitedAmount: encodeValue(chargeAmount),
            walletDebitedAt: encodeValue(new Date()),
            walletChargeDue: encodeValue(false),
            cost: encodeValue(cost)
          }
        },
        updateMask: { fieldPaths: ['walletDebited', 'walletDebitedAmount', 'walletDebitedAt', 'walletChargeDue', 'cost'] },
        currentDocument: { updateTime: doc.updateTime }
      },
      walletIncrementWrite(env, `users/${f.userId}`, -chargeAmount)
    ]);
    if (!result.error) return;
  }
  // The provider accepted the order but we could not take the money.
  console.error('chargeWalletAfterProvider: debit failed for', orderId);
  if (quiet) return;   // reconcile retries run every tick - the admin was already alerted the first time
  try {
    await patchDoc(env, `orders/${orderId}`, { walletDebitFailed: true }, ['walletDebitFailed']);
    await tgApi(env, 'sendMessage', {
      chat_id: env.TELEGRAM_CHAT_ID, parse_mode: 'HTML',
      text: `🚨 <b>Wallet debit FAILED</b>\n\nOrder <code>${orderId}</code> was accepted by the provider but the customer's wallet could not be charged (${Number(chargeAmount).toLocaleString()} MMK). Charge it by hand in the admin panel.`
    });
  } catch (e) { console.error('debit-failed alert error', e); }
}

// Every tick: finds wallet orders that were marked walletChargeDue but never debited (Worker crashed or
// timed out between the provider accepting the order and the debit) and charges the exact order price now.
// Pending = still being placed (leave it); Fail = never charged (just clear the flag).
async function reconcileWalletDebits(env) {
  let rows = [];
  try {
    rows = await runQueryPaginated(env, (limit) => ({
      from: [{ collectionId: 'orders' }],
      where: { fieldFilter: { field: { fieldPath: 'walletChargeDue' }, op: 'EQUAL', value: { booleanValue: true } } },
      orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
      limit
    }), 'orders', 'reconcileWalletDebits', 100);
  } catch (e) { console.error('reconcileWalletDebits query failed', e); return; }
  for (const { id, fields: f } of rows) {
    try {
      if (f.walletDebited === true) { await patchDoc(env, `orders/${id}`, { walletChargeDue: false }, ['walletChargeDue']); continue; }
      if (f.status === 'Pending') continue;
      if (f.status === 'Fail') { await patchDoc(env, `orders/${id}`, { walletChargeDue: false }, ['walletChargeDue']); continue; }
      const amount = Math.round(Number(f.price));
      if (!f.userId || !(amount > 0)) continue;
      await chargeWalletAfterProvider(env, id, amount, Number(f.cost) || 0, f.walletDebitFailed === true);
    } catch (e) { console.error('reconcileWalletDebits error for', id, e); }
  }
}

// One-time helper (POST /admin-backfill-google-email?offset=0). Wallet orders
// prove ownership through users/{id}.googleEmail; older accounts may not have
// it yet. This copies the REAL Firebase Auth e-mail of each linked Google
// account onto its users doc where missing. Processes a small batch per call
// (Workers cap subrequests) - call again with the returned nextOffset until
// done:true. Auth: X-Sync-Secret header (TELEGRAM_WEBHOOK_SECRET) or a full-admin Bearer token.
async function handleBackfillGoogleEmail(request, env, url) {
  const who = await authorizeAdminSync(request, env, url);
  if (!who) return corsJsonAdmin({ ok: false, error: 'Unauthorized' }, 401);
  const offset = Math.max(0, parseInt(url.searchParams.get('offset') || '0', 10) || 0);
  const BATCH = 12;
  const links = (await queryAllDocs(env, 'googleAccounts', 5000)).sort((a, b) => (a.id < b.id ? -1 : 1));
  const slice = links.slice(offset, offset + BATCH);
  const out = { ok: true, total: links.length, updated: 0, skipped: 0, errors: [] };
  for (const l of slice) {
    try {
      const gid = l.fields.guestId;
      const u = gid ? await getDoc(env, `users/${gid}`) : null;
      if (!u || u.fields.googleEmail) { out.skipped++; continue; }
      const res = await identityToolkit(env, 'accounts:lookup', { localId: [l.id] });
      const email = wLower(((res.users || [])[0] || {}).email);
      if (!email) { out.skipped++; continue; }
      await patchDoc(env, `users/${gid}`, { googleEmail: email }, ['googleEmail']);
      out.updated++;
    } catch (e) { out.errors.push(`${l.id}: ${e.message}`); }
  }
  out.nextOffset = offset + BATCH;
  out.done = out.nextOffset >= links.length;
  return corsJsonAdmin(out);
}

// Atomic "I've got this one" marker so two overlapping cron sweeps (or a
// sweep racing an admin panel action) can't both start placing the same
// order. Fails harmlessly if the doc changed since we last read it.
async function claimOrderForFulfillment(env, orderId, updateTime) {
  const result = await commitWrites(env, [{
    update: {
      name: docName(env, `orders/${orderId}`),
      fields: { walletFulfillClaimedAt: { timestampValue: new Date().toISOString() } }
    },
    updateMask: { fieldPaths: ['walletFulfillClaimedAt'] },
    currentDocument: { updateTime }
  }]);
  return !result.error;
}

async function recordFulfillFailure(env, orderId, priorAttempts, message) {
  const attempts = priorAttempts + 1;
  console.error(`Wallet order ${orderId} fulfill attempt ${attempts} failed: ${message}`);
  if (attempts >= MAX_WALLET_FULFILL_ATTEMPTS) {
    await finalizeWalletOrderFailure(env, orderId, `Auto-fulfill failed after ${attempts} attempts: ${message}`, { walletFulfillAttempts: attempts });
  } else {
    // Left as "Pending" on purpose - queryPending will pick it up again on
    // the next cron tick and retry, which self-heals from transient errors
    // (network blip, G2Bulk momentarily out of balance, etc). The claim
    // marker from claimOrderForFulfillment does NOT block this retry since
    // the next tick only checks status === 'Pending', not the claim field.
    await patchDoc(env, `orders/${orderId}`, { walletFulfillAttempts: attempts }, ['walletFulfillAttempts']);
  }
}

// Marks a wallet-paid order Fail AND refunds the customer's wallet in the
// same atomic commit, so there's no window where the order reads "Fail"
// with the money still gone. Idempotent via the walletRefunded flag - safe
// to call more than once for the same order (e.g. a G2Bulk webhook retry
// racing this Worker's own cron sweep).
async function finalizeWalletOrderFailure(env, orderId, statusNote, extraOrderFields = {}) {
  const doc = await getDoc(env, `orders/${orderId}`);
  if (!doc) return;
  if (doc.fields.status === 'Fail' && doc.fields.walletRefunded) return; // already handled

  const orderFieldsEncoded = {
    status: { stringValue: 'Fail' },
    statusNote: { stringValue: statusNote }
  };
  for (const k in extraOrderFields) orderFieldsEncoded[k] = encodeValue(extraOrderFields[k]);
  const orderFieldPaths = ['status', 'statusNote', ...Object.keys(extraOrderFields)];

  // walletFlow 2 orders are charged only AFTER the provider accepts them, so
  // a failure before that point has nothing to refund (walletDebited unset).
  const eligibleForRefund = doc.fields.paidFromWallet === true
    && walletWasCharged(doc.fields)
    && !doc.fields.walletRefunded
    && doc.fields.userId
    && typeof doc.fields.price === 'number';

  if (!eligibleForRefund) {
    await patchDoc(env, `orders/${orderId}`, { status: 'Fail', statusNote, ...extraOrderFields }, orderFieldPaths);
    await notifyOrderStatus(env, orderId, 'Fail');
    return;
  }

  const userPath = `users/${doc.fields.userId}`;
  const refundAmount = Math.round(Number(doc.fields.walletDebitedAmount || doc.fields.price));

  orderFieldsEncoded.walletRefunded = { booleanValue: true };
  orderFieldsEncoded.refundedAt = { timestampValue: new Date().toISOString() };
  orderFieldPaths.push('walletRefunded', 'refundedAt');

  const result = await commitWrites(env, [
    {
      update: { name: docName(env, `orders/${orderId}`), fields: orderFieldsEncoded },
      updateMask: { fieldPaths: orderFieldPaths },
      currentDocument: { updateTime: doc.updateTime } // race guard against a double refund
    },
    walletIncrementWrite(env, userPath, refundAmount)
  ]);
  if (result.error) {
    // Lost the race (doc changed since we read it, e.g. already refunded by
    // another path a moment ago) - don't retry blindly, next call will
    // re-read fresh state and see walletRefunded is already true.
    console.error(`finalizeWalletOrderFailure: commit failed for ${orderId}`, JSON.stringify(result.error));
    return;
  }
  await notifyOrderStatus(env, orderId, 'Fail');
  await notifyAdminAutoRefund(env, orderId, doc.fields, statusNote, refundAmount);
}

// Pings the admin Telegram chat whenever finalizeWalletOrderFailure actually
// refunds a customer's wallet - this branch only runs after auto-fulfill has
// already exhausted MAX_WALLET_FULFILL_ATTEMPTS retries, so it's worth an
// admin's attention (provider outage, bad game config, low provider
// balance, etc). Fire-and-forget: a Telegram hiccup here must never block
// or unwind the refund itself, which is why this is called after the
// commit above, not folded into it.
async function notifyAdminAutoRefund(env, orderId, fields, statusNote, amount) {
  try {
    const text = `⚠️ <b>Auto-Refund Issued</b>\n\n`
      + `Order: <code>${orderId}</code>\n`
      + `Game: ${fields.game || '-'}\n`
      + `User: <code>${fields.userId || '-'}</code>\n`
      + `Amount: ${Number(amount || 0).toLocaleString()} MMK\n`
      + `Reason: ${statusNote}\n\n`
      + `Wallet was auto-refunded after repeated fulfillment failures. Check the provider (balance/API status) before more orders hit the same wall.`;
    await tgApi(env, 'sendMessage', { chat_id: env.TELEGRAM_CHAT_ID, parse_mode: 'HTML', text });
  } catch (err) {
    console.error('notifyAdminAutoRefund error', orderId, err);
  }
}

// Matches G2Bulk's documented POST /v1/games/:code/order (api.g2bulk.com/docs
// #order): catalogue_name/player_id/server_id/charname/remark/callback_url in
// the body, X-API-Key + optional X-Idempotency-Key (must be a 36-char UUID -
// crypto.randomUUID() already produces that format) as headers. Response is
// flat - { success, order_id, status: "PENDING", ... } at the top level, no
// "order" wrapper (confirmed against G2Bulk's docs and mirrored from
// admin.html's placeG2BulkOrder); the order later resolves via the
// callback_url webhook (handled by the separate g2bulk-function Worker), not
// by anything in this function.
//
// Mirrors admin.html's placeG2BulkOrder() exactly, including reading
// webhookUrl/webhookSecret from the same settings/g2bulk Firestore doc the
// G2Bulk Settings modal writes to — that stays the single place to configure
// it, whether an order is placed from here or from the admin panel.
// G2Bulk's game top-up ("SMM") endpoint only ever accepts quantity=1 per
// call - there's no quantity param on it (unlike the voucher/giftcard
// purchase endpoint used by fulfillG2BulkVoucherOrder below, which does
// take one). So a "60 UC x 5" order is placed here as 5 separate G2Bulk
// order calls, one per unit, each tagged with its own remark suffix
// (`#1`, `#2`, ...) so the webhook (g2bulk-function Worker) can tell which
// unit a given callback belongs to and update the right slot below.
//
// Progress is saved to orders/{id}.g2bulkSubOrders after every unit so a
// retry (recordFulfillFailure leaves the order "Pending" on transient
// failures) resumes from the first not-yet-placed unit instead of
// re-placing - and re-paying G2Bulk's balance for - units that already
// went through.
// The g2bulk-function webhook Worker can record a unit's COMPLETED/FAILED result
// into orders/{id}.g2bulkSubOrders while THIS function is still placing later units
// (instant top-ups finish in seconds). Writing our older local subOrders array back
// as-is would erase those results and strand the order in "Processing". So the
// read-merge-write is one optimistic-concurrency commit: the write carries the
// updateTime of the doc we merged from as a precondition, and if the webhook wrote
// in between, the commit is rejected and we re-read + re-merge (same pattern as
// recordSubOrderResult in the webhook Worker). buildFields({done, terminal}) returns
// the plain fields to write; `terminal` = the webhook already finished the order.
async function writeSubOrdersMerged(env, orderId, subOrders, buildFields) {
  for (let attempt = 0; attempt < 6; attempt++) {
    const fresh = await getDoc(env, `orders/${orderId}`);
    if (!fresh) break;
    const freshSubs = Array.isArray(fresh.fields.g2bulkSubOrders) ? fresh.fields.g2bulkSubOrders : [];
    for (const sub of subOrders) {
      const f = freshSubs.find(x => x && x.index === sub.index);
      if (f && f.result && !sub.result) sub.result = f.result;
    }
    const info = {
      done: subOrders.filter(s => s.result).length,
      terminal: fresh.fields.status === 'Success' || fresh.fields.status === 'Fail'
    };
    const fields = buildFields(info);
    const encoded = {};
    for (const k in fields) encoded[k] = encodeValue(fields[k]);
    const result = await commitWrites(env, [{
      update: { name: docName(env, `orders/${orderId}`), fields: encoded },
      updateMask: { fieldPaths: Object.keys(encoded) },
      currentDocument: { updateTime: fresh.updateTime }
    }]);
    if (!result.error) return info;
    await new Promise(r => setTimeout(r, 150 + Math.random() * 250)); // webhook won the race - re-read and re-merge
  }
  // Contention/permission trouble that never cleared: fall back to a plain write
  // (the pre-merge behaviour) rather than dropping the progress we just made.
  console.error('writeSubOrdersMerged: gave up after retries, writing without precondition for', orderId);
  const info = { done: subOrders.filter(s => s.result).length, terminal: false };
  const fields = buildFields(info);
  await patchDoc(env, `orders/${orderId}`, fields, Object.keys(fields));
  return info;
}

async function fulfillG2BulkOrder(env, orderId, fields, g2bulk) {
  if (!g2bulk.apiKey) return; // not configured yet - leave Pending for the admin to handle
  if (g2bulk.enabled === false) return; // explicitly turned off in the G2Bulk Settings modal

  const attempts = Number(fields.walletFulfillAttempts || 0);
  const quantity = Math.max(1, Number(fields.quantity) || 1);

  const existing = Array.isArray(fields.g2bulkSubOrders) ? fields.g2bulkSubOrders : [];
  const subOrders = [];
  for (let i = 1; i <= quantity; i++) {
    subOrders.push(existing.find(s => s && s.index === i) || { index: i });
  }

  const callbackUrl = g2bulk.webhookUrl
    ? `${g2bulk.webhookUrl}?secret=${encodeURIComponent(g2bulk.webhookSecret || '')}`
    : undefined;

  // Create AND save every unit's idempotency key BEFORE any G2Bulk call. If the Worker is
  // killed right after G2Bulk accepts a unit, the retry then reuses the same key instead of
  // generating a new one and buying that unit twice.
  let keysAdded = false;
  for (const sub of subOrders) {
    if (sub.g2bulkOrderId) continue;
    if (!sub.idempotencyKey || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(sub.idempotencyKey)) {
      sub.idempotencyKey = crypto.randomUUID();
      keysAdded = true;
    }
  }
  if (keysAdded) await writeSubOrdersMerged(env, orderId, subOrders, () => ({ quantity, g2bulkSubOrders: subOrders }));

  for (const sub of subOrders) {
    if (sub.g2bulkOrderId) continue; // already placed successfully on a prior attempt

    // Deterministic per-unit key (kept once generated) so re-sending THIS
    // same unit on retry reuses the same idempotency key, rather than
    // risking G2Bulk accepting - and charging for - it twice.
    // Docs: must be a 36-character UUID. Replace any old "<orderId>-u<n>-<hex>" key
    // left by an earlier attempt (that format isn't a UUID, so nothing was placed with it).
    // (idempotencyKey was generated and saved above, before the loop)

    let data;
    try {
      const res = await fetch(`https://api.g2bulk.com/v1/games/${encodeURIComponent(fields.g2bulkGameCode)}/order`, {
        method: 'POST',
        headers: { 'X-API-Key': g2bulk.apiKey, 'X-Idempotency-Key': sub.idempotencyKey, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          catalogue_name: fields.g2bulkCatalogueName,
          player_id: fields.playerId,
          server_id: fields.serverId || undefined,
          charname: fields.charName || fields.g2bulkCharname || undefined,
          remark: `PCA Game Store order ${orderId}#${sub.index}`,
          callback_url: callbackUrl
        })
      });
      data = await res.json();
      if (!res.ok || data.success === false) throw new Error(data.message || data.error || (data.detail && data.detail.message) || (typeof data.detail === 'string' ? data.detail : '') || data.msg || ('G2Bulk rejected the order (HTTP ' + res.status + ')'));
    } catch (err) {
      // Persist whatever units DID get placed before this failure, so the
      // next retry (below) resumes instead of restarting from unit 1.
      await writeSubOrdersMerged(env, orderId, subOrders, () => ({ quantity, g2bulkSubOrders: subOrders }));
      await recordFulfillFailure(env, orderId, attempts, `G2Bulk (unit ${sub.index}/${quantity}): ${err.message}`);
      return;
    }

    sub.g2bulkOrderId = data.order_id ?? data.order?.order_id ?? null;
    sub.status = data.status || data.order?.status || 'PENDING';
  }

  await writeSubOrdersMerged(env, orderId, subOrders, ({ done, terminal }) => {
    const finalPatch = {
      quantity,
      g2bulkSubOrders: subOrders,
      g2bulkSubOrdersTotal: quantity,
      g2bulkSubOrdersDone: done,
      // Kept for back-compat with anything still reading a single id/status
      // (e.g. an order detail view) - reflects unit #1.
      g2bulkOrderId: subOrders[0].g2bulkOrderId || null
    };
    // If the webhook already finished the whole order (every unit reported back before
    // we got here), don't drag it back to "Processing" or overwrite its g2bulkStatus.
    if (!terminal) { finalPatch.status = 'Processing'; finalPatch.g2bulkStatus = subOrders[0].status || 'PENDING'; }
    return finalPatch;
  });
}

// Voucher/giftcard purchase — different endpoint from the game top-up flow
// above (POST /v1/products/:id/purchase). Comes back COMPLETED immediately
// with codes attached, or PENDING with a poll_url to check a few seconds
// later. There's no webhook for this side of the G2Bulk API, so
// pollPendingVoucherOrders() (run every cron tick, see runCronSweep) is
// what eventually flips a still-PENDING voucher order to Success/Fail
// server-side, even with the admin panel closed. Mirrors admin.html's
// placeG2BulkVoucherOrder() exactly.
async function fulfillG2BulkVoucherOrder(env, orderId, fields, g2bulk) {
  if (!g2bulk.apiKey) return; // not configured yet - leave Pending for the admin to handle
  if (g2bulk.enabled === false) return; // explicitly turned off in the G2Bulk Settings modal

  const attempts = Number(fields.walletFulfillAttempts || 0);
  // The package this order was placed for has no G2Bulk product linked
  // (admin.html's pkg-voucher-id was never set) - hitting the purchase
  // endpoint with a "null" product id just burns all 5 retries on a
  // guaranteed-forever G2Bulk error. Fail fast with a clear reason instead
  // so the admin sees exactly what to fix (link the package) rather than a
  // generic G2Bulk error message after several minutes of retries.
  if (!fields.g2bulkVoucherProductId) {
    await finalizeWalletOrderFailure(env, orderId, 'This package has no G2Bulk product linked (pkg-voucher-id missing) - link it in the game editor.', { walletFulfillAttempts: attempts + 1 });
    return;
  }
  const voucherQty = Math.max(1, Number(fields.quantity) || Number(fields.voucherQuantity) || 1);
  // G2Bulk docs: re-read the single product right before buying. If we can see
  // there are fewer codes in stock than ordered, fail now (nothing was bought,
  // wallet orders are not charged) instead of sending a purchase that will bounce.
  // Any problem reading the product is ignored - the purchase call is the real check.
  try {
    const pres = await fetch(`https://api.g2bulk.com/v1/products/${encodeURIComponent(fields.g2bulkVoucherProductId)}`, { headers: { 'X-API-Key': g2bulk.apiKey } });
    if (pres.ok) {
      const pj = await pres.json().catch(() => ({}));
      const prod = (pj && pj.product) || pj || {};
      const stock = Number(prod.stock);
      if (prod.stock !== undefined && prod.stock !== null && Number.isFinite(stock) && stock < voucherQty) {
        await finalizeWalletOrderFailure(env, orderId, `Out of stock at G2Bulk (ordered ${voucherQty}, available ${stock}).`, { walletFulfillAttempts: attempts + 1 });
        return;
      }
    }
  } catch (e) { console.error('voucher stock pre-check skipped', e && e.message); }

  const idempotencyKey = fields.g2bulkVoucherIdempotencyKey || crypto.randomUUID();
  if (!fields.g2bulkVoucherIdempotencyKey) {
    await patchDoc(env, `orders/${orderId}`, { g2bulkVoucherIdempotencyKey: idempotencyKey }, ['g2bulkVoucherIdempotencyKey']);
  }

  let data;
  try {
    const res = await fetch(`https://api.g2bulk.com/v1/products/${encodeURIComponent(fields.g2bulkVoucherProductId)}/purchase`, {
      method: 'POST',
      headers: { 'X-API-Key': g2bulk.apiKey, 'X-Idempotency-Key': idempotencyKey, 'Content-Type': 'application/json' },
      body: JSON.stringify({ quantity: voucherQty })
    });
    data = await res.json();
    if (!res.ok || data.success === false) throw new Error(data.message || data.error || (data.detail && data.detail.message) || (typeof data.detail === 'string' ? data.detail : '') || data.msg || ('G2Bulk rejected the order (HTTP ' + res.status + ')'));
  } catch (err) {
    await recordFulfillFailure(env, orderId, attempts, 'G2Bulk voucher: ' + err.message);
    return;
  }

  if (data.status === 'COMPLETED') {
    await patchDoc(env, `orders/${orderId}`, {
      status: 'Success',
      confirmedAt: new Date(),
      g2bulkVoucherOrderId: data.order_id || null,
      deliveredCodes: data.delivery_items || []
    }, ['status', 'confirmedAt', 'g2bulkVoucherOrderId', 'deliveredCodes']);
    await notifyOrderStatus(env, orderId, 'Success');
    return;
  }

  // PENDING - save the poll_url; pollPendingVoucherOrders() below checks it
  // every cron tick (~every 1 min) until G2Bulk finishes preparing the codes.
  await patchDoc(env, `orders/${orderId}`, {
    status: 'Processing',
    g2bulkVoucherOrderId: data.order_id || null,
    g2bulkVoucherPollUrl: data.poll_url || null,
    g2bulkStatus: 'PENDING'
  }, ['status', 'g2bulkVoucherOrderId', 'g2bulkVoucherPollUrl', 'g2bulkStatus']);
}

// Queries orders that are sitting in "Processing" with a saved voucher
// poll_url - i.e. G2Bulk accepted the purchase but hadn't prepared the
// codes yet. Needed because, unlike the game top-up endpoint, G2Bulk's
// voucher/giftcard purchase has no webhook - polling is the only way to
// ever find out it finished, and this Worker is the only thing that polls
// when the admin panel isn't open.
// Pages through every order stuck like this (see runQueryPaginated) instead
// of stopping at one page — with no filter and no orderBy on this, Firestore
// returns docs in a stable document-name order, so a single-page `limit`
// here would keep re-polling the exact same subset every tick forever and
// leave any order past that page permanently un-polled once the backlog
// grew past one page.
async function queryProcessingVoucherOrders(env) {
  return runQueryPaginated(env, (limit) => ({
    from: [{ collectionId: 'orders' }],
    where: {
      compositeFilter: {
        op: 'AND',
        filters: [
          { fieldFilter: { field: { fieldPath: 'status' }, op: 'EQUAL', value: { stringValue: 'Processing' } } },
          { unaryFilter: { field: { fieldPath: 'g2bulkVoucherPollUrl' }, op: 'IS_NOT_NULL' } }
        ]
      }
    },
    orderBy: [
      { field: { fieldPath: 'g2bulkVoucherPollUrl' }, direction: 'ASCENDING' },
      { field: { fieldPath: '__name__' }, direction: 'ASCENDING' }
    ],
    limit
  }), 'orders', 'queryProcessingVoucherOrders', 1000);
}

// Checks one PENDING voucher order's poll_url once. Per G2Bulk's /delivery
// endpoint docs the HTTP status code itself carries the meaning: 200 ready,
// 202 still processing, 410 failed+refunded. Mirrors admin.html's
// checkG2BulkVoucherDeliveryStatus().
async function checkVoucherDeliveryStatus(env, orderId, pollUrl, g2bulk) {
  try {
    const url = pollUrl.startsWith('http') ? pollUrl : `https://api.g2bulk.com${pollUrl}`;
    const res = await fetch(url, { headers: { 'X-API-Key': g2bulk.apiKey } });
    if (res.status === 200) {
      const data = await res.json().catch(() => ({}));
      await patchDoc(env, `orders/${orderId}`, {
        status: 'Success',
        confirmedAt: new Date(),
        deliveredCodes: data.delivery_items || []
      }, ['status', 'confirmedAt', 'deliveredCodes']);
      await notifyOrderStatus(env, orderId, 'Success');
    } else if (res.status === 410) {
      await finalizeWalletOrderFailure(env, orderId, 'G2Bulk voucher order failed/refunded.', { g2bulkStatus: 'REFUNDED' });
    } else if (res.status === 401) {
      return 'AUTH_FAILED';   // docs: never loop on 401 (repeated auth failures can permanently ban the IP)
    } else if (res.status === 429 || res.status >= 500) {
      return 'BACKOFF';       // docs: back off on 429/5xx
    }
    // 202/404/anything else -> still processing or not ready yet; leave as
    // Processing, the next cron tick (~1 min later) will check again.
  } catch (err) {
    console.error('checkVoucherDeliveryStatus error', orderId, err);
  }
}

// Called once per cron tick (see runCronSweep). This is the piece that was
// completely missing before: without it, any voucher order that came back
// PENDING from G2Bulk had nothing left to ever re-check it once the initial
// purchase call returned - it just sat in "Processing" forever unless an
// admin happened to have the admin panel open (which polls for ~20s after
// clicking Auto-Fulfill) or manually pressed "Check Status".
async function pollPendingVoucherOrders(env) {
  const g2bulkDoc = await getDoc(env, 'settings/g2bulk');
  const g2bulk = (g2bulkDoc && g2bulkDoc.fields) || {};
  if (!g2bulk.apiKey) return;
  // After a 401 the key is bad: pause polling for 10 min instead of failing
  // once per stuck order on every 1-minute cron tick.
  const guardDoc = await getDoc(env, 'settings/g2bulkPollGuard');
  if (guardDoc && Number(guardDoc.fields.until) > Date.now()) return;
  const stuck = await queryProcessingVoucherOrders(env);
  for (const d of stuck) {
    const r = await checkVoucherDeliveryStatus(env, d.id, d.fields.g2bulkVoucherPollUrl, g2bulk);
    if (r === 'AUTH_FAILED') {
      await patchDoc(env, 'settings/g2bulkPollGuard', { until: Date.now() + 10 * 60 * 1000 }, ['until']);
      console.error('G2Bulk delivery poll got 401 - pausing voucher polling for 10 minutes');
      break;
    }
    if (r === 'BACKOFF') break;
  }
}

async function fulfillJCPlaysOrder(env, orderId, fields, jcplays) {
  if (!jcplays.uid || !jcplays.email || !jcplays.mKey) return; // not configured yet - leave Pending

  let data;
  try {
    const res = await fetch(jcplaysProxyUrl(env, jcplays), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        endpointPath: 'createorder',
        extraParams: {
          product: fields.jcplaysProduct,
          productid: fields.jcplaysProductId,
          userid: fields.playerId,
          ...(fields.serverId ? { zoneid: fields.serverId } : {})
        },
        uid: jcplays.uid,
        email: jcplays.email,
        mKey: jcplays.mKey,
        baseUrl: jcplays.baseUrl || 'https://jcplays.com/smilecoin/api'
      })
    });
    data = await res.json();
    if (data && data.error) throw new Error(data.error);
    if (data.status !== 200) throw new Error(data.message || `JCPlays rejected the order (status ${data.status})`);
  } catch (err) {
    // JCPlay's own API docs (createorder) confirm it takes no client-supplied
    // order reference/idempotency parameter — unlike every other provider in
    // this file (G2Bulk's X-Idempotency-Key, FazerCards' Idempotency-Key),
    // there is
    // no way to safely tell JCPlays "this is the same order, don't charge
    // twice." A network blip AFTER JCPlays already deducted the balance and
    // fulfilled would look identical, from here, to a blip before it did.
    // So this is deliberately NOT left "Pending" for recordFulfillFailure()'s
    // usual next-tick auto-retry - that could silently double-charge the
    // JCPlays balance and double-deliver the customer. Instead it's parked
    // in "Processing" with a note, and an admin is pinged to verify against
    // the JCPlays dashboard/transaction history before resolving it by hand.
    console.error(`JCPlays fulfill failed for ${orderId}: ${err.message}`);
    await patchDoc(env, `orders/${orderId}`, {
      status: 'Processing',
      statusNote: `JCPlays order call failed (${err.message}) — balance may already be charged. Verify manually in the JCPlays dashboard/transaction history, then resolve this order by hand.`
    }, ['status', 'statusNote']);
    await tgApi(env, 'sendMessage', {
      chat_id: env.TELEGRAM_CHAT_ID, parse_mode: 'HTML',
      text: `⚠️ <b>JCPlays order needs manual check</b>\nOrder <code>${esc(orderId)}</code> — createorder call failed: ${esc(err.message)}\n\nThis was <b>not</b> auto-retried (JCPlays has no dedup key, so a blind retry could double-charge). Check the JCPlays dashboard/transaction history, then resolve this order from the admin panel.`
    });
    return;
  }

  // JCPlays has no idempotency key - once this succeeds the balance is
  // already charged, so this write must not be skipped/retried blindly.
  //
  // Redeem-code extraction, confirmed against JCPlay's own API docs
  // (createorder response example): for SmileCode/voucher products the
  // codes come back as `codes` (an array — usually just one entry, but the
  // shape is an array) and/or `info` (the same code(s) as a comma-separated
  // string). Older revisions of this function guessed at `code`/`pin`/
  // `serial`/`voucher_code`/`card_code`, none of which are real JCPlay
  // fields — meaning every voucher-type JCPlays order (e.g. PUBG Mobile
  // "Starlight", SmileCode) would resolve to "Success" with an EMPTY code:
  // JCPlays had already charged the balance and generated the code, but the
  // customer's Success screen showed nothing and nobody knew to look. The
  // old guessed fields are kept below only as a defensive fallback in case
  // some product's response ever varies, and the full raw response is now
  // also saved (jcplaysRedeemRaw) so a code is never silently lost even if
  // this extraction is still wrong for some product.
  const codesArray = Array.isArray(data.codes) ? data.codes.filter(Boolean)
    : (data.data && Array.isArray(data.data.codes) ? data.data.codes.filter(Boolean) : []);
  const redeemCode = codesArray[0] || data.info || (data.data && data.data.info)
    || data.code || data.pin || data.serial || data.voucher_code || data.card_code
    || (data.data && (data.data.code || data.data.pin || data.data.serial)) || '';
  const deliveredCodes = codesArray.length ? codesArray : (redeemCode ? [redeemCode] : []);
  await patchDoc(env, `orders/${orderId}`, {
    status: 'Success',
    confirmedAt: new Date(),
    jcplaysOrderId: data.order_id || null,
    jcplaysRedeemCode: redeemCode,
    jcplaysRedeemRaw: JSON.stringify(data).slice(0, 2000),
    deliveredCodes
  }, ['status', 'confirmedAt', 'jcplaysOrderId', 'jcplaysRedeemCode', 'jcplaysRedeemRaw', 'deliveredCodes']);
  await notifyOrderStatus(env, orderId, 'Success');
}

// One-off audit for orders that resolved Success under the OLD (buggy)
// redeem-code extraction — see the note in fulfillJCPlaysOrder above. Pages
// through every JCPlays-linked order that's currently Success (see
// runQueryPaginated) and reports the ones with no jcplaysRedeemCode saved.
// Deliberately checked in JS after the fetch, not as a Firestore filter -
// Firestore can't combine "field is null/empty" with other filters the way
// this needs without a dedicated index, and this is a run-once cleanup tool,
// not a hot path, so the extra client-side filtering cost doesn't matter.
// the `fields` keys FazerCards expects at order time for it (almost always
// ['player_id'], sometimes also a zone/server key) - saved onto
// games/{id}.fazercardFields at import time by admin.html's
// importFazerCardCatalogue(). admin.html does this with a plain Object.values
// scan over its already-loaded GAMES cache; this Worker has no such cache,
// so it queries Firestore directly instead.
async function queryGameByFazerCardCategory(env, categoryId) {
  if (!categoryId) return null;
  try {
    const token = await getAccessToken(env);
    const body = {
      structuredQuery: {
        from: [{ collectionId: 'games' }],
        where: { fieldFilter: { field: { fieldPath: 'fazercardCategoryId' }, op: 'EQUAL', value: { stringValue: categoryId } } },
        limit: 1
      }
    };
    const res = await fetch(
      `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`,
      { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` }, body: JSON.stringify(body) }
    );
    const rows = await res.json();
    if (!res.ok || !Array.isArray(rows)) return null;
    const row = rows.find(r => r.document);
    return row ? decodeFields(row.document.fields || {}) : null;
  } catch (err) {
    console.error('queryGameByFazerCardCategory threw', err);
    return null;
  }
}

// Matches FazerCards' documented POST /topups/order (api.fzr.cards/api/v2):
// category_id/offer_id/fields in the body, X-API-Key + optional
// Idempotency-Key header. Response is { ok, order: { id, status } }.
// FazerCards top-ups resolve async with no order-creation webhook wired up
// yet, so this only places the order and leaves it "Processing" -
// pollPendingFazerCardOrders() below (run every cron tick, ~1 min) is what
// actually finishes it, the same role pollPendingVoucherOrders() plays for
// G2Bulk vouchers.
//
// Mirrors admin.html's placeFazerCardOrder(), with one deliberate
// difference: a missing serverId/playerId sends that field as '' instead of
// being silently dropped from the JSON body, so a FazerCards-side
// missing-field rejection is explicit instead of a field just vanishing.
async function fulfillFazerCardOrder(env, orderId, fields, fazercard) {
  if (!fazercard.apiKey) return; // not configured yet - leave Pending for the admin to handle
  if (fazercard.enabled === false) return; // explicitly turned off in the FazerCards Settings modal

  const attempts = Number(fields.walletFulfillAttempts || 0);
  const idempotencyKey = fields.fazercardIdempotencyKey || crypto.randomUUID();
  if (!fields.fazercardIdempotencyKey) {
    await patchDoc(env, `orders/${orderId}`, { fazercardIdempotencyKey: idempotencyKey }, ['fazercardIdempotencyKey']);
  }

  const linkedGame = await queryGameByFazerCardCategory(env, fields.fazercardCategoryId);
  const fieldKeys = (linkedGame && Array.isArray(linkedGame.fazercardFields) && linkedGame.fazercardFields.length)
    ? linkedGame.fazercardFields
    : ['player_id'];
  const orderFields = {};
  fieldKeys.forEach(key => {
    orderFields[key] = /server|zone/i.test(key) ? (fields.serverId || '') : (fields.playerId || '');
  });

  let data;
  try {
    const res = await fetch(`${FAZERCARD_BASE_URL}/topups/order`, {
      method: 'POST',
      headers: { 'X-API-Key': fazercard.apiKey, 'Idempotency-Key': idempotencyKey, 'Content-Type': 'application/json' },
      body: JSON.stringify({ category_id: fields.fazercardCategoryId, offer_id: fields.fazercardOfferId, fields: orderFields })
    });
    data = await res.json();
    if (!data.ok) throw new Error(data.error || `FazerCards request failed (HTTP ${res.status})`);
  } catch (err) {
    await recordFulfillFailure(env, orderId, attempts, 'FazerCards: ' + err.message);
    return;
  }

  await patchDoc(env, `orders/${orderId}`, {
    status: 'Processing',
    fazercardOrderId: (data.order && data.order.id) || null,
    fazercardStatus: (data.order && data.order.status) || 'processing'
  }, ['status', 'fazercardOrderId', 'fazercardStatus']);
}

// Queries orders sitting "Processing" with a saved fazercardOrderId - same
// role as queryProcessingVoucherOrders for G2Bulk vouchers: without this, a
// FazerCards order confirmed via Telegram (or auto-fulfilled from a wallet
// payment) would sit in "Processing" forever unless an admin happened to
// have the admin panel open (which only short-polls for ~20s right after
// placing) or manually pressed "Check FazerCards Status".
// Pages through every stuck order — same reasoning as
// queryProcessingVoucherOrders above (no orderBy + a single-page limit would
// keep re-polling the same subset forever once the backlog outgrew one page).
async function queryProcessingFazerCardOrders(env) {
  // Single equality filter + __name__ ordering only -> served by Firestore's automatic
  // indexes (no composite index needed). fazercardOrderId is filtered in JS instead.
  const rows = await runQueryPaginated(env, (limit) => ({
    from: [{ collectionId: 'orders' }],
    where: {
      fieldFilter: { field: { fieldPath: 'status' }, op: 'EQUAL', value: { stringValue: 'Processing' } }
    },
    orderBy: [
      { field: { fieldPath: '__name__' }, direction: 'ASCENDING' }
    ],
    limit
  }), 'orders', 'queryProcessingFazerCardOrders', 1000);
  return rows.filter((d) => d.fields && d.fields.fazercardOrderId);
}

// Checks one Processing order's FazerCards status once and applies it -
// same status vocabulary as admin.html's checkFazerCardOrderStatus
// (created/processing/completed/failed/cancelled/refunded).
async function checkFazerCardOrderStatusOnce(env, orderId, fazercardOrderId, fazercard, currentStatus) {
  try {
    const res = await fetch(`${FAZERCARD_BASE_URL}/orders/${encodeURIComponent(fazercardOrderId)}`, {
      headers: { 'X-API-Key': fazercard.apiKey }
    });
    const data = await res.json();
    if (!data.ok) return;
    const status = ((data.order && data.order.status) || '').toLowerCase();

    if (status === 'completed') {
      await patchDoc(env, `orders/${orderId}`, {
        status: 'Success', confirmedAt: new Date(), fazercardStatus: status
      }, ['status', 'confirmedAt', 'fazercardStatus']);
      await notifyOrderStatus(env, orderId, 'Success');
    } else if (status === 'failed' || status === 'cancelled' || status === 'refunded') {
      await finalizeWalletOrderFailure(env, orderId, 'FazerCards order failed/refunded.', { fazercardStatus: status });
    } else if (status && status !== currentStatus) {
      // Skip the write when nothing changed - this runs every minute per Processing order,
      // and an identical patch is still a billed write (plus a re-read on every listener).
      await patchDoc(env, `orders/${orderId}`, { fazercardStatus: status }, ['fazercardStatus']);
    }
    // no status in response - leave as-is, next tick tries again
  } catch (err) {
    console.error('checkFazerCardOrderStatusOnce error', orderId, err);
  }
}

// Called once per cron tick (see runCronSweep) - the FazerCards counterpart
// to pollPendingVoucherOrders.
async function pollPendingFazerCardOrders(env) {
  const fazercardDoc = await getDoc(env, 'settings/fazercard');
  const fazercard = (fazercardDoc && fazercardDoc.fields) || {};
  if (!fazercard.apiKey) return;
  const stuck = await queryProcessingFazerCardOrders(env);
  for (const d of stuck) {
    await checkFazerCardOrderStatusOnce(env, d.id, d.fields.fazercardOrderId, fazercard, d.fields.fazercardStatus);
  }
}

// Direct (game top-up) G2Bulk orders: the only completion signal used to be the G2Bulk webhook (a
// different Worker) or an open admin panel. This checks the same /games/order/status endpoint the
// admin panel uses. Only unambiguous results are applied here: every unit COMPLETED -> Success,
// every unit FAILED/REFUNDED -> Fail (+ wallet refund). Mixed results are left to the webhook/admin.
async function checkG2BulkDirectOrderOnce(env, orderId, fields, g2bulk) {
  try {
    const subs = Array.isArray(fields.g2bulkSubOrders) && fields.g2bulkSubOrders.length
      ? fields.g2bulkSubOrders : [{ index: 1, g2bulkOrderId: fields.g2bulkOrderId }];
    if (subs.some(sb => !sb || !sb.g2bulkOrderId)) return;
    const results = [];
    for (const sb of subs) {
      if (sb.result) { results.push(sb.result); continue; }
      const res = await fetch('https://api.g2bulk.com/v1/games/order/status', {
        method: 'POST',
        headers: { 'X-API-Key': g2bulk.apiKey, 'Content-Type': 'application/json' },
        body: JSON.stringify({ order_id: sb.g2bulkOrderId })
      });
      if (res.status === 401) return 'AUTH_FAILED';
      if (res.status === 429 || res.status >= 500) return 'BACKOFF';
      const data = await res.json().catch(() => ({}));
      const st = String((data.order && data.order.status) || data.status || '').toUpperCase();
      results.push(st === 'COMPLETED' ? 'COMPLETED' : (st === 'FAILED' || st === 'REFUNDED') ? 'FAILED' : null);
    }
    if (results.every(r => r === 'COMPLETED')) {
      await patchDoc(env, `orders/${orderId}`, { status: 'Success', confirmedAt: new Date(), g2bulkStatus: 'COMPLETED', g2bulkSubOrdersDone: subs.length }, ['status', 'confirmedAt', 'g2bulkStatus', 'g2bulkSubOrdersDone']);
      await notifyOrderStatus(env, orderId, 'Success');
    } else if (results.every(r => r === 'FAILED')) {
      await finalizeWalletOrderFailure(env, orderId, 'G2Bulk order failed/refunded.', { g2bulkStatus: 'FAILED' });
    }
  } catch (err) { console.error('checkG2BulkDirectOrderOnce error', orderId, err); }
}

const G2BULK_DIRECT_POLL_CAP = 5; // direct orders checked per cron tick (subrequest budget)
async function pollPendingG2BulkDirectOrders(env) {
  const g2bulkDoc = await getDoc(env, 'settings/g2bulk');
  const g2bulk = (g2bulkDoc && g2bulkDoc.fields) || {};
  if (!g2bulk.apiKey) return;
  const guardDoc = await getDoc(env, 'settings/g2bulkPollGuard');
  if (guardDoc && Number(guardDoc.fields.until) > Date.now()) return;
  const rows = await runQueryPaginated(env, (limit) => ({
    from: [{ collectionId: 'orders' }],
    where: { fieldFilter: { field: { fieldPath: 'status' }, op: 'EQUAL', value: { stringValue: 'Processing' } } },
    orderBy: [{ field: { fieldPath: '__name__' }, direction: 'ASCENDING' }],
    limit
  }), 'orders', 'pollPendingG2BulkDirectOrders', 1000);
  const direct = rows.filter(d => d.fields && d.fields.g2bulkOrderId && !d.fields.g2bulkVoucherPollUrl && !d.fields.fazercardOrderId).slice(0, G2BULK_DIRECT_POLL_CAP);
  for (const d of direct) {
    const r = await checkG2BulkDirectOrderOnce(env, d.id, d.fields, g2bulk);
    if (r === 'AUTH_FAILED') { await patchDoc(env, 'settings/g2bulkPollGuard', { until: Date.now() + 10 * 60 * 1000 }, ['until']); break; }
    if (r === 'BACKOFF') break;
  }
}

// Right after an admin taps Confirm: re-check the provider a few times over ~20 s so a fast order shows
// Success within seconds instead of waiting for the next 1-minute cron tick. Stops as soon as the
// order is Success/Fail; anything still running afterwards is picked up by the cron polls.
async function burstCheckOrder(env, orderId) {
  const g2bulkDoc = await getDoc(env, 'settings/g2bulk');
  const g2bulk = (g2bulkDoc && g2bulkDoc.fields) || {};
  let fazercard = null;
  for (const wait of [3000, 4000, 6000, 8000]) {
    await new Promise(r => setTimeout(r, wait));
    const doc = await getDoc(env, `orders/${orderId}`);
    if (!doc || doc.fields.status !== 'Processing') return;
    const f = doc.fields;
    let r;
    if (f.g2bulkVoucherPollUrl && g2bulk.apiKey) r = await checkVoucherDeliveryStatus(env, orderId, f.g2bulkVoucherPollUrl, g2bulk);
    else if (f.fazercardOrderId) {
      if (!fazercard) { const fd = await getDoc(env, 'settings/fazercard'); fazercard = (fd && fd.fields) || {}; }
      if (fazercard.apiKey) await checkFazerCardOrderStatusOnce(env, orderId, f.fazercardOrderId, fazercard, f.fazercardStatus);
    } else if (f.g2bulkOrderId && g2bulk.apiKey) r = await checkG2BulkDirectOrderOnce(env, orderId, f, g2bulk);
    if (r === 'AUTH_FAILED' || r === 'BACKOFF') return;
  }
}

// ---------------------------------------------------------------------------
// Webhook — handle Confirm/Reject button presses
// ---------------------------------------------------------------------------

// Only the configured admin chat may press Confirm/Reject, and - if the optional
// TELEGRAM_ADMIN_IDS variable is set (comma-separated Telegram user ids) - only those people.
// Without TELEGRAM_ADMIN_IDS every member of the admin chat is still allowed (old behaviour).
function isAuthorizedTelegramActor(env, cb) {
  const chatId = cb && cb.message && cb.message.chat ? cb.message.chat.id : undefined;
  const configured = String(env.TELEGRAM_CHAT_ID || '');
  if (chatId !== undefined && /^-?\d+$/.test(configured) && String(chatId) !== configured) return false;
  const allow = String(env.TELEGRAM_ADMIN_IDS || '').split(',').map(x => x.trim()).filter(Boolean);
  if (allow.length && !allow.includes(String(cb && cb.from && cb.from.id))) return false;
  return true;
}

let _webhookCtx = null; // execution context of the current webhook call (used for the short follow-up poll after Confirm)
async function handleTelegramWebhook(request, env, ctx) {
  _webhookCtx = ctx || null;
  const secretHeader = request.headers.get('X-Telegram-Bot-Api-Secret-Token');
  // Timing-safe compare (was a plain !==) — this secret gates every order-
  // confirm / topup-approve button, so it deserves the same care as the
  // admin-sync secret check below.
  if (!timingSafeEqualStr(secretHeader || '', env.TELEGRAM_WEBHOOK_SECRET || '')) {
    return new Response('forbidden', { status: 403 });
  }

  let update;
  try {
    update = await request.json();
  } catch (err) {
    return new Response('bad json', { status: 400 });
  }

  const cb = update.callback_query;
  if (cb) {
    if (!isAuthorizedTelegramActor(env, cb)) {
      await answerCallback(env, cb.id, 'You are not allowed to do this.', true);
      return new Response('ok');
    }
    const raw = cb.data || '';
    const sep = raw.indexOf(':');
    if (sep === -1) { await answerCallback(env, cb.id, 'Unknown action.'); return new Response('ok'); }
    const action = raw.slice(0, sep);
    const targetId = raw.slice(sep + 1);

    try {
      if (action === 'confirm') await resolveOrder(env, targetId, 'Processing', cb, '⏳ <b>MOVED TO PROCESSING</b>');
      else if (action === 'reject') await resolveOrder(env, targetId, 'Fail', cb, '❌ <b>REJECTED</b>');
      else if (action === 'cost') await showCostDetail(env, targetId, cb);
      else if (action === 'wtopup_confirm') await resolveTopupConfirm(env, targetId, cb);
      else if (action === 'wtopup_reject') await resolveTopup(env, targetId, 'Rejected', cb, '❌ <b>REJECTED</b>');
      // The status button editResolvedMessage() leaves in place after
      // Confirm/Reject - just a label, nothing to do when tapped.
      else if (action === 'status') await answerCallback(env, cb.id, 'This order is already resolved.');
      else await answerCallback(env, cb.id, 'Unknown action.');
      if (['confirm', 'reject', 'wtopup_confirm', 'wtopup_reject'].includes(action)) {
        await writeAudit(env, action, action.startsWith('wtopup') ? 'walletTopup' : 'order', targetId, cb.from, 'Telegram button');
      }
    } catch (err) {
      console.error('webhook handler error', err);
      await answerCallback(env, cb.id, err.message || 'Failed to update — please use the admin panel.');
    }
    return new Response('ok');
  }

  // A plain text message: an admin linking their Telegram account
  // (/link CODE, or /start link_CODE from a deep-link tap - see
  // handleTelegramLinkCommand).
  const msg = update.message;
  if (msg && msg.text) {
    try {
      await handleTelegramLinkCommand(env, msg);
    } catch (err) { console.error('telegram link command error', err); }
  }
  return new Response('ok');
}

// Tapped a game button on the Cost Update index message: send that game's table (Cost, and Price
// if it moved) as a native bordered table message. callback data = "<cycleId>_<index>" split on ':'.
async function showCostDetail(env, data, cb) {
  const [cycleId, idxStr] = String(data).split(':');
  const i = Number(idxStr);
  if (!/^\d+$/.test(cycleId || '') || !Number.isInteger(i) || i < 0) { await answerCallback(env, cb.id, 'Unknown action.'); return; }
  const doc = await getDoc(env, `costUpdates/${cycleId}_${Math.floor(i / COST_DETAIL_CHUNK)}`);
  const row = doc && Array.isArray(doc.fields.items) ? doc.fields.items[i % COST_DETAIL_CHUNK] : null;
  if (!row) { await answerCallback(env, cb.id, 'This update is no longer available.', true); return; }
  await answerCallback(env, cb.id);
  const msg = cb.message || {};
  await sendCostRow(env, row, msg.chat && msg.chat.id ? msg.chat.id : env.TELEGRAM_CHAT_ID, msg.message_thread_id);
}

// Shows the resolved status as the card's button rather than burying it in
// the text - Confirm/Reject are swapped out for a single status button
// (not tappable for any real action; see the 'status' case in the webhook
// callback router) so it's the first thing the admin's eye lands on instead
// of a line at the bottom of the message. statusLine may still carry HTML
// (existing call sites do, e.g. '✅ <b>CONFIRMED</b>') - button labels can't
// render HTML, so it's stripped for the button and only the plain text is
// used.
async function editResolvedMessage(env, id, fields, buildMessage, statusLine) {
  if (!fields.telegramMessageId || !fields.telegramChatId) return;
  const base = { chat_id: fields.telegramChatId, message_id: fields.telegramMessageId };
  const { text, photo } = buildMessage(id, fields);
  const buttonLabel = statusLine.replace(/<[^>]+>/g, '').trim();
  // Bot API 10.3: a real disabled button (`disabled: {}`) instead of a tappable status button.
  // If Telegram rejects it for any reason, fall back to the old tappable `status:` button.
  const btnStyle = /reject|fail/i.test(buttonLabel) ? 'danger' : /confirm|credit|success/i.test(buttonLabel) ? 'success' : 'primary';
  const markupFor = (useDisabled) => ({ inline_keyboard: [[useDisabled
    ? { text: buttonLabel, style: btnStyle, disabled: {} }
    : { text: buttonLabel, callback_data: `status:${id}` }]] });
  // BUG FIX: this used to trust buildMessage()'s `photo` (= f.receiptUrl),
  // but receiptUrl stays set on the doc even when sendPhoto failed at send
  // time and sweepCollection() fell back to a plain text message. Calling
  // editMessageCaption on a message that has no caption (it's plain text)
  // fails with a Telegram Bad Request - tgApi() doesn't throw on that, so
  // the failure was silent: Firestore status updated fine, but the
  // Confirm/Reject buttons and badge in Telegram never changed, no matter
  // which button was tapped or how many times. Prefer the actually-sent
  // message type (telegramMessageIsPhoto, persisted at send time) when
  // it's present; only fall back to the receiptUrl guess for older docs
  // sent before this field existed.
  if (fields.telegramIsRich) {
    const built = buildMessage(id, fields);
    if (built.rich) {
      let r = await tgApi(env, 'editMessageText', { ...base, rich_message: built.rich, reply_markup: markupFor(true) });
      if (r && r.ok === false && !/not modified/i.test(r.description || '')) r = await tgApi(env, 'editMessageText', { ...base, rich_message: built.rich, reply_markup: markupFor(false) });
      // Could not re-render the table: at least make sure the buttons show the new state.
      if (r && r.ok === false && !/not modified/i.test(r.description || '')) {
        console.error('rich card edit failed, updating buttons only', id, JSON.stringify(r));
        await tgApi(env, 'editMessageReplyMarkup', { ...base, reply_markup: markupFor(false) });
      }
      return;
    }
  }
  const isPhotoMessage = fields.telegramMessageIsPhoto !== undefined ? fields.telegramMessageIsPhoto : !!photo;
  const edit = (reply_markup) => isPhotoMessage
    ? tgApi(env, 'editMessageCaption', { ...base, caption: text, parse_mode: 'HTML', reply_markup })
    : tgApi(env, 'editMessageText', { ...base, text, parse_mode: 'HTML', reply_markup });
  const res = await edit(markupFor(true));
  if (res && res.ok === false && !/not modified/i.test(res.description || '')) await edit(markupFor(false));
}

async function resolveOrder(env, orderId, status, cb, statusLine) {
  const path = `orders/${orderId}`;
  const doc = await getDoc(env, path);
  if (!doc) { await answerCallback(env, cb.id, 'Order not found in app.'); return; }
  if (doc.fields.status === 'Success' || doc.fields.status === 'Fail') {
    await answerCallback(env, cb.id, `Already ${doc.fields.status}.`);
    return;
  }

  // Confirm on a G2Bulk/JCPlays/voucher-linked order now actually places it
  // with the provider - same as tapping "Start Processing"/"Auto-Fulfill"
  // in the admin panel - instead of just writing status:"Processing" and
  // leaving the real purchase call for later. That gap used to mean a
  // voucher order confirmed via Telegram never got its G2Bulk purchase call
  // made at all, so it sat in "Processing" with nothing to move it forward
  // until an admin opened the panel and pressed the extra Auto-Fulfill
  // button by hand.
  if (status === 'Processing') {
    const isG2Bulk = !!(doc.fields.g2bulkGameCode && doc.fields.g2bulkCatalogueName);
    const isJCPlays = !isG2Bulk && !!doc.fields.jcplaysProductId;
    const isVoucher = !isG2Bulk && !isJCPlays && !!doc.fields.g2bulkVoucherProductId;
    const isFazerCard = !isG2Bulk && !isJCPlays && !isVoucher && !!(doc.fields.fazercardCategoryId && doc.fields.fazercardOfferId);

    if (isG2Bulk || isJCPlays || isVoucher || isFazerCard) {
      const g2bulkDoc = await getDoc(env, 'settings/g2bulk');
      const jcplaysDoc = await getDoc(env, 'settings/jcplays');
      const fazercardDoc = await getDoc(env, 'settings/fazercard');
      const g2bulk = (g2bulkDoc && g2bulkDoc.fields) || {};
      const jcplays = (jcplaysDoc && jcplaysDoc.fields) || {};
      const fazercard = (fazercardDoc && fazercardDoc.fields) || {};
      const providerLabel = isG2Bulk ? 'G2Bulk' : isJCPlays ? 'JCPlays' : isVoucher ? 'G2Bulk (Voucher)' : 'FazerCards';

      // G2Bulk (direct and voucher) charges the wallet at order time but
      // accepts the order into its queue even when the balance can't cover
      // it - it never auto-fails that order back, so it would otherwise sit
      // "Processing" in Telegram forever with no further signal. Check the
      // LIVE balance before placing and block with a popup alert instead of
      // placing a doomed order. The Confirm/Reject buttons are deliberately
      // left in place (no editResolvedMessage call) so the admin can just
      // top up and tap Confirm again.
      if (isG2Bulk || isVoucher) {
        const balanceInfo = await buildBalanceInfo(g2bulk, doc.fields.price);
        if (balanceInfo && balanceInfo.insufficient) {
          await answerCallback(
            env, cb.id,
            `⚠️ G2Bulk balance low: ${balanceInfo.balanceUsd.toFixed(2)} USD (~${Math.round(balanceInfo.mmkEquivalent).toLocaleString()} MMK) — need ${Number(doc.fields.price || 0).toLocaleString()} MMK.\n\nOrder NOT placed. Top up G2Bulk then tap Confirm again.`,
            true
          );
          return;
        }
      }

      // Atomic claim: a double tap or a Telegram webhook retry must never place the same
      // order with the provider twice (JCPlays has no dedup key, so that would charge twice).
      const claimed = await claimOrderForFulfillment(env, orderId, doc.updateTime);
      if (!claimed) { await answerCallback(env, cb.id, 'Already being processed - please wait a moment.'); return; }
      await answerCallback(env, cb.id, `Confirmed — placing with ${providerLabel}...`);
      if (isG2Bulk) await fulfillG2BulkOrder(env, orderId, doc.fields, g2bulk);
      else if (isJCPlays) await fulfillJCPlaysOrder(env, orderId, doc.fields, jcplays);
      else if (isVoucher) await fulfillG2BulkVoucherOrder(env, orderId, doc.fields, g2bulk);
      else if (isFazerCard) await fulfillFazerCardOrder(env, orderId, doc.fields, fazercard);

      // Re-read after fulfillment - it may have already resolved to
      // Success (JCPlays/instant voucher delivery) or Fail (rejected by the
      // provider), not just "Processing".
      const after = await getDoc(env, path);
      const finalFields = (after && after.fields) || { ...doc.fields, status: 'Processing' };
      const finalStatusLine = finalFields.status === 'Success' ? '✅ <b>CONFIRMED</b>'
        : finalFields.status === 'Fail' ? '❌ <b>REJECTED</b>'
        : '⏳ <b>SENT TO ' + providerLabel.toUpperCase() + ' — PROCESSING</b>';
      await editResolvedMessage(env, orderId, finalFields, buildOrderMessage, finalStatusLine);
      await markCardStatus(env, orderId, finalFields.status);
      await notifyOrderDm(env, orderId, finalFields, finalFields.status);
      // Don't wait for the 1-minute cron: check the provider again a few times right away.
      if (finalFields.status === 'Processing' && _webhookCtx) _webhookCtx.waitUntil(burstCheckOrder(env, orderId).catch(e => console.error('burst check failed', orderId, e)));
      return;
    }
  }

  if (!canTransition('order', doc.fields.status || 'Pending', status)) {
    await answerCallback(env, cb.id, `Already ${doc.fields.status || 'handled'}.`);
    return;
  }
  const orderWrite = await commitWrites(env, [{
    update: {
      name: docName(env, path),
      fields: { status: { stringValue: status }, statusNote: { stringValue: 'Updated via Telegram' } }
    },
    updateMask: { fieldPaths: ['status', 'statusNote'] },
    currentDocument: { updateTime: doc.updateTime } // race guard: fails if the doc changed since we read it
  }]);
  if (commitFailed(orderWrite)) { await answerCallback(env, cb.id, '⚠️ This order just changed - nothing was saved. Tap again.', true); return; }
  await answerCallback(env, cb.id, status === 'Processing' ? '✅ Moved to Processing' : '❌ Marked Fail');
  await editResolvedMessage(env, orderId, { ...doc.fields, status }, buildOrderMessage, statusLine);
  await markCardStatus(env, orderId, status);
  await notifyOrderDm(env, orderId, { ...doc.fields, status }, status);
}

// DMs the customer back inside pcagamestore_bot's own chat once a human
// resolves their order via the Telegram Confirm/Reject buttons here —
// calls gamestore-auth-worker.js's EXISTING /notify-order-status endpoint,
// the same one admin.html already calls when an order is resolved from
// the web panel, so this just closes the same gap for the Telegram-button
// path (mirrors notifyTopupCustomer() below for wallet top-ups). Looks up
// users/{userId}.telegramId - store.html writes that once a customer links
// Telegram via the Mini App; an order from someone who never linked
// Telegram simply has nothing to notify and is skipped. Best-effort: a
// failure here must never break the confirm/reject flow the admin just
// completed.
async function notifyOrderDm(env, orderId, fields, status) {
  const userId = fields.userId;
  if (!userId) return;
  try {
    const userDoc = await getDoc(env, `users/${userId}`);
    const telegramId = userDoc && userDoc.fields.telegramId;
    if (!telegramId) return;
    const fresh = await getDoc(env, `orders/${orderId}`);
    if (fresh && fresh.fields.dmSentStatus === status) return; // this status was already sent to the customer
    const secretDoc = await getDoc(env, 'settings/adminBroadcast');
    const secret = (secretDoc && secretDoc.fields.secret) || env.ADMIN_BROADCAST_SECRET || '';
    if (!secret) { console.error('notifyOrderDm: no adminBroadcast secret configured'); return; }
    const res = await callAuthWorker(env, '/notify-order-status', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        secret, telegramId, orderId, status,
        game: fields.game || '', price: fields.price || 0,
        userName: resolvedCustomerLabel(fields), userId,
        inGameId: fields.playerId || '', serverId: fields.serverId || '',
        packageName: fields.itemAmount || '',
      }),
    });
    if (!res.ok) console.error('notifyOrderDm failed', res.status, await res.text());
    else await patchDoc(env, `orders/${orderId}`, { dmSentStatus: status }, ['dmSentStatus']);
  } catch (err) {
    console.error('notifyOrderDm error', err);
  }
}

// Credits the wallet and marks Completed. The currentDocument.updateTime
// precondition on the walletTopups write means a double press (or the cron
// and a click racing each other) can never credit the same request twice.
async function creditWalletTopup(env, topupId, doc) {
  const amount = Number(doc.fields.amount || 0);
  const userId = doc.fields.userId;
  if (!userId) throw new Error('Top up request has no userId.');

  if (!(amount > 0)) throw new Error('Top up request has an invalid amount.');
  const userPath = `users/${userId}`;

  const creditResult = await commitWrites(env, [
    {
      update: {
        name: docName(env, `walletTopups/${topupId}`),
        fields: {
          status: { stringValue: 'Completed' },
          confirmedAt: { timestampValue: new Date().toISOString() },
          statusNote: { stringValue: 'Updated via Telegram' }
        }
      },
      updateMask: { fieldPaths: ['status', 'confirmedAt', 'statusNote'] },
      currentDocument: { updateTime: doc.updateTime }
    },
    // Atomic server-side increment (no read-then-write race with a wallet
    // order being charged at the same moment).
    walletIncrementWrite(env, userPath, Math.round(amount))
  ]);
  if (creditResult.error) throw new Error('Could not credit the wallet (request changed - try again).');
  return amount;
}

async function resolveTopupConfirm(env, topupId, cb) {
  const path = `walletTopups/${topupId}`;
  const doc = await getDoc(env, path);
  if (!doc) { await answerCallback(env, cb.id, 'Top up request not found.'); return; }
  if (doc.fields.status === 'Completed' || doc.fields.status === 'Rejected') {
    await answerCallback(env, cb.id, `Already ${doc.fields.status}.`);
    return;
  }
  // Crypto top-ups self-verify against FazerCards' own payment status (see
  // fazercards-functions Worker's /fazercard-payment-status) and must never
  // be credited by a human tap - that would credit a balance nobody has
  // actually paid for yet. The sweep above should never send these to
  // Telegram in the first place; this is just the second layer of defense.
  if (doc.fields.fazercardPaymentId) {
    await answerCallbackLong(env, cb, 'This is a crypto top up - it credits itself once FazerCards confirms payment. Do not confirm manually.');
    return;
  }
  const amount = await creditWalletTopup(env, topupId, doc);
  await answerCallback(env, cb.id, `✅ Credited ${amount.toLocaleString()} MMK`);
  await editResolvedMessage(
    env, topupId, { ...doc.fields, status: 'Completed' }, buildTopupMessage,
    '✅ <b>COMPLETED — WALLET CREDITED</b>'
  );
  await notifyTopupCustomer(env, doc, 'Completed');
}

async function resolveTopup(env, topupId, status, cb, statusLine) {
  const path = `walletTopups/${topupId}`;
  const doc = await getDoc(env, path);
  if (!doc) { await answerCallback(env, cb.id, 'Top up request not found.'); return; }
  if (doc.fields.status === 'Completed' || doc.fields.status === 'Rejected') {
    await answerCallback(env, cb.id, `Already ${doc.fields.status}.`);
    return;
  }
  if (!canTransition('topup', doc.fields.status || 'Pending', status)) {
    await answerCallback(env, cb.id, `Already ${doc.fields.status || 'handled'}.`);
    return;
  }
  const rejectWrite = await commitWrites(env, [{
    update: { name: docName(env, path), fields: { status: { stringValue: status } } },
    updateMask: { fieldPaths: ['status'] },
    currentDocument: { updateTime: doc.updateTime }
  }]);
  if (commitFailed(rejectWrite)) { await answerCallback(env, cb.id, '⚠️ This top up just changed - nothing was saved. Tap again.', true); return; }
  await answerCallback(env, cb.id, '❌ Marked Rejected');
  await editResolvedMessage(env, topupId, { ...doc.fields, status }, buildTopupMessage, statusLine);
  await notifyTopupCustomer(env, doc, status);
}

// DMs the customer back inside pcagamestore_bot's own chat (via
// gamestore-auth-worker.js's /notify-topup-status) once a human resolves
// their wallet top-up here. Only fires when the topup doc actually has a
// telegramId (only true for top-ups submitted through that bot's own
// in-chat flow — a store.html web top-up has no Telegram chat to DM back
// into). Best-effort: a failure here must never break the confirm/reject
// flow the admin just completed, so every error is swallowed and logged.
async function notifyTopupCustomer(env, doc, status) {
  // Same lookup notifyOrderDm() uses for orders: the telegramId store.html
  // saved on users/{userId} when the customer logged in with Telegram. The
  // top-up doc's own telegramId (set by the bot's in-chat flow) wins if
  // present, but a Mini App / web top-up doesn't carry one, so fall back to
  // the user's account instead of skipping.
  let telegramId = doc.fields.telegramId;
  if (!telegramId && doc.fields.userId) {
    try {
      const userDoc = await getDoc(env, `users/${doc.fields.userId}`);
      telegramId = userDoc && userDoc.fields && userDoc.fields.telegramId;
    } catch (err) {
      console.error('notifyTopupCustomer: users lookup failed', err);
    }
  }
  if (!telegramId) {
    console.log('notifyTopupCustomer: skipped - no telegramId on the top-up doc or on users/' + (doc.fields.userId || '?') + ' (customer never linked Telegram)');
    return;
  }
  telegramId = String(telegramId);
  try {
    const secretDoc = await getDoc(env, 'settings/adminBroadcast');
    const secret = (secretDoc && secretDoc.fields.secret) || env.ADMIN_BROADCAST_SECRET || '';
    if (!secret) { console.error('notifyTopupCustomer: no adminBroadcast secret configured'); return; }
    const res = await callAuthWorker(env, '/notify-topup-status', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ secret, telegramId, amount: doc.fields.amount || 0, status }),
    });
    if (!res.ok) console.error('notifyTopupCustomer failed', res.status, await res.text());
    else console.log('notifyTopupCustomer: delivered request OK for telegramId', telegramId, 'status', status);
  } catch (err) {
    console.error('notifyTopupCustomer error', err);
  }
}

// ---------------------------------------------------------------------------
// Firebase Auth: ID-token verification + admin custom claims
// ---------------------------------------------------------------------------
// WHY THIS EXISTS: the admin panel signs in with Firebase email/password, but
// Firestore rules can only tell "a real admin" from "anyone who signed up with
// the public Firebase API key" if admins carry a custom claim that only THIS
// Worker (service account) can set. Rules then check
//   request.auth.token.admin == true
// All admins are equal - no per-admin role/permission tiers.
//
// Endpoints (both POST, CORS-enabled):
//   /admin-login-lookup  { username }            -> { ok, email }
//        Replaces the browser's pre-login read of adminUsernames/{username}
//        (which exposed every admin's TOTP secret to the world).
//        Returns ONLY the sign-in email.
//   /admin-sync-claims   (no body)               -> summary JSON
//        Reads adminUsernames, sets {admin:true, adminRole:'full'} on each
//        admin's Firebase Auth user, revokes claims from removed admins, and
//        mirrors each provider's `enabled` flag into the PUBLIC
//        settings/providerFlags doc (the storefront reads that instead of
//        the secret provider docs).
//        Auth: EITHER the TELEGRAM_WEBHOOK_SECRET (header X-Sync-Secret, or
//        ?secret=) for the one-time bootstrap, OR a Bearer ID token of a
//        currently signed-in admin.
//
// Needs no new secrets. The service account behind FIREBASE_CLIENT_EMAIL must be
// allowed to manage Firebase Auth users (the default "firebase-adminsdk"
// account is).

const GOOGLE_SECURETOKEN_JWKS_URL = 'https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com';
const AUTH_ADMIN_SCOPE = 'https://www.googleapis.com/auth/cloud-platform';
const PROVIDER_FLAG_KEYS = ['g2bulk', 'fazercard', 'jcplays'];

let cachedJwks = null;            // { keys: { [kid]: CryptoKey }, expires, fetchedAt }
let cachedAuthAdminToken = null;  // { token, expires }

function corsHeadersAdmin() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Sync-Secret'
  };
}
function corsJsonAdmin(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...corsHeadersAdmin() }
  });
}
function corsPreflightAdmin() {
  return new Response(null, { status: 204, headers: corsHeadersAdmin() });
}

function base64urlToBytes(str) {
  let s = String(str).replace(/-/g, '+').replace(/_/g, '/');
  while (s.length % 4) s += '=';
  return Uint8Array.from(atob(s), c => c.charCodeAt(0));
}

function timingSafeEqualStr(a, b) {
  const ea = new TextEncoder().encode(String(a));
  const eb = new TextEncoder().encode(String(b));
  if (ea.length !== eb.length) return false;
  let diff = 0;
  for (let i = 0; i < ea.length; i++) diff |= ea[i] ^ eb[i];
  return diff === 0;
}

function safeParseJson(str) {
  try { return str ? JSON.parse(str) : null; } catch (e) { return null; }
}

// Same JWT-bearer flow as getAccessToken(), but for an arbitrary scope, with
// its own cache. getAccessToken() is left untouched on purpose (it carries the
// whole cron/webhook load).
async function mintServiceAccountToken(env, scope) {
  const now = Math.floor(Date.now() / 1000);
  const header = { alg: 'RS256', typ: 'JWT' };
  const claim = {
    iss: env.FIREBASE_CLIENT_EMAIL,
    scope,
    aud: 'https://oauth2.googleapis.com/token',
    iat: now,
    exp: now + 3600
  };
  const signingInput = `${base64url(JSON.stringify(header))}.${base64url(JSON.stringify(claim))}`;
  const key = await importPrivateKey(env.FIREBASE_PRIVATE_KEY);
  const signature = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', key, new TextEncoder().encode(signingInput));
  const jwt = `${signingInput}.${base64url(signature)}`;
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=${jwt}`
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Service-account auth failed: ' + JSON.stringify(data));
  return { token: data.access_token, expires: now + (data.expires_in || 3600) };
}

async function getAuthAdminToken(env) {
  const now = Math.floor(Date.now() / 1000);
  if (cachedAuthAdminToken && cachedAuthAdminToken.expires > now + 60) return cachedAuthAdminToken.token;
  cachedAuthAdminToken = await mintServiceAccountToken(env, AUTH_ADMIN_SCOPE);
  return cachedAuthAdminToken.token;
}

async function identityToolkit(env, action, body) {
  const token = await getAuthAdminToken(env);
  const res = await fetch(`https://identitytoolkit.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/${action}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify(body)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`identitytoolkit ${action} failed: ${res.status} ${JSON.stringify(data).slice(0, 300)}`);
  return data;
}

// Google's public keys that sign Firebase ID tokens. Cached per isolate; an
// unknown `kid` triggers at most one refetch per minute so garbage tokens
// can't turn this into a request amplifier.
async function loadGoogleSigningKeys(force) {
  const nowMs = Date.now();
  if (cachedJwks && !force && cachedJwks.expires > nowMs) return cachedJwks.keys;
  if (cachedJwks && force && nowMs - cachedJwks.fetchedAt < 60 * 1000) return cachedJwks.keys;
  const res = await fetch(GOOGLE_SECURETOKEN_JWKS_URL);
  if (!res.ok) throw new Error('Could not load Google signing keys');
  const jwks = await res.json();
  const keys = {};
  for (const jwk of (jwks.keys || [])) {
    keys[jwk.kid] = await crypto.subtle.importKey(
      'jwk', jwk, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['verify']
    );
  }
  const m = /max-age=(\d+)/.exec(res.headers.get('cache-control') || '');
  const ttlMs = (m ? Math.min(parseInt(m[1], 10), 24 * 3600) : 3600) * 1000;
  cachedJwks = { keys, expires: nowMs + ttlMs, fetchedAt: nowMs };
  return keys;
}

// Verifies a Firebase Auth ID token exactly as documented for custom backends:
// RS256 signature against Google's keys, then exp/iat/auth_time, aud === project
// id, iss === https://securetoken.google.com/<project>, non-empty sub.
// Returns the decoded payload (custom claims appear as top-level fields).
async function verifyFirebaseIdToken(env, idToken) {
  if (typeof idToken !== 'string') throw new Error('missing token');
  const parts = idToken.split('.');
  if (parts.length !== 3) throw new Error('malformed token');
  let header, payload;
  try {
    header = JSON.parse(new TextDecoder().decode(base64urlToBytes(parts[0])));
    payload = JSON.parse(new TextDecoder().decode(base64urlToBytes(parts[1])));
  } catch (e) { throw new Error('malformed token'); }
  if (header.alg !== 'RS256' || !header.kid) throw new Error('bad token header');

  let keys = await loadGoogleSigningKeys(false);
  let key = keys[header.kid];
  if (!key) { keys = await loadGoogleSigningKeys(true); key = keys[header.kid]; }
  if (!key) throw new Error('unknown signing key');

  const ok = await crypto.subtle.verify(
    'RSASSA-PKCS1-v1_5', key, base64urlToBytes(parts[2]), new TextEncoder().encode(parts[0] + '.' + parts[1])
  );
  if (!ok) throw new Error('bad signature');

  const now = Math.floor(Date.now() / 1000);
  const skew = 300;
  if (typeof payload.exp !== 'number' || payload.exp <= now) throw new Error('token expired');
  if (typeof payload.iat !== 'number' || payload.iat > now + skew) throw new Error('token issued in the future');
  if (typeof payload.auth_time === 'number' && payload.auth_time > now + skew) throw new Error('bad auth_time');
  if (payload.aud !== env.FIREBASE_PROJECT_ID) throw new Error('wrong audience');
  if (payload.iss !== `https://securetoken.google.com/${env.FIREBASE_PROJECT_ID}`) throw new Error('wrong issuer');
  if (typeof payload.sub !== 'string' || !payload.sub) throw new Error('missing subject');
  return payload;
}

// ---------------------------------------------------------------------------
// Customer registration  (POST /register-user  { idToken } -> { ok, userId, created, banned })
// ---------------------------------------------------------------------------
// The browser never invents a customer ID. After a verified Google / Telegram
// sign-in the store posts the Firebase ID token here and the Worker:
//   1. verifies the token,
//   2. returns the customer's EXISTING ID if this sign-in already owns one
//      (every existing customer keeps their current ID and wallet),
//   3. otherwise issues a fresh random ID, creates users/{id} with a zero
//      wallet, links the sign-in to it, and sends the new customer's details
//      to the admin Telegram chat.
// A link doc (googleAccounts/telegramAccounts/...) is client-writable, so it is
// never trusted on its own: the users doc must also be owned by this sign-in
// (authUid, or the verified e-mail on it).
const CUSTOMER_ID_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // same alphabet the store used for guest IDs

function makeCustomerId() {
  const n = CUSTOMER_ID_ALPHABET.length;
  const limit = 256 - (256 % n); // rejection sampling: no modulo bias
  let out = '';
  while (out.length < 8) {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    for (const b of bytes) {
      if (b < limit && out.length < 8) out += CUSTOMER_ID_ALPHABET[b % n];
    }
  }
  return out;
}

function customerSignInKind(payload) {
  const uid = String(payload.sub || '');
  const provider = payload.firebase && payload.firebase.sign_in_provider;
  if (uid.startsWith('tg_')) return { kind: 'telegram', coll: 'telegramAccounts', label: 'Telegram' };
  if (provider === 'google.com') return { kind: 'google', coll: 'googleAccounts', label: 'Google' };
  if (provider === 'facebook.com') return { kind: 'facebook', coll: 'facebookAccounts', label: 'Facebook' };
  return null;
}

function customerOwnsUserDoc(user, payload, kind) {
  const f = user.fields;
  if (wStr(f.authUid) === String(payload.sub)) return true;
  const email = wStr(payload.email).toLowerCase();
  const verified = payload.email_verified === true;
  if (kind === 'google' && verified && email && wStr(f.googleEmail).toLowerCase() === email) return true;
  if (kind === 'facebook' && verified && email && wStr(f.facebookEmail).toLowerCase() === email) return true;
  return false;
}

async function queryOneUserBy(env, field, value) {
  const token = await getAccessToken(env);
  const res = await fetch(`${firestoreDocsBase(env)}:runQuery`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ structuredQuery: {
      from: [{ collectionId: 'users' }],
      where: { fieldFilter: { field: { fieldPath: field }, op: 'EQUAL', value: { stringValue: String(value) } } },
      limit: 1
    } })
  });
  if (!res.ok) throw new Error('user lookup failed (' + res.status + ')');
  const rows = await res.json();
  const hit = Array.isArray(rows) ? rows.find(r => r && r.document) : null;
  if (!hit) return null;
  return { id: hit.document.name.split('/').pop(), fields: decodeFields(hit.document.fields || {}) };
}

async function findExistingCustomer(env, payload, sign) {
  const uid = String(payload.sub);
  if (sign) {
    const link = await getDoc(env, `${sign.coll}/${uid}`);
    const gid = link ? wStr(link.fields.guestId) : '';
    if (gid) {
      const user = await getDoc(env, `users/${gid}`);
      if (user) {
        let ok = customerOwnsUserDoc(user, payload, sign.kind);
        // Older Telegram accounts were linked before authUid was claimed on the doc.
        if (!ok && sign.kind === 'telegram' && !wStr(user.fields.authUid) && wStr(user.fields.telegramId) === uid.slice(3)) ok = true;
        if (ok) return { id: gid, fields: user.fields };
      }
    }
  }
  // A users doc already claimed by this exact sign-in (also covers e-mail accounts).
  const claimed = await queryOneUserBy(env, 'authUid', uid);
  if (claimed) return claimed;
  // Google / Facebook: a wallet already verified to this e-mail.
  const email = wStr(payload.email).toLowerCase();
  if (sign && email && payload.email_verified === true) {
    if (sign.kind === 'google') { const u = await queryOneUserBy(env, 'googleEmail', email); if (u) return u; }
    if (sign.kind === 'facebook') { const u = await queryOneUserBy(env, 'facebookEmail', email); if (u) return u; }
  }
  return null;
}

// PATCH with currentDocument.exists=false: true if created, false if it already exists.
async function createDocIfAbsent(env, path, fields) {
  const token = await getAccessToken(env);
  const encoded = {};
  for (const k in fields) encoded[k] = encodeValue(fields[k]);
  const res = await fetch(`${firestoreDocsBase(env)}/${path}?currentDocument.exists=false`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ fields: encoded })
  });
  if (res.ok) return true;
  const body = await res.json().catch(() => ({}));
  const status = body && body.error && body.error.status;
  if (status === 'ALREADY_EXISTS' || status === 'FAILED_PRECONDITION' || res.status === 409) return false;
  throw new Error('createDocIfAbsent failed (' + res.status + ')');
}

function notifyNewCustomer(env, ctx, info) {
  if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) return;
  const lines = [
    'New customer registered',
    `ID: <code>${esc(info.id)}</code>`,
    `Name: ${esc(info.name || '-')}`,
    `Sign-in: ${esc(info.label)}`
  ];
  if (info.email) lines.push(`Email: ${esc(info.email)}`);
  if (info.telegramId) lines.push(`Telegram ID: <code>${esc(info.telegramId)}</code>`);
  if (info.username) lines.push(`Username: @${esc(info.username)}`);
  lines.push('Wallet: 0 MMK');
  lines.push(`Time: ${new Date().toISOString().replace('T', ' ').slice(0, 19)} UTC`);
  ctx.waitUntil(
    tgApi(env, 'sendMessage', { chat_id: env.TELEGRAM_CHAT_ID, text: lines.join('\n'), parse_mode: 'HTML' })
      .catch(e => console.error('new-customer Telegram notice failed', e && e.message))
  );
}

async function handleRegisterUser(request, env, ctx) {
  const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
  if (await rateLimitHit(env, 'registerUser', ip, 30, 300)) {
    return corsJson({ ok: false, error: 'Too many attempts. Try again in a few minutes.' }, 429);
  }
  let body;
  try { body = await request.json(); } catch (e) { return corsJson({ ok: false, error: 'Bad request' }, 400); }
  let payload;
  try { payload = await verifyFirebaseIdToken(env, body && body.idToken); }
  catch (e) { return corsJson({ ok: false, error: 'Sign-in could not be verified' }, 401); }

  const uid = String(payload.sub);
  const sign = customerSignInKind(payload);
  const respond = (c, created) => corsJson({ ok: true, userId: c.id, created, banned: !!c.fields.banned });

  const existing = await findExistingCustomer(env, payload, sign);
  if (existing) {
    // Older accounts may not carry their owner uid yet; Firestore rules use it to let
    // the customer read their own wallet/orders. Proven ownership above, so claim it now.
    if (!wStr(existing.fields.authUid)) {
      try { await patchDoc(env, `users/${existing.id}`, { authUid: uid }, ['authUid']); }
      catch (e) { console.error('authUid claim failed', e && e.message); }
    }
    return respond(existing, false);
  }
  if (!sign) return corsJson({ ok: false, error: 'Please sign in with Google or Telegram.' }, 400);

  // One registration per sign-in at a time (double taps, two tabs).
  const claimPath = `registrationClaims/${uid}`;
  if (!(await createDocIfAbsent(env, claimPath, { at: new Date() }))) {
    for (let i = 0; i < 4; i++) {
      await new Promise(r => setTimeout(r, 500));
      const again = await findExistingCustomer(env, payload, sign);
      if (again) return respond(again, false);
    }
    // Stale claim from an attempt that crashed half-way: take it over.
    await patchDoc(env, claimPath, { at: new Date() }, ['at']);
  }

  let id = null;
  for (let i = 0; i < 6 && !id; i++) {
    const candidate = makeCustomerId();
    if (!(await getDoc(env, `users/${candidate}`))) id = candidate;
  }
  if (!id) return corsJson({ ok: false, error: 'Could not issue an ID, please try again.' }, 500);

  const email = wStr(payload.email).toLowerCase();
  const name = wStr(payload.name).slice(0, 60);
  const now = new Date();
  const fields = {
    guestId: id, authUid: uid, walletBalance: 0, createdAt: now, lastSeen: now,
    name, phone: '', photo: wStr(payload.picture), registeredVia: sign.kind, registeredBy: 'worker'
  };
  if (sign.kind === 'google' && email && payload.email_verified === true) fields.googleEmail = email;
  if (sign.kind === 'facebook' && email && payload.email_verified === true) fields.facebookEmail = email;
  if (sign.kind === 'telegram') fields.telegramId = uid.slice(3);
  await patchDoc(env, `users/${id}`, fields, Object.keys(fields));

  const link = { guestId: id, linkedAt: now };
  if (email) link.email = email;
  await patchDoc(env, `${sign.coll}/${uid}`, link, Object.keys(link));

  const tgUsername = sign.kind === 'telegram' ? wStr(payload.telegram_username || payload.username) : '';
  notifyNewCustomer(env, ctx, { id, name, label: sign.label, email, telegramId: sign.kind === 'telegram' ? uid.slice(3) : '', username: tgUsername });
  if (sign.kind === 'telegram') {
    // Welcome DM to the customer inside the bot chat (User ID / Name / Balance).
    ctx.waitUntil(
      callAuthWorker(env, '/notify-connected', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ telegramId: uid.slice(3), guestId: id, name, balance: 0 })
      }).catch(e => console.error('new-customer Telegram DM failed', e && e.message))
    );
  }
  return corsJson({ ok: true, userId: id, created: true, banned: false });
}

// ---------------------------------------------------------------------------
// Customer writes  (POST /create-order, /create-topup, /update-profile)
// ---------------------------------------------------------------------------
// Firestore rules no longer let a browser create or edit users / orders /
// walletTopups. Each request carries { idToken, userId?, ... }:
//   1. the Firebase ID token is verified (same check as /register-user),
//   2. the customer is found - and proven to own the account - from that sign-in,
//   3. only whitelisted fields are copied, every value is length/type checked,
//   4. fields the browser must never control (status, userId, timestamp, wallet
//      flags) are set here, and the doc is written with the service account.
const CLEAN_ID = /^[A-Za-z0-9_-]{1,64}$/;
const MAX_VOUCHER_QTY = 10; // keep in sync with Store.html's MAX_VOUCHER_QTY

function cleanStr(v, max) {
  return wStr(v).replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/[<>`]/g, '').trim().slice(0, max); // no HTML / template characters in customer text
}
function cleanHttpsUrl(v) {
  const t = wStr(v).trim();
  return /^https:\/\/[^\s"'<>`\\]{4,900}$/.test(t) ? t : '';
}
function cleanLocation(v) {
  if (!v || typeof v !== 'object' || Array.isArray(v)) return null;
  const out = {};
  for (const k of Object.keys(v).slice(0, 8)) {
    if (!/^[A-Za-z0-9_]{1,24}$/.test(k)) continue;
    const x = v[k];
    if (typeof x === 'number' && Number.isFinite(x)) out[k] = x;
    else if (typeof x === 'string') out[k] = x.slice(0, 100);
  }
  return Object.keys(out).length ? out : null;
}
function badRequest(msg, status = 400, code = '') {
  return corsJson({ ok: false, error: msg, ...(code ? { code } : {}) }, status);
}

async function resolveCustomer(env, payload, sign, hintId) {
  const hint = wStr(hintId);
  if (CLEAN_ID.test(hint)) {
    const user = await getDoc(env, `users/${hint}`);
    if (user) {
      let ok = customerOwnsUserDoc(user, payload, sign.kind);
      if (!ok && sign.kind === 'telegram' && !wStr(user.fields.authUid) && wStr(user.fields.telegramId) === String(payload.sub).slice(3)) ok = true;
      if (ok) return { id: hint, fields: user.fields };
    }
  }
  return findExistingCustomer(env, payload, sign);
}

async function authCustomerRequest(request, env, bucket, limit, windowSec, failClosed = false) {
  let body;
  try { body = await request.json(); } catch (e) { return { error: badRequest('Bad request') }; }
  let payload;
  try { payload = await verifyFirebaseIdToken(env, body && body.idToken); }
  catch (e) { return { error: badRequest('Please sign in again.', 401) }; }
  if (await rateLimitHit(env, bucket, payload.sub, limit, windowSec, failClosed)) {
    return { error: badRequest('Too many requests. Please try again in a few minutes.', 429) };
  }
  const sign = customerSignInKind(payload);
  if (!sign) return { error: badRequest('Please sign in with Google or Telegram.') };
  const customer = await resolveCustomer(env, payload, sign, body.userId);
  if (!customer) return { error: badRequest('Account not found. Please sign in again.', 404, 'NO_ACCOUNT') };
  if (customer.fields.banned) return { error: badRequest('Your account has been restricted. Contact support.', 403) };
  return { body, payload, customer };
}

// The In-Game ID must pass the game's name check before an order is saved.
//  * Direct G2Bulk games: the Worker calls G2Bulk's public checkPlayerId ITSELF and takes the player
//    name from that answer (the browser's name is never trusted).
//  * FazerCards / Aluu.in games: their check lives behind other Workers, so the Worker can only
//    require the store's "ID verified" flag.
//  * Games with no player ID (vouchers) and games without any name check are not touched.
async function verifyPlayerName(env, f, o) {
  const games = await getGamesCached(env);
  const g = games.find(x => wStr(x.fields.name) === wStr(f.game) && wStr(x.fields.serverName) === wStr(f.serverRegion));
  if (!g) return { ok: true }; // unknown game: the price check rejects it
  const gf = g.fields;
  if (!f.playerId) {
    // A direct top-up game always needs a player ID: leaving it out must not skip the name check.
    // Voucher / gift-card orders carry no player ID and stay untouched.
    const isCodeOrder = !!(f.g2bulkVoucherProductId || f.fazercardGiftCategoryId || f.jcplaysProduct);
    if (!isCodeOrder && (gf.g2bulkCode || gf.fazercardCategoryId || gf.aluuCode)) {
      return { ok: false, reason: 'Please enter your In-Game ID and check it first.' };
    }
    return { ok: true };
  }
  if (gf.g2bulkCode) {
    let data;
    try {
      const res = await fetch('https://api.g2bulk.com/v1/games/checkPlayerId', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ game: String(gf.g2bulkCode), user_id: f.playerId, server_id: f.serverId || undefined }),
        signal: AbortSignal.timeout(8000)
      });
      if (!res.ok) return { ok: false, retry: true, reason: 'Name check is unavailable right now. Please try again in a moment.' };
      data = await res.json();
    } catch (e) {
      return { ok: false, retry: true, reason: 'Name check is unavailable right now. Please try again in a moment.' };
    }
    if (data && data.valid === 'valid') return { ok: true, name: cleanStr(data.name, 80) };
    return { ok: false, reason: 'ID not found. Please check your In-Game ID and try again.' };
  }
  if (gf.fazercardCategoryId || gf.aluuCode) {
    return o.idVerified === true ? { ok: true } : { ok: false, reason: 'Please check your In-Game ID first - the player name must appear before you can buy.' };
  }
  return { ok: true };
}

// ---- Abuse guards -----------------------------------------------------------
const MAX_PENDING_RECEIPT_ORDERS = 5;  // unpaid-approval orders one customer may have waiting
const MAX_PENDING_WALLET_ORDERS = 10;  // wallet orders waiting for the provider
const MAX_PENDING_TOPUPS = 3;
const DEFAULT_HIGH_VALUE_MMK = 300000;  // override with env.HIGH_VALUE_ALERT_MMK

// How many docs of `collectionId` this customer has with status Pending (stops counting at `cap`+1).
async function countPendingFor(env, collectionId, userId, cap) {
  const token = await getAccessToken(env);
  const res = await fetch(`https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ structuredQuery: {
      from: [{ collectionId }],
      where: { compositeFilter: { op: 'AND', filters: [
        { fieldFilter: { field: { fieldPath: 'userId' }, op: 'EQUAL', value: { stringValue: String(userId) } } },
        { fieldFilter: { field: { fieldPath: 'status' }, op: 'EQUAL', value: { stringValue: 'Pending' } } }
      ] } },
      limit: cap + 1
    } })
  });
  const rows = await res.json();
  if (!res.ok || !Array.isArray(rows)) throw new Error('pending count failed');
  return rows.filter(r => r && r.document).length;
}

// True when the same receipt image / transaction id is already on a live (not rejected) order or top-up.
async function receiptAlreadyUsed(env, receiptUrl, transactionId) {
  const dead = new Set(['Fail', 'Rejected', 'Cancelled', 'Refunded']);
  const probes = [];
  if (receiptUrl) { probes.push(queryEqualString(env, 'orders', 'receiptUrl', receiptUrl, 10)); probes.push(queryEqualString(env, 'walletTopups', 'receiptUrl', receiptUrl, 10)); }
  if (transactionId) { probes.push(queryEqualString(env, 'orders', 'transactionId', transactionId, 10)); probes.push(queryEqualString(env, 'walletTopups', 'transactionId', transactionId, 10)); }
  const results = await Promise.all(probes);
  return results.some(list => (list || []).some(d => !dead.has(wStr(d.fields.status))));
}

// Big-money alert to the admin's Telegram (never blocks the customer).
function alertHighValue(env, ctx, kind, id, f) {
  const limit = Number(env.HIGH_VALUE_ALERT_MMK) || DEFAULT_HIGH_VALUE_MMK;
  const amount = Number(kind === 'order' ? f.price : f.amount) || 0;
  if (amount < limit) return;
  const text = `💰 <b>High-value ${kind}</b>\n\n${kind === 'order' ? 'Order' : 'Top-up'}: <code>${id}</code>\n`
    + `User: <code>${f.userId || '-'}</code>\nAmount: ${amount.toLocaleString()} MMK`
    + (f.game ? `\nGame: ${f.game}` : '') + `\n\nCheck the receipt carefully before approving.`;
  ctx.waitUntil(tgApi(env, 'sendMessage', { chat_id: env.TELEGRAM_CHAT_ID, parse_mode: 'HTML', text }).catch(() => {}));
}

// Who did what (Telegram Confirm/Reject buttons). Written with the service account; keep the
// auditLog collection admin-read-only / no client writes in firestore.rules.
async function writeAudit(env, action, targetType, targetId, actor, detail) {
  try {
    await createDoc(env, 'auditLog', {
      action, targetType, targetId: String(targetId), source: 'telegram',
      actorTelegramId: actor && actor.id ? String(actor.id) : '',
      actorName: actor ? cleanStr([actor.first_name, actor.last_name].filter(Boolean).join(' ') || actor.username || '', 80) : '',
      detail: cleanStr(detail || '', 200), at: new Date()
    });
  } catch (e) { console.error('audit write failed', action, targetId, e && e.message); }
}

async function handleCreateOrder(request, env, ctx) {
  const auth = await authCustomerRequest(request, env, 'createOrder', 10, 300, true);
  if (auth.error) return auth.error;
  const { body, payload, customer } = auth;
  const o = body.order && typeof body.order === 'object' ? body.order : null;
  if (!o) return badRequest('Bad request');
  const wallet = body.wallet === true;

  const f = {
    playerId: cleanStr(o.playerId, 64),
    serverId: cleanStr(o.serverId, 64),
    serverRegion: cleanStr(o.serverRegion, 80),
    game: cleanStr(o.game, 120),
    itemAmount: cleanStr(o.itemAmount, 160)
  };
  if (!f.game || !f.itemAmount) return badRequest('Missing game or package.');
  const inGameName = cleanStr(o.inGameName, 80);
  if (inGameName) f.inGameName = inGameName;
  // Which price tab the package came from (Currency / Package / a custom tab) - display only.
  const packageTab = cleanStr(o.packageTab, 60);
  if (packageTab) f.packageTab = packageTab;
  const transactionId = cleanStr(o.transactionId, 80);
  if (transactionId) f.transactionId = transactionId;

  const price = Number(o.price);
  if (!Number.isInteger(price) || price < 0 || price > 50000000) return badRequest('Invalid price.');
  f.price = price;
  const cost = Number(o.cost);
  f.cost = Number.isFinite(cost) && cost >= 0 ? Math.round(cost) : 0;
  // Quantity is only meaningful for G2Bulk voucher packages (one purchase call
  // buys N codes). Every other order is always exactly 1, whatever the browser says.
  const qty = parseInt(o.quantity, 10);
  f.quantity = (cleanStr(o.g2bulkVoucherProductId, 160) && qty >= 1 && qty <= MAX_VOUCHER_QTY) ? qty : 1;

  // Provider links: a pair is kept only when both halves are present (same rule the store used).
  const pair = (a, b) => {
    const x = cleanStr(o[a], 160), y = cleanStr(o[b], 160);
    if (x && y) { f[a] = x; f[b] = y; }
  };
  pair('g2bulkGameCode', 'g2bulkCatalogueName');
  pair('jcplaysProduct', 'jcplaysProductId');
  pair('fazercardCategoryId', 'fazercardOfferId');
  pair('fazercardGiftCategoryId', 'fazercardGiftCardId');
  const voucherId = cleanStr(o.g2bulkVoucherProductId, 160);
  if (voucherId) f.g2bulkVoucherProductId = voucherId;

  const loc = cleanLocation(o.location);
  if (loc) f.location = loc;

  // Run the provider name check and the pending-orders count AT THE SAME TIME
  // (they used to run one after the other, adding their latencies together).
  const pendingMax = wallet ? MAX_PENDING_WALLET_ORDERS : MAX_PENDING_RECEIPT_ORDERS;
  const pendingP = countPendingFor(env, 'orders', customer.id, pendingMax).then(n => ({ n }), e => ({ e }));
  const idCheck = await verifyPlayerName(env, f, o);
  if (!idCheck.ok) return badRequest(idCheck.reason, idCheck.retry ? 503 : 400, 'ID_CHECK_FAILED');
  if (idCheck.name) f.inGameName = idCheck.name; // the checked name, straight from the provider

  // Too many orders already waiting? (spam / Telegram flooding)
  const pendingRes = await pendingP;
  if (pendingRes.e) return badRequest('Could not verify your orders right now. Please try again.', 503);
  if (pendingRes.n >= pendingMax) {
    return badRequest('You already have several orders waiting. Please wait until they are processed.', 429, 'TOO_MANY_PENDING');
  }

  if (wallet) {
    // Wallet order = a REQUEST. fulfillWalletOrders() proves owner / price / balance,
    // calls the provider, and only then debits the wallet.
    f.paymentMethod = 'Wallet Balance';
    f.paidFromWallet = true;
    f.walletFlow = WALLET_FLOW;
    f.authUid = String(payload.sub);
  } else {
    f.paymentMethod = cleanStr(o.paymentMethod, 80);
    f.receiptUrl = cleanHttpsUrl(o.receiptUrl);
    if (!f.paymentMethod) return badRequest('Please choose a payment method.');
    if (!f.receiptUrl) return badRequest('Payment receipt is required.');
    try {
      if (await receiptAlreadyUsed(env, f.receiptUrl, f.transactionId)) {
        return badRequest('This payment receipt / transaction ID has already been used.', 409, 'DUPLICATE_RECEIPT');
      }
    } catch (e) {
      return badRequest('Could not verify your receipt right now. Please try again.', 503);
    }

    // Receipt orders: the Worker re-derives the price from the games catalogue
    // (same check wallet orders get in gateWalletOrder) BEFORE the order exists,
    // so a tampered price never reaches Telegram / the admin queue. Cost is also
    // taken from the catalogue, never from the browser.
    const priced = await verifyWalletPrice(env, f);
    if (!priced.ok) return badRequest(priced.reason, 409, 'PRICE_CHANGED');
    f.price = priced.price;
    f.cost = priced.cost;
  }

  f.status = 'Pending';
  f.timestamp = new Date();
  f.userId = customer.id;
  f.userName = cleanStr(customer.fields.name, 60) || customer.id;
  f.userPhone = cleanStr(customer.fields.phone, 30);

  let orderId = '';
  for (let i = 0; i < 8 && !orderId; i++) {
    const buf = new Uint32Array(1);
    crypto.getRandomValues(buf);
    const candidate = String(10000000 + (buf[0] % 90000000));
    if (await createDocIfAbsent(env, `orders/${candidate}`, f)) orderId = candidate;
  }
  if (!orderId) return badRequest('Could not save the order. Please try again.', 500);

  alertHighValue(env, ctx, 'order', orderId, f);
  // Telegram card / wallet auto-fulfil right away (the cron stays as the safety net).
  // After the sweep, DM the customer's live order tracker (status as buttons) if they
  // linked Telegram. Done AFTER the sweep so our tracker writes can't race the wallet
  // claim, and skipped if the sweep already moved the order past Pending (that status
  // change sends its own tracker update).
  ctx.waitUntil((async () => {
    try { await runPingSweep(env, 'order'); } catch (e) { console.error('post-create sweep failed', e); }
    try {
      const fresh = await getDoc(env, `orders/${orderId}`);
      if (fresh && fresh.fields.status === 'Pending') await notifyOrderDm(env, orderId, fresh.fields, 'Pending');
    } catch (e) { console.error('pending order DM failed', e); }
  })());
  return corsJson({ ok: true, orderId });
}


// ---------------------------------------------------------------------------
// POST /my-state - the ONLY way a customer's browser reads its private data.
// Replaces direct Firestore reads/listeners on users, orders, walletTopups and
// users/{id}/inbox so those collections can be locked to admin-only in
// firestore.rules. ONE call returns everything (profile+balance, recent orders,
// top-ups, inbox) to keep the daily Worker request count low. The caller is
// verified by authCustomerRequest (Firebase ID token -> their own account), so a
// customer can only ever get their own data, and internal/provider fields are
// stripped before anything leaves the Worker.
// Body: { idToken, userId?, parts?: ['user','orders','topups','inbox'] }
// ---------------------------------------------------------------------------
const MY_STATE_DENY = /^(authUid|banned|adminNote|notes?|ip|userAgent|idempotency|cost|profit|supplier|provider|g2bulk(?!Status$|OrderId$)|fazercard(?!OrderId$)|jcplays(?!OrderId$)|aluu|bay2game|wallet(Fulfill|Refund|Claim)|claim)/i;
function publicFields(fields) {
  const out = {};
  for (const k in fields) if (!MY_STATE_DENY.test(k)) out[k] = fields[k];
  // The storefront only needs to know an order was code-linked (to show the "contact support" box when no code
  // was delivered), not which supplier product it maps to - so expose a boolean instead of the denied IDs.
  if (fields.g2bulkVoucherProductId || fields.jcplaysProductId || fields.fazercardGiftCardId) out.codeLinked = true;
  return out;
}
function tsMs(v) { const t = v instanceof Date ? v.getTime() : new Date(v).getTime(); return Number.isFinite(t) ? t : 0; }

async function queryOwned(env, collectionId, userId, cap, parentPath) {
  const token = await getAccessToken(env);
  const base = `https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents`;
  const sq = parentPath
    ? { from: [{ collectionId }], limit: cap }
    : { from: [{ collectionId }], limit: 300, where: { fieldFilter: { field: { fieldPath: 'userId' }, op: 'EQUAL', value: { stringValue: String(userId) } } } };
  const res = await fetch(`${base}${parentPath ? '/' + parentPath : ''}:runQuery`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ structuredQuery: sq })
  });
  const rows = await res.json();
  if (!res.ok || !Array.isArray(rows)) throw new Error('my-state query failed: ' + collectionId);
  return rows.filter(r => r.document)
    .map(r => ({ id: r.document.name.split('/').pop(), ...publicFields(decodeFields(r.document.fields || {})) }))
    .sort((a, b) => tsMs(b.timestamp) - tsMs(a.timestamp))   // newest first; sorted here so no composite index is needed
    .slice(0, cap);
}


// Lifetime totals for the Profile VIP card, computed here so the browser never reads orders/top-ups.
async function statsFor(env, customer) {
  const token = await getAccessToken(env);
  const run = async (collectionId, status) => {
    const res = await fetch(`https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ structuredQuery: { from: [{ collectionId }], limit: 2000, where: { compositeFilter: { op: 'AND', filters: [
        { fieldFilter: { field: { fieldPath: 'userId' }, op: 'EQUAL', value: { stringValue: String(customer.id) } } },
        { fieldFilter: { field: { fieldPath: 'status' }, op: 'EQUAL', value: { stringValue: status } } }
      ] } } } })
    });
    const rows = await res.json();
    if (!res.ok || !Array.isArray(rows)) throw new Error('stats query failed: ' + collectionId);
    return rows.filter(r => r.document).map(r => decodeFields(r.document.fields || {}));
  };
  const [orders, topups] = await Promise.all([run('orders', 'Success'), run('walletTopups', 'Completed')]);
  const f = customer.fields; const games = new Set();
  let total = 0, count = 0;
  orders.forEach(o => { total += Number(o.price) || 0; count++; if (o.game) games.add(o.game); });
  topups.forEach(t => { total += Number(t.amount) || 0; });
  total += (Number(f.archivedSpent) || 0) + (Number(f.legacyArchivedSpent) || 0) + (Number(f.archivedTopups) || 0);
  count += (Number(f.archivedOrders) || 0) + (Number(f.legacyArchivedOrders) || 0);
  [f.archivedGames, f.legacyArchivedGames].forEach(a => { if (Array.isArray(a)) a.forEach(g => { if (g) games.add(g); }); });
  return { total, count, games: games.size };
}

// g2bulk_cache is public-read / admin-write in firestore.rules, so customers can't fill it. The Worker
// (service account) does it from the 1-minute cron: each voucher category is refreshed about every 4 minutes
// (spread across ticks so a tick never makes more than a few subrequests). Error bodies are never cached.
async function refreshG2BulkCache(env) {
  const games = await getGamesCached(env);
  const ids = [...new Set(games.map(g => g.fields && g.fields.g2bulkVoucherCategory).filter(Boolean).map(String))];
  const slot = Math.floor(Date.now() / 60000);
  for (let i = 0; i < ids.length; i++) {
    if ((slot + i) % 4 !== 0) continue;
    try {
      const res = await fetch(`https://api.g2bulk.com/v1/category/${encodeURIComponent(ids[i])}`, { signal: AbortSignal.timeout(8000) });
      const data = await res.json();
      if (!res.ok || !data || data.success === false) continue;
      await patchDoc(env, `g2bulk_cache/cat_${ids[i]}`, { data, fetchedAt: Date.now() }, ['data', 'fetchedAt']);
    } catch (e) { console.error('g2bulk cache refresh failed for', ids[i], e && e.message); }
  }
}


// ---- History is loaded one DAY at a time: "orders" = today's orders (+ any older order still Pending/Processing),
// "dayOrders" = one chosen past day, read only when the customer opens that day. ----
function dayRangeIso(day, tzMin) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(day || ''))) return null;
  const off = Number.isFinite(Number(tzMin)) ? Math.max(-840, Math.min(840, Number(tzMin))) : 390;
  const [y, m, d] = day.split('-').map(Number);
  const start = Date.UTC(y, m - 1, d) - off * 60000;
  return { start: new Date(start).toISOString(), end: new Date(start + 86400000).toISOString() };
}
async function ownedOrdersQuery(env, where) {
  const token = await getAccessToken(env);
  const res = await fetch(`https://firestore.googleapis.com/v1/projects/${env.FIREBASE_PROJECT_ID}/databases/(default)/documents:runQuery`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ structuredQuery: { from: [{ collectionId: 'orders' }], where: { compositeFilter: { op: 'AND', filters: where } }, limit: 200 } })
  });
  const rows = await res.json();
  if (!res.ok || !Array.isArray(rows)) throw new Error('orders query failed: ' + res.status + ' ' + JSON.stringify(rows).slice(0, 300));
  return rows.filter(r => r.document).map(r => ({ id: r.document.name.split('/').pop(), ...publicFields(decodeFields(r.document.fields || {})) }));
}
const eqUser = uid => ({ fieldFilter: { field: { fieldPath: 'userId' }, op: 'EQUAL', value: { stringValue: String(uid) } } });
async function ordersForDay(env, userId, range) {
  try {
    return await ownedOrdersQuery(env, [eqUser(userId),
      { fieldFilter: { field: { fieldPath: 'timestamp' }, op: 'GREATER_THAN_OR_EQUAL', value: { timestampValue: range.start } } },
      { fieldFilter: { field: { fieldPath: 'timestamp' }, op: 'LESS_THAN', value: { timestampValue: range.end } } }]);
  } catch (e) {
    // Composite index (orders: userId + timestamp) missing -> log Firestore's create-index link and fall back
    console.error('ordersForDay: needs composite index userId+timestamp:', e && e.message);
    const all = await queryOwned(env, 'orders', userId, 300);
    const a = Date.parse(range.start), b = Date.parse(range.end);
    return all.filter(o => { const t = tsMs(o.timestamp); return t >= a && t < b; });
  }
}
async function openOrders(env, userId) {
  try {
    return await ownedOrdersQuery(env, [eqUser(userId),
      { fieldFilter: { field: { fieldPath: 'status' }, op: 'IN', value: { arrayValue: { values: [{ stringValue: 'Pending' }, { stringValue: 'Processing' }] } } } }]);
  } catch (e) { console.error('openOrders failed:', e && e.message); return []; }
}
async function ordersToday(env, userId, range) {
  const [today, open] = await Promise.all([ordersForDay(env, userId, range), openOrders(env, userId)]);
  const map = new Map(); [...today, ...open].forEach(o => map.set(o.id, o));
  return [...map.values()].sort((a, b) => tsMs(b.timestamp) - tsMs(a.timestamp)).slice(0, 200);
}

async function handleMyState(request, env) {
  const auth = await authCustomerRequest(request, env, 'myState', 60, 60);
  if (auth.error) return auth.error;
  const { body, customer } = auth;
  const want = new Set(Array.isArray(body.parts) && body.parts.length ? body.parts : ['user', 'orders', 'topups', 'inbox']);
  try {
    const range = dayRangeIso(body.day, body.tzMin);
    const [orders, topups, inbox, stats, dayOrders] = await Promise.all([
      want.has('orders') ? (range ? ordersToday(env, customer.id, range) : queryOwned(env, 'orders', customer.id, 100)) : null,
      want.has('topups') ? queryOwned(env, 'walletTopups', customer.id, 50) : null,
      want.has('inbox') ? queryOwned(env, 'inbox', customer.id, 50, `users/${encodeURIComponent(customer.id)}`) : null,
      want.has('stats') ? statsFor(env, customer) : null,
      (want.has('dayOrders') && range) ? ordersForDay(env, customer.id, range).then(a => a.sort((x, y) => tsMs(y.timestamp) - tsMs(x.timestamp))) : null
    ]);
    return corsJson({
      ok: true,
      user: want.has('user') ? { id: customer.id, ...publicFields(customer.fields) } : undefined,
      orders, topups, inbox, stats, dayOrders
    });
  } catch (err) {
    console.error('/my-state failed', err);
    return corsJson({ ok: false, error: 'Could not load your data. Please try again.' }, 503);
  }
}

async function handleCreateTopup(request, env, ctx) {
  const auth = await authCustomerRequest(request, env, 'createTopup', 5, 300, true);
  if (auth.error) return auth.error;
  const { body, customer } = auth;
  const t = body.topup && typeof body.topup === 'object' ? body.topup : null;
  if (!t) return badRequest('Bad request');

  const amount = Number(t.amount);
  if (!Number.isInteger(amount) || amount <= 0 || amount > 10000000) return badRequest('Invalid top up amount.');
  const paymentMethod = cleanStr(t.paymentMethod, 80);
  const receiptUrl = cleanHttpsUrl(t.receiptUrl);
  if (!paymentMethod) return badRequest('Please choose a payment method.');
  if (!receiptUrl) return badRequest('Payment receipt is required.');
  const topupTxId = cleanStr(t.transactionId, 80);

  try {
    if ((await countPendingFor(env, 'walletTopups', customer.id, MAX_PENDING_TOPUPS)) >= MAX_PENDING_TOPUPS) {
      return badRequest('You already have top-ups waiting for approval. Please wait until they are processed.', 429, 'TOO_MANY_PENDING');
    }
    if (await receiptAlreadyUsed(env, receiptUrl, topupTxId)) {
      return badRequest('This payment receipt has already been used.', 409, 'DUPLICATE_RECEIPT');
    }
  } catch (e) {
    return badRequest('Could not verify your top-up right now. Please try again.', 503);
  }

  const created = await createDoc(env, 'walletTopups', {
    amount, paymentMethod, receiptUrl,
    ...(topupTxId ? { transactionId: topupTxId } : {}),
    status: 'Pending',
    timestamp: new Date(),
    userId: customer.id,
    userName: cleanStr(customer.fields.name, 60) || customer.id,
    userPhone: cleanStr(customer.fields.phone, 30)
  });
  const topupId = created && created.name ? String(created.name).split('/').pop() : '';
  if (!topupId) return badRequest('Could not save the top up request. Please try again.', 500);

  alertHighValue(env, ctx, 'topup', topupId, { amount, userId: customer.id });
  ctx.waitUntil(runPingSweep(env, 'topup'));
  return corsJson({ ok: true, topupId });
}

// Profile-type edits only. Money / ban / role / ID fields can never be set from here.
async function handleUpdateProfile(request, env) {
  const auth = await authCustomerRequest(request, env, 'updateProfile', 60, 300);
  if (auth.error) return auth.error;
  const { body, payload, customer } = auth;

  const fields = { lastSeen: new Date() };
  if (typeof body.name === 'string') fields.name = cleanStr(body.name, 60);
  if (typeof body.phone === 'string') fields.phone = cleanStr(body.phone, 30);
  if (typeof body.photo === 'string') fields.photo = body.photo === '' ? '' : cleanHttpsUrl(body.photo);
  if (!wStr(customer.fields.authUid)) fields.authUid = String(payload.sub); // older account: claim owner uid (ownership proven above)

  const write = {
    update: { name: docName(env, `users/${customer.id}`), fields: {} },
    updateMask: { fieldPaths: Object.keys(fields) },
    currentDocument: { exists: true } // never re-create an account the admin deleted
  };
  for (const k in fields) write.update.fields[k] = encodeValue(fields[k]);

  const fav = body.favorite;
  if (fav && typeof fav === 'object' && CLEAN_ID.test(wStr(fav.gameId))) {
    const el = { values: [{ stringValue: String(fav.gameId) }] };
    write.updateTransforms = [{ fieldPath: 'favoriteGames', ...(fav.add === false ? { removeAllFromArray: el } : { appendMissingElements: el }) }];
  }

  const result = await commitWrites(env, [write]);
  if (result && result.error) return badRequest('Could not update your profile.', 500);
  return corsJson({ ok: true });
}

// POST { username } -> { ok:true, email } | { ok:false }
// Deliberately returns nothing but the email (never totpSecret / permissions /
// telegram fields). Unknown usernames get the same { ok:false } as any other
// failure, so the login screen can keep its generic "Wrong username or password".
async function handleAdminLoginLookup(request, env) {
  // No auth exists yet at this point in the login flow (this IS the first
  // step), so an IP-based rate limit is the only guard against a script
  // trying admin usernames one by one to enumerate valid admin emails.
  // 10 tries / 5 min is generous for a real person mistyping their username,
  // brutal for a brute-forcer.
  const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
  if (await rateLimitHit(env, 'adminLoginLookup', ip, 10, 300)) {
    return corsJsonAdmin({ ok: false, error: 'Too many attempts. Try again in a few minutes.' }, 429);
  }
  let body;
  try { body = await request.json(); } catch (e) { return corsJsonAdmin({ ok: false }, 400); }
  const username = String(body.username || '').trim().toLowerCase();
  if (!/^[a-z0-9_.]{3,64}$/.test(username)) return corsJsonAdmin({ ok: false });
  const doc = await getDoc(env, `adminUsernames/${username}`);
  if (!doc || !doc.fields.email) return corsJsonAdmin({ ok: false });
  return corsJsonAdmin({ ok: true, email: doc.fields.email });
}

async function authorizeAdminSync(request, env, url) {
  const secret = request.headers.get('X-Sync-Secret') || url.searchParams.get('secret') || '';
  if (secret && env.TELEGRAM_WEBHOOK_SECRET && timingSafeEqualStr(secret, env.TELEGRAM_WEBHOOK_SECRET)) {
    return { via: 'secret' };
  }
  const m = /^Bearer\s+(.+)$/i.exec(request.headers.get('Authorization') || '');
  if (!m) return null;
  let payload;
  try { payload = await verifyFirebaseIdToken(env, m[1]); } catch (e) { return null; }
  // NOTE: the token is NOT required to carry admin:true any more. That made a brand-new /
  // never-synced admin unable to ever get the claim (it needs the claim to ask for the claim).
  // A valid Firebase sign-in whose e-mail is listed in adminUsernames (checked just below) is enough;
  // the sync only copies adminUsernames -> custom claims, so it grants nothing that list doesn't.
  if (!payload.email) return null;
  // Confirm against Firestore that this person is a listed admin before running the sync.
  const rows = await queryEqualString(env, 'adminUsernames', 'email', String(payload.email).toLowerCase(), 5);
  return rows.length ? { via: 'admin', uid: payload.sub } : null;
}

async function syncAdminClaims(env) {
  const summary = { updated: [], unchanged: [], missingAuthUser: [], revoked: [], errors: [] };

  const admins = await queryAllDocs(env, 'adminUsernames');
  // queryAllDocs() swallows errors and returns []. Never treat that as "there
  // are no admins" - it would revoke everyone.
  if (!admins.length) throw new Error('No adminUsernames found (or the read failed) - aborting without changes.');

  const desiredUids = [];
  for (const a of admins) {
    const email = String(a.fields.email || '').trim().toLowerCase();
    if (!email) { summary.errors.push(`${a.id}: no email on the adminUsernames doc`); continue; }
    // Role/permission tiers removed: every admin gets full access.
    const role = 'full';
    try {
      const lookup = await identityToolkit(env, 'accounts:lookup', { email: [email] });
      const user = (lookup.users || [])[0];
      if (!user) { summary.missingAuthUser.push(a.id); continue; }
      desiredUids.push(user.localId);
      const current = safeParseJson(user.customAttributes) || {};
      if (current.admin === true && current.adminRole === role) { summary.unchanged.push(a.id); continue; }
      await identityToolkit(env, 'accounts:update', {
        localId: user.localId,
        customAttributes: JSON.stringify({ ...current, admin: true, adminRole: role })
      });
      summary.updated.push(a.id);
    } catch (e) {
      summary.errors.push(`${a.id}: ${e.message}`);
    }
  }

  // Revoke claims from admins that were removed since the last sync. Only when
  // everything above went cleanly, so a partial failure can never lock people out.
  if (!summary.errors.length) {
    const prevDoc = await getDoc(env, 'settings/adminClaimUids');
    const previous = (prevDoc && Array.isArray(prevDoc.fields.uids)) ? prevDoc.fields.uids : [];
    for (const uid of previous.filter(u => !desiredUids.includes(u))) {
      try {
        const lookup = await identityToolkit(env, 'accounts:lookup', { localId: [uid] });
        const user = (lookup.users || [])[0];
        if (user) {
          const attrs = safeParseJson(user.customAttributes) || {};
          delete attrs.admin; delete attrs.adminRole;
          await identityToolkit(env, 'accounts:update', {
            localId: uid,
            customAttributes: JSON.stringify(attrs),
            validSince: String(Math.floor(Date.now() / 1000)) // also kills their refresh tokens
          });
        }
        summary.revoked.push(uid);
      } catch (e) { summary.errors.push(`revoke ${uid}: ${e.message}`); }
    }
    if (!summary.errors.length) {
      await patchDoc(env, 'settings/adminClaimUids', { uids: desiredUids, updatedAt: new Date().toISOString() }, ['uids', 'updatedAt']);
    }
  }

  // Public mirror of each provider's on/off switch. The storefront only needs
  // "is this provider enabled?", and must not read settings/g2bulk etc. (they
  // hold API keys) once the Firestore rules are tightened.
  const flags = {};
  for (const p of PROVIDER_FLAG_KEYS) {
    const d = await getDoc(env, `settings/${p}`);
    flags[p] = !(d && d.fields.enabled === false);
  }
  await patchDoc(env, 'settings/providerFlags', flags, PROVIDER_FLAG_KEYS);
  summary.providerFlags = flags;
  return summary;
}

async function handleAdminSyncClaims(request, env, url) {
  const who = await authorizeAdminSync(request, env, url);
  if (!who) return corsJsonAdmin({ ok: false, error: 'forbidden' }, 403);
  try {
    const summary = await syncAdminClaims(env);
    return corsJsonAdmin({ ok: true, via: who.via, ...summary });
  } catch (err) {
    console.error('syncAdminClaims failed', err);
    return corsJsonAdmin({ ok: false, error: err.message }, 500);
  }
}

// ---------------------------------------------------------------------------
// Cloudflare usage for the admin Dashboard - POST /cf-usage
// ---------------------------------------------------------------------------
// Returns today's (UTC) request/error totals for EVERY Worker on the account
// plus a per-Worker breakdown, straight from Cloudflare's GraphQL Analytics
// API - i.e. real numbers including storefront traffic, not just what the
// admin panel itself sends.
//
// Needs two secrets (nothing else):
//   wrangler secret put CF_API_TOKEN    - an Account API token with the single
//                                         permission  Account > Account Analytics > Read
//   wrangler secret put CF_ACCOUNT_ID   - Dashboard > Workers & Pages > Account ID
//
// Auth: a Bearer Firebase ID token carrying the `admin: true` custom claim
// (the same claim /admin-sync-claims sets), verified with verifyFirebaseIdToken()
// above. The Cloudflare API token never leaves this Worker. The response is
// cached at the edge for 60 s (every admin sees the same numbers), so tapping
// Refresh repeatedly costs one Analytics API call per minute.
const CF_USAGE_DAILY_LIMIT = 100000;   // Workers Free plan: 100,000 requests/day, resets 00:00 UTC
const CF_USAGE_CACHE_SECONDS = 60;

async function fetchCfWorkersUsage(env, sinceIso, untilIso) {
  const query = `query WorkersUsage($accountTag: string!, $since: string!, $until: string!) {
    viewer {
      accounts(filter: { accountTag: $accountTag }) {
        workersInvocationsAdaptive(limit: 500, filter: { datetime_geq: $since, datetime_leq: $until }) {
          dimensions { scriptName status }
          sum { requests errors subrequests }
        }
      }
    }
  }`;
  const res = await fetch('https://api.cloudflare.com/client/v4/graphql', {
    method: 'POST',
    headers: { Authorization: `Bearer ${env.CF_API_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables: { accountTag: env.CF_ACCOUNT_ID, since: sinceIso, until: untilIso } })
  });
  const data = await res.json().catch(() => null);
  if (!res.ok || !data) throw new Error('Cloudflare Analytics API HTTP ' + res.status);
  if (data.errors && data.errors.length) throw new Error(String(data.errors[0].message || 'GraphQL error').slice(0, 200));
  const acct = (((data.data || {}).viewer || {}).accounts || [])[0];
  return (acct && acct.workersInvocationsAdaptive) || [];
}

async function handleCfUsage(request, env) {
  // Auth first, so an unauthenticated caller can't even learn whether the secrets are set.
  const m = /^Bearer\s+(.+)$/i.exec(request.headers.get('Authorization') || '');
  let payload = null;
  if (m) { try { payload = await verifyFirebaseIdToken(env, m[1]); } catch (e) { payload = null; } }
  if (!payload || payload.admin !== true) return corsJsonAdmin({ ok: false, error: 'Unauthorized' }, 401);

  if (!env.CF_API_TOKEN || !env.CF_ACCOUNT_ID) {
    return corsJsonAdmin({ ok: false, error: 'CF_API_TOKEN / CF_ACCOUNT_ID secrets are not set on this Worker' }, 500);
  }

  const cacheKey = new Request('https://cf-usage.internal/today');
  const cache = (typeof caches !== 'undefined' && caches.default) ? caches.default : null;
  if (cache) {
    const hit = await cache.match(cacheKey);
    if (hit) return corsJsonAdmin(await hit.json());
  }

  try {
    const start = new Date();
    start.setUTCHours(0, 0, 0, 0);
    const now = new Date();
    const rows = await fetchCfWorkersUsage(env, start.toISOString(), now.toISOString());

    const scripts = {};
    const total = { requests: 0, errors: 0, subrequests: 0 };
    for (const r of rows) {
      const name = (r.dimensions && r.dimensions.scriptName) || '(unknown)';
      const sc = scripts[name] || (scripts[name] = { name, requests: 0, errors: 0 });
      const reqs = (r.sum && r.sum.requests) || 0;
      const errs = (r.sum && r.sum.errors) || 0;
      sc.requests += reqs; sc.errors += errs;
      total.requests += reqs; total.errors += errs; total.subrequests += (r.sum && r.sum.subrequests) || 0;
    }
    const body = {
      ok: true,
      dayStartUtc: start.toISOString(),
      generatedAt: now.toISOString(),
      dailyLimit: CF_USAGE_DAILY_LIMIT,
      total,
      scripts: Object.values(scripts).sort((a, b) => b.requests - a.requests)
    };
    if (cache) {
      await cache.put(cacheKey, new Response(JSON.stringify(body), {
        headers: { 'Content-Type': 'application/json', 'Cache-Control': `max-age=${CF_USAGE_CACHE_SECONDS}` }
      }));
    }
    return corsJsonAdmin(body);
  } catch (e) {
    return corsJsonAdmin({ ok: false, error: String((e && e.message) || e).slice(0, 200) }, 502);
  }
}

// ---------------------------------------------------------------------------
// Firestore usage for the admin Dashboard - POST /firestore-usage
// ---------------------------------------------------------------------------
// Total document reads / writes / deletes for the WHOLE Firebase project today
// - storefront customers, this Worker's cron/webhooks and every admin device
// together - straight from Google Cloud Monitoring (the same numbers as the
// Firebase Console's Usage tab). The admin panel's own counter can only see
// what that one device sends, so it misses the Worker and the storefront.
//
// One-time setup (no new secrets, it reuses the Firebase service account):
//   1. Google Cloud Console > APIs & Services > enable "Cloud Monitoring API"
//      (same project as Firebase).
//   2. IAM > the firebase-adminsdk service account (the FIREBASE_CLIENT_EMAIL
//      one) > add role "Monitoring Viewer".
//
// Auth: same as /cf-usage - a Bearer Firebase ID token with the admin claim.
// Cached at the edge for 60 s. Cloud Monitoring itself lags a few minutes
// behind real time, so the numbers trail live activity slightly.
const FS_USAGE_CACHE_SECONDS = 60;
const MONITORING_SCOPE = 'https://www.googleapis.com/auth/monitoring.read';
const FS_DAILY_LIMITS = { reads: 50000, writes: 20000, deletes: 20000 }; // Firestore free tier, per day
let cachedMonitoringToken = null;

async function getMonitoringToken(env) {
  const now = Math.floor(Date.now() / 1000);
  if (cachedMonitoringToken && cachedMonitoringToken.expires > now + 60) return cachedMonitoringToken.token;
  cachedMonitoringToken = await mintServiceAccountToken(env, MONITORING_SCOPE);
  return cachedMonitoringToken.token;
}

// Firestore's free-tier quota resets at midnight Pacific time, so "today" for
// this card is measured from the last 00:00 America/Los_Angeles (DST-safe).
function pacificDayStartUtc(now) {
  const parts = {};
  new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles', hourCycle: 'h23',
    hour: '2-digit', minute: '2-digit', second: '2-digit'
  }).formatToParts(now).forEach(p => { parts[p.type] = parseInt(p.value, 10); });
  const sinceMidnightMs = (((parts.hour * 60) + parts.minute) * 60 + parts.second) * 1000 + now.getMilliseconds();
  return new Date(now.getTime() - sinceMidnightMs);
}

async function fetchFirestoreMetricTotal(env, metricType, startIso, endIso) {
  const token = await getMonitoringToken(env);
  const periodSec = Math.max(60, Math.ceil((Date.parse(endIso) - Date.parse(startIso)) / 1000));
  const qs = new URLSearchParams({
    filter: `metric.type="${metricType}"`,
    'interval.startTime': startIso,
    'interval.endTime': endIso,
    'aggregation.alignmentPeriod': `${periodSec}s`,
    'aggregation.perSeriesAligner': 'ALIGN_SUM',
    'aggregation.crossSeriesReducer': 'REDUCE_SUM'
  });
  const res = await fetch(
    `https://monitoring.googleapis.com/v3/projects/${env.FIREBASE_PROJECT_ID}/timeSeries?${qs}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const data = await res.json().catch(() => null);
  if (!res.ok || !data) {
    const msg = (data && data.error && data.error.message) || `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    throw err;
  }
  let total = 0;
  for (const ts of (data.timeSeries || [])) {
    for (const p of (ts.points || [])) total += Number((p.value && p.value.int64Value) || 0);
  }
  return total;
}

async function handleFirestoreUsage(request, env) {
  // Auth first, same as handleCfUsage.
  const m = /^Bearer\s+(.+)$/i.exec(request.headers.get('Authorization') || '');
  let payload = null;
  if (m) { try { payload = await verifyFirebaseIdToken(env, m[1]); } catch (e) { payload = null; } }
  if (!payload || payload.admin !== true) return corsJsonAdmin({ ok: false, error: 'Unauthorized' }, 401);

  const cacheKey = new Request('https://firestore-usage.internal/today');
  const cache = (typeof caches !== 'undefined' && caches.default) ? caches.default : null;
  if (cache) {
    const hit = await cache.match(cacheKey);
    if (hit) return corsJsonAdmin(await hit.json());
  }

  try {
    const now = new Date();
    const start = pacificDayStartUtc(now);
    const startIso = start.toISOString(), endIso = now.toISOString();
    const [reads, writes, deletes] = await Promise.all([
      fetchFirestoreMetricTotal(env, 'firestore.googleapis.com/document/read_count', startIso, endIso),
      fetchFirestoreMetricTotal(env, 'firestore.googleapis.com/document/write_count', startIso, endIso),
      fetchFirestoreMetricTotal(env, 'firestore.googleapis.com/document/delete_count', startIso, endIso)
    ]);
    const body = {
      ok: true, dayStartUtc: startIso, generatedAt: endIso,
      limits: FS_DAILY_LIMITS, reads, writes, deletes
    };
    if (cache) {
      await cache.put(cacheKey, new Response(JSON.stringify(body), {
        headers: { 'Content-Type': 'application/json', 'Cache-Control': `max-age=${FS_USAGE_CACHE_SECONDS}` }
      }));
    }
    return corsJsonAdmin(body);
  } catch (e) {
    const status = e && e.status;
    const hint = (status === 403 || status === 401)
      ? 'Cloud Monitoring refused the request - enable the "Cloud Monitoring API" and give the Firebase service account the "Monitoring Viewer" role.'
      : null;
    return corsJsonAdmin({ ok: false, error: String((e && e.message) || e).slice(0, 200), hint }, 502);
  }
}

// ---------------------------------------------------------------------------
// Admin online status (public, cached) - GET /admin-status
// ---------------------------------------------------------------------------
// The admin panel already writes { lastActive: serverTimestamp() } to
// settings/presence on a heartbeat. The storefront used to listen to that doc
// directly, so EVERY heartbeat was billed as a Firestore read per open
// storefront. Now the storefront just fetches this route instead: the Worker
// reads the doc once and the answer is cached at the edge for
// ADMIN_STATUS_CACHE_SECONDS, so N visitors cost at most ~1 read per cache
// window per Cloudflare location - and nothing listens to settings/presence
// any more, so heartbeat writes cost no reads at all.
// No KV/D1 needed. Returns only { online, ageMs } - no other admin data.
const ADMIN_STATUS_ONLINE_MS = 12 * 60 * 1000; // same threshold the storefront used before
const ADMIN_STATUS_CACHE_SECONDS = 60;

function adminStatusHeaders(cacheSeconds) {
  return {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': cacheSeconds > 0 ? `public, max-age=${cacheSeconds}` : 'no-store'
  };
}
function adminStatusJson(body, status = 200, cacheSeconds = ADMIN_STATUS_CACHE_SECONDS) {
  return new Response(JSON.stringify(body), { status, headers: adminStatusHeaders(cacheSeconds) });
}
function adminStatusPreflight() {
  return new Response(null, { status: 204, headers: adminStatusHeaders(0) });
}

async function handleAdminStatus(env) {
  const cacheKey = new Request('https://admin-status.internal/v1');
  const cache = (typeof caches !== 'undefined' && caches.default) ? caches.default : null;
  if (cache) {
    const hit = await cache.match(cacheKey);
    if (hit) {
      const cached = await hit.json();
      // Age keeps growing while the answer sits in cache, so re-derive it here.
      const ageMs = cached.lastActiveMs ? Math.max(0, Date.now() - cached.lastActiveMs) : null;
      return adminStatusJson({
        ok: true, online: ageMs !== null && ageMs < ADMIN_STATUS_ONLINE_MS, ageMs,
        lastActiveMs: cached.lastActiveMs || null
      }, 200, ADMIN_STATUS_CACHE_SECONDS);
    }
  }

  const docSnap = await getDoc(env, 'settings/presence');
  let lastActiveMs = null;
  const la = docSnap && docSnap.fields && docSnap.fields.lastActive;
  if (la) {
    const t = typeof la === 'number' ? la : new Date(la).getTime();
    if (!isNaN(t)) lastActiveMs = t;
  }
  const ageMs = lastActiveMs ? Math.max(0, Date.now() - lastActiveMs) : null;
  const body = {
    ok: true, online: ageMs !== null && ageMs < ADMIN_STATUS_ONLINE_MS, ageMs, lastActiveMs
  };
  if (cache) {
    await cache.put(cacheKey, new Response(JSON.stringify(body), {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': `max-age=${ADMIN_STATUS_CACHE_SECONDS}` }
    }));
  }
  return adminStatusJson(body, 200, ADMIN_STATUS_CACHE_SECONDS);
}


// ---------------------------------------------------------------------------
// One-time image migration: imgbb -> Cloudflare D1
// ---------------------------------------------------------------------------
// Open in a browser (needs the SWEEP_SECRET secret and a D1 binding named DB):
//   https://telegram-order.pca-game-store.workers.dev/migrate-images?secret=SWEEP_SECRET
// Optional: &c=games,paymentMethods,settings,adminUsernames   (collections to scan, in order)
//           &budget=40   (work per page load; raise it on a paid Workers plan)
// It copies every imgbb picture it finds into the D1 `images` table, then replaces the link
// inside the Firestore document (only that field, and only if nobody edited the doc meanwhile).
// The page reloads itself until everything is done. Safe to run again: finished pictures are
// remembered (img_map), failures are listed at the end and retried on the next run.
// Each scanned document costs 1 Firestore read - the default list is small on purpose. Add
// users / orders / walletTopups to &c= only if you also want those pictures moved.
const MIG_IBB_RE = /https?:\/\/(?:i\.ibb\.co(?:\.com)?|ibb\.co(?:\.com)?)\/[^\s"'<>)\]]+/g;
const MIG_MAX_BYTES = 1400 * 1024;
const MIG_DEFAULT_COLLECTIONS = 'games,paymentMethods,settings,adminUsernames';

function migWalkStrings(v, fn) {
  if (!v || typeof v !== 'object') return;
  if (typeof v.stringValue === 'string') {
    const r = fn(v.stringValue);
    if (r !== v.stringValue) v.stringValue = r;
    return;
  }
  if (v.mapValue && v.mapValue.fields) for (const k in v.mapValue.fields) migWalkStrings(v.mapValue.fields[k], fn);
  if (v.arrayValue && v.arrayValue.values) v.arrayValue.values.forEach(x => migWalkStrings(x, fn));
}

function migFindUrls(fields) {
  const set = new Set();
  for (const k in fields) {
    migWalkStrings(fields[k], (str) => { (str.match(MIG_IBB_RE) || []).forEach(u => set.add(u)); return str; });
  }
  return [...set];
}

// Rewrites links in place; returns the names of the top-level fields that changed.
function migApplyMap(fields, map) {
  const changed = [];
  for (const k in fields) {
    const before = JSON.stringify(fields[k]);
    migWalkStrings(fields[k], (str) => str.replace(MIG_IBB_RE, (u) => map.get(u) || u));
    if (JSON.stringify(fields[k]) !== before) changed.push(k);
  }
  return changed;
}

function migSniff(b) {
  if (b.length > 3 && b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff) return ['image/jpeg', 'jpg'];
  if (b.length > 8 && b[0] === 0x89 && b[1] === 0x50 && b[2] === 0x4e && b[3] === 0x47) return ['image/png', 'png'];
  if (b.length > 5 && b[0] === 0x47 && b[1] === 0x49 && b[2] === 0x46) return ['image/gif', 'gif'];
  if (b.length > 12 && b[0] === 0x52 && b[1] === 0x49 && b[2] === 0x46 && b[3] === 0x46 && b[8] === 0x57 && b[9] === 0x45 && b[10] === 0x42 && b[11] === 0x50) return ['image/webp', 'webp'];
  return null;
}

function migB64(bytes) {
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  return btoa(s);
}

function migHtml(body, refreshUrl) {
  const head = refreshUrl ? `<meta http-equiv="refresh" content="1;url=${refreshUrl.replace(/&/g, '&amp;')}">` : '';
  const esc = (x) => x; // body is built from escaped pieces below
  return new Response(`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${head}<body style="font-family:sans-serif;padding:16px;line-height:1.6">${esc(body)}</body>`,
    { headers: { 'content-type': 'text/html; charset=utf-8' } });
}
const migEsc = (x) => String(x).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

async function handleMigrateImages(env, url) {
  const q = url.searchParams;
  const given = q.get('secret') || '';
  if (!env.SWEEP_SECRET || !timingSafeEqualStr(given, env.SWEEP_SECRET)) return new Response('forbidden', { status: 403 });
  if (!env.DB) return migHtml('D1 binding <b>DB</b> မရှိသေးပါ။ telegram-order worker မှာ D1 <b>pca-images</b> ကို DB အမည်နဲ့ ချိတ်ပါ။');

  const cols = (q.get('c') || MIG_DEFAULT_COLLECTIONS).split(',').map(x => x.trim()).filter(Boolean);
  const budget = Math.max(10, Number(q.get('budget')) || 40);
  let ci = Number(q.get('i')) || 0;
  let pageToken = q.get('t') || '';
  let skip = Number(q.get('skip')) || 0;
  let ok = Number(q.get('ok')) || 0;       // pictures moved
  let bad = Number(q.get('bad')) || 0;     // pictures / documents that failed
  let dp = Number(q.get('dp')) || 0;       // documents updated
  const base = (env.IMAGE_BASE_URL || 'https://imgbb-functions.pca-game-store.workers.dev').replace(/\/$/, '');
  const db = env.DB;

  await db.batch([
    db.prepare('CREATE TABLE IF NOT EXISTS images (id TEXT PRIMARY KEY, mime TEXT NOT NULL, size INTEGER NOT NULL, data TEXT NOT NULL, created_at INTEGER NOT NULL)'),
    db.prepare('CREATE TABLE IF NOT EXISTS img_map (old_url TEXT PRIMARY KEY, new_url TEXT NOT NULL)'),
    db.prepare('CREATE TABLE IF NOT EXISTS img_fail (old_url TEXT PRIMARY KEY, reason TEXT, at INTEGER)'),
  ]);

  const at = await getAccessToken(env);
  const auth = { Authorization: `Bearer ${at}` };
  let used = 3;
  let currentToken = pageToken;
  let stoppedAt = -1;

  const here = (extra) => {
    const p = new URLSearchParams({ secret: given, c: cols.join(','), budget: String(budget), i: String(ci), t: currentToken, skip: String(extra), ok: String(ok), bad: String(bad), dp: String(dp) });
    return `${url.origin}/migrate-images?${p}`;
  };

  while (ci < cols.length) {
    const col = cols[ci];
    currentToken = pageToken;
    const listUrl = `${firestoreDocsBase(env)}/${col}?pageSize=20${pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : ''}`;
    const res = await fetch(listUrl, { headers: auth });
    used++;
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      return migHtml(`❌ <b>${migEsc(col)}</b> ကို ဖတ်မရပါ (HTTP ${res.status})<br><small>${migEsc(t.slice(0, 200))}</small><br>ဒီ collection မရှိရင် &amp;c= ထဲကနေ ဖယ်ပါ။`);
    }
    const page = await res.json();
    const docs = page.documents || [];

    let idx = skip;
    for (; idx < docs.length; idx++) {
      const doc = docs[idx];
      const fields = doc.fields || {};
      const urls = migFindUrls(fields);
      if (!urls.length) continue;

      // 1) which of these pictures are already in D1?
      const map = new Map();
      for (let i = 0; i < urls.length; i += 80) {
        const chunk = urls.slice(i, i + 80);
        const r = await db.prepare(`SELECT old_url, new_url FROM img_map WHERE old_url IN (${chunk.map(() => '?').join(',')})`).bind(...chunk).all();
        used++;
        for (const row of r.results || []) map.set(row.old_url, row.new_url);
      }
      const todo = urls.filter(u => !map.has(u));
      const cost = todo.length + (todo.length ? 1 : 0) + 1;
      if (used + cost > budget && used > 6) { stoppedAt = idx; break; }

      // 2) download the missing ones and store them in D1
      const stmts = [];
      for (let i = 0; i < todo.length; i += 4) {
        await Promise.all(todo.slice(i, i + 4).map(async (u) => {
          try {
            const r = await fetch(u);
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const bytes = new Uint8Array(await r.arrayBuffer());
            if (bytes.length > MIG_MAX_BYTES) throw new Error('too large (' + Math.round(bytes.length / 1024) + ' KB)');
            const kind = migSniff(bytes);
            if (!kind) throw new Error('not an image');
            const id = crypto.randomUUID().replace(/-/g, '');
            const newUrl = `${base}/i/${id}.${kind[1]}`;
            stmts.push(db.prepare('INSERT INTO images (id, mime, size, data, created_at) VALUES (?1, ?2, ?3, ?4, ?5)').bind(id, kind[0], bytes.length, migB64(bytes), Date.now()));
            stmts.push(db.prepare('INSERT OR REPLACE INTO img_map (old_url, new_url) VALUES (?1, ?2)').bind(u, newUrl));
            stmts.push(db.prepare('DELETE FROM img_fail WHERE old_url = ?1').bind(u));
            map.set(u, newUrl);
            ok++;
          } catch (e) {
            bad++;
            stmts.push(db.prepare('INSERT OR REPLACE INTO img_fail (old_url, reason, at) VALUES (?1, ?2, ?3)').bind(u, String(e.message || e).slice(0, 120), Date.now()));
          }
        }));
        used += Math.min(4, todo.length - i);
      }
      if (stmts.length) { await db.batch(stmts); used++; }

      // 3) point the document at the new links
      const changed = migApplyMap(fields, map);
      if (changed.length) {
        const docPath = doc.name.split('/documents/')[1];
        const mask = changed.map(f => `updateMask.fieldPaths=${encodeURIComponent(/^[A-Za-z_][A-Za-z0-9_]*$/.test(f) ? f : '`' + f.replace(/`/g, '\\`') + '`')}`).join('&');
        const body = { fields: {} };
        changed.forEach(f => { body.fields[f] = fields[f]; });
        const pr = await fetch(`${firestoreDocsBase(env)}/${docPath}?${mask}&currentDocument.updateTime=${encodeURIComponent(doc.updateTime)}`, {
          method: 'PATCH', headers: { ...auth, 'Content-Type': 'application/json' }, body: JSON.stringify(body),
        });
        used++;
        if (pr.ok) dp++;
        else {
          bad++;
          const t = await pr.text().catch(() => '');
          await db.prepare('INSERT OR REPLACE INTO img_fail (old_url, reason, at) VALUES (?1, ?2, ?3)').bind('doc:' + docPath, `PATCH ${pr.status} ${t.slice(0, 80)} (run again)`, Date.now()).run();
          used++;
        }
      }
    }

    if (stoppedAt >= 0) break;
    if (page.nextPageToken) { pageToken = page.nextPageToken; skip = 0; }
    else { ci++; pageToken = ''; skip = 0; }
    if (used >= budget) { currentToken = pageToken; stoppedAt = 0; break; }
  }

  const stats = `ပုံ ${ok} ပုံ ကူးပြီး · Firestore doc ${dp} ခု ပြင်ပြီး · မအောင်မြင် ${bad}`;
  if (ci < cols.length) {
    const col = migEsc(cols[Math.min(ci, cols.length - 1)]);
    return migHtml(`⏳ လုပ်နေဆဲ... (<b>${col}</b>)<br>${stats}<br><small>ဒီစာမျက်နှာ ကိုယ်တိုင် ဆက်သွားပါမယ်၊ မပိတ်ပါနဲ့။</small>`, here(stoppedAt > 0 ? stoppedAt : skip));
  }

  const fails = await db.prepare('SELECT old_url, reason FROM img_fail ORDER BY at DESC LIMIT 30').all();
  const list = (fails.results || []).map(r => `<li><small>${migEsc(r.old_url)}<br>→ ${migEsc(r.reason)}</small></li>`).join('');
  return migHtml(`✅ ပြီးပါပြီ<br>${stats}${list ? `<br><br>မအောင်မြင်တာတွေ (ပြန်လုပ်ရင် ထပ်စမ်းပါမယ်):<ul>${list}</ul>` : ''}`);
}
