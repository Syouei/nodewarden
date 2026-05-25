# NodeWarden ESA Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port NodeWarden from Cloudflare Workers to Alibaba Cloud ESA (Edge Scripting Appliance), replacing D1/R2/KV/Durable Objects with ESA equivalents (SQLite/OSS/Redis/WebSocket background service).

**Architecture:**
The port replaces Cloudflare's edge runtime with Alibaba Cloud ESA while maintaining the Bitwarden-compatible API layer unchanged. Key changes: D1 → SQLite binding, R2/KV → OSS/Redis, Durable Objects (WebSocket hub) → ESA WebSocket + Redis pub/sub for state, scheduled triggers → ESA timer/EventBridge. The router, handlers, and services are mostly unchanged except where they directly reference Cloudflare runtime types.

**Tech Stack:** ESA (SQLite, Redis, OSS bindings), TypeScript, Node.js-compatible runtime

---

## File Structure

```
src/
├── index.ts                      # Worker entry → ESA function handler
├── types/index.ts                # Env interface (D1/KV/R2/DO → ESA bindings)
├── durable/
│   └── notifications-hub.ts     # DO pattern → Redis + WebSocket service
├── services/
│   ├── storage.ts                # D1Database → SQLite binding
│   ├── blob-store.ts             # R2/KV → OSS + Redis fallback
│   ├── auth.ts                   # JWT handling (standard crypto, portable)
│   ├── ratelimit.ts              # D1 rate limit → SQLite or Redis
│   └── backup-*.ts              # S3/WebDAV (standard fetch, portable)
└── router*.ts                    # Unchanged
    handlers/*.ts                 # Unchanged (call storage service)
```

**New files to create:**
- `esa.config.yaml` (or `esa.yml`) — ESA deployment configuration
- `src/esa/adapters/database.ts` — SQLite wrapper matching D1PreparedStatement API
- `src/esa/adapters/blob.ts` — OSS wrapper matching R2Bucket API
- `src/esa/adapters/kv.ts` — Redis KV wrapper matching KVNamespace API
- `src/esa/services/notifications.ts` — Redis pub/sub + WebSocket manager (Task 4)
- `src/esa/background.ts` — Background task queue (replaces waitUntil)

---

## Scope: What Changes vs What Stays

### Changes (must port)
1. `src/types/index.ts` — Env interface with Cloudflare bindings
2. `src/index.ts` — Worker entry point (`fetch`, `scheduled`) → ESA handler
3. `src/services/storage.ts` — D1Database usage throughout
4. `src/services/blob-store.ts` — R2/KV storage abstraction
5. `src/services/ratelimit.ts` — D1-based rate limiting
6. `src/durable/notifications-hub.ts` — Durable Object WebSocket hub → Redis + WS service
7. `wrangler.toml` → ESA deployment config
8. Database initialization pattern (per-isolate vs per-function)

### Does NOT Change
- All `src/handlers/*.ts` — call storage service, no CF types
- All `src/router*.ts` — pure request routing
- All `src/services/storage-*.ts` — call D1 via StorageService wrapper
- `src/services/auth.ts`, `backup-uploader.ts`, `totp.ts` — standard Web crypto/fetch
- `webapp/` — static frontend, unchanged

---

## Task 1: Create ESA Adapter Layer (Database, Blob, KV)

**Files:**
- Create: `src/esa/adapters/database.ts`
- Create: `src/esa/adapters/blob.ts`
- Create: `src/esa/adapters/kv.ts`
- Create: `src/esa/background.ts`
- Create: `src/esa/types.ts` — ESA-specific Env type definitions
- Create: `src/esa/services/notifications.ts` — (in Task 4)

- [ ] **Step 1: Create ESA type definitions**

```typescript
// src/esa/types.ts

// ESA SQLite binding (wraps database driver)
export interface ESADatabase {
  prepare(sql: string): ESAPreparedStatement;
  exec(sql: string): Promise<ESAResult>;
}

export interface ESAPreparedStatement {
  bind(...values: (string | number | null | undefined | ArrayBuffer | Uint8Array)[]): ESAPreparedStatement;
  first<T = any>(columns?: string): Promise<T | null>;
  run(...values: (string | number | null | undefined)[]): Promise<ESAResult>;
  all(...values: (string | number | null | undefined)[]): Promise<any[]>;
}

// ESA Blob storage (OSS wrapper)
export interface ESABucket {
  put(key: string, value: string | ArrayBuffer | ReadableStream, options?: {
    httpMetadata?: { contentType?: string };
    customMetadata?: Record<string, string>;
  }): Promise<void>;
  get(key: string): Promise<ESABlobObject | null>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string }): Promise<{ objects: { key: string }[] }>;
}

export interface ESABlobObject {
  body: ReadableStream | null;
  size: number;
  httpMetadata?: { contentType?: string };
  customMetadata?: Record<string, string>;
}

// ESA KV store (Redis wrapper)
export interface ESAKVNamespace {
  put(key: string, value: string, options?: { metadata?: any }): Promise<void>;
  get(key: string, options?: { type?: 'string' | 'arrayBuffer' | 'json' }): Promise<any>;
  getWithMetadata<T = any>(key: string, type?: 'string' | 'arrayBuffer' | 'json'): Promise<{ value: any; metadata: T | null }>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string; limit?: number }): Promise<{ keys: { name: string }[] }>;
}

// ESA Env interface (replaces Cloudflare Env)
export interface ESAEnv {
  DB: ESADatabase;
  REDIS: ESAKVNamespace;
  OSS: ESABucket;
  // WebSocket hub: no DO equivalent, use separate service or stateless approach
  JWT_SECRET: string;
  // Attachment storage preference
  ATTACHMENTS?: ESABucket;
  ATTACHMENTS_KV?: ESAKVNamespace;
}
```

- [ ] **Step 2: Create SQLite database adapter**

```typescript
// src/esa/adapters/database.ts
// Adapts ESA SQLite binding to match D1Database API surface used by StorageService

import type { ESADatabase, ESAPreparedStatement, ESAResult } from '../types';

export class D1LikeDatabase {
  constructor(private db: ESADatabase) {}

  prepare(sql: string): ESAPreparedStatement {
    return new D1LikePreparedStatement(this.db.prepare(sql));
  }

  async exec(sql: string): Promise<ESAResult> {
    return this.db.exec(sql);
  }
}

export class D1LikePreparedStatement {
  private boundValues: any[] = [];

  constructor(private stmt: ESAPreparedStatement) {}

  bind(...values: any[]): D1LikePreparedStatement {
    this.boundValues = values.map(v => v === undefined ? null : v);
    return this;
  }

  async run(...values: any[]): Promise<ESAResult> {
    const args = values.length > 0 ? values : this.boundValues;
    return this.stmt.run(...args.map(v => v === undefined ? null : v));
  }

  async first<T = any>(columns?: string): Promise<T | null> {
    const args = this.boundValues;
    return this.stmt.first<T>(columns);
  }

  async all(...values: any[]): Promise<any[]> {
    const args = values.length > 0 ? values : this.boundValues;
    return this.stmt.all(...args.map(v => v === undefined ? null : v));
  }
}
```

- [ ] **Step 3: Create OSS blob storage adapter**

```typescript
// src/esa/adapters/blob.ts
// Wraps Alibaba Cloud OSS SDK to match R2Bucket API surface

import type { ESABucket, ESABlobObject } from '../types';

const DEFAULT_CONTENT_TYPE = 'application/octet-stream';

export class R2LikeBucket {
  constructor(private bucket: ESABucket) {}

  async put(
    key: string,
    value: string | ArrayBuffer | ReadableStream,
    options?: {
      httpMetadata?: { contentType?: string };
      customMetadata?: Record<string, string>;
    }
  ): Promise<void> {
    let body: ArrayBuffer;
    if (typeof value === 'string') {
      body = new TextEncoder().encode(value).buffer;
    } else if (value instanceof ArrayBuffer) {
      body = value;
    } else {
      // ReadableStream → ArrayBuffer
      const reader = value.getReader();
      const chunks: Uint8Array[] = [];
      let result;
      while (!(result = await reader.read()).done) {
        chunks.push(result.value);
      }
      const total = chunks.reduce((sum, c) => sum + c.length, 0);
      const combined = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        combined.set(chunk, offset);
        offset += chunk.length;
      }
      body = combined.buffer;
    }

    await this.bucket.put(key, body, {
      httpMetadata: options?.httpMetadata,
      customMetadata: options?.customMetadata,
    });
  }

  async get(key: string): Promise<ESABlobObject | null> {
    const object = await this.bucket.get(key);
    if (!object) return null;

    let body: ReadableStream | null = null;
    if (object.body) {
      body = object.body as ReadableStream;
    }

    return {
      body,
      size: object.size,
      httpMetadata: object.httpMetadata,
      customMetadata: object.customMetadata,
    };
  }

  async delete(key: string): Promise<void> {
    await this.bucket.delete(key);
  }
}
```

- [ ] **Step 4: Create Redis KV adapter**

```typescript
// src/esa/adapters/kv.ts
// Wraps ESA Redis binding to match KVNamespace API

import type { ESAKVNamespace } from '../types';

export class KVLikeNamespace {
  constructor(private kv: ESAKVNamespace) {}

  async put(key: string, value: string, options?: { metadata?: any }): Promise<void> {
    const serialized = options?.metadata
      ? JSON.stringify({ value, metadata: options.metadata })
      : value;
    await this.kv.put(key, serialized, options);
  }

  async get(key: string, options?: { type?: 'string' | 'arrayBuffer' | 'json' }): Promise<any> {
    const result = await this.kv.getWithMetadata<any>(key, options?.type || 'string');
    if (!result.value) return null;
    if (options?.type === 'json') {
      return JSON.parse(result.value);
    }
    return result.value;
  }

  async getWithMetadata<T = any>(key: string, type: 'string' | 'arrayBuffer' | 'json' = 'string'): Promise<{ value: any; metadata: T | null }> {
    const result = await this.kv.getWithMetadata<T>(key, type);
    if (!result.value) return { value: null, metadata: null };
    return result;
  }

  async delete(key: string): Promise<void> {
    await this.kv.delete(key);
  }

  async list(options?: { prefix?: string; limit?: number }): Promise<{ keys: { name: string }[] }> {
    return this.kv.list(options);
  }
}
```

- [ ] **Step 5: Create background task wrapper (replaces waitUntil)**

```typescript
// src/esa/background.ts
// ESA background task execution — replaces ctx.waitUntil()

// ESA uses FC (Function Compute) background tasks. The pattern:
// 1. If ESA provides a background context, use it to schedule tasks
// 2. Otherwise, use async fire-and-forget (promise.detach for non-awaited tasks)
// 3. For guaranteed delivery, enqueue to Redis and process via separate consumer

export async function scheduleBackgroundTask(task: () => Promise<void>): void {
  // Fire-and-forget: detaching a promise allows it to continue after response sent
  // Most modern Node.js runtimes support promise cleanup via unhandled rejection tracking
  task().catch(err => console.error('Background task failed:', err));
}

// For scheduled triggers (cron), ESA invokes the handler directly.
// For waitUntil-like behavior in HTTP handlers, use promise.detach() if available,
// or simply don't await the promise before returning the response.
export function esawaitUntil(promise: Promise<void>): void {
  // Promise.detach() is available in Node.js 15+ and some edge runtimes
  // It prevents the promise from being tied to the request lifecycle
  if ('detach' in promise) {
    (promise as any).detach();
  } else {
    // Fallback: the promise continues running but may be terminated on response close
    promise.catch(err => console.error('Background task failed:', err));
  }
}
```

- [ ] **Step 6: Commit**

```bash
git add src/esa/
git commit -m "feat(esa): add ESA adapter layer types and wrappers"
```

---

## Task 2: Port Storage Service (D1 → SQLite)

**Files:**
- Modify: `src/services/storage.ts` — replace `D1Database` with ESA SQLite adapter

- [ ] **Step 1: Examine current D1 usage in storage.ts**

The `StorageService` constructor takes `private db: D1Database`. All repository files use `db.prepare(sql)` and chain `.bind().run()` or `.bind().all()` or `.bind().first()`.

- [ ] **Step 2: Create storage service ESA version**

```typescript
// In src/services/storage.ts, modify the constructor and safeBind helper

// Change constructor from:
constructor(private db: D1Database) {}

// To:
constructor(private db: ESADatabase) {}

// The D1PreparedStatement API (safeBind, bind chain) is already matched
// by the D1LikePreparedStatement adapter in Task 1.
```

Note: The D1PreparedStatement → D1LikePreparedStatement adapter handles the API surface. The only change needed in storage.ts is the import (from ESA adapter) and potentially the type annotation if needed. Check if `D1Database` is referenced elsewhere in the file — likely only in constructor.

- [ ] **Step 3: Run storage tests**

Run existing tests to verify SQLite adapter compatibility. If tests exist, run them. If not, verify by checking the D1-like API surface matches what all `storage-*.ts` repo files expect.

---

## Task 3: Port Blob Storage (R2/KV → OSS + Redis)

**Files:**
- Modify: `src/services/blob-store.ts` — replace `R2Bucket`/`KVNamespace` with ESA equivalents

- [ ] **Step 1: Analyze blob-store.ts R2/KV type guards**

The file uses `hasR2Storage(env)` and `hasKvStorage(env)` type guards. These need to be updated to use ESA adapter types.

- [ ] **Step 2: Update type guards for ESA**

```typescript
// In src/services/blob-store.ts, change:

import type { Env } from '../types';  // Keep existing
import type { ESABucket, ESAKVNamespace } from '../esa/types';

function hasR2Storage(env: Env): env is Env & { ATTACHMENTS: ESABucket } {
  return !!env.ATTACHMENTS;
}

function hasKvStorage(env: Env): env is Env & { ATTACHMENTS_KV: ESAKVNamespace } {
  return !!env.ATTACHMENTS_KV;
}
```

- [ ] **Step 3: Verify put/get/delete API compatibility**

The `putBlobObject`, `getBlobObject`, `deleteBlobObject` functions use:
- R2: `env.ATTACHMENTS.put(key, value, { httpMetadata, customMetadata })` and `env.ATTACHMENTS.get(key)`
- KV: `env.ATTACHMENTS_KV.put(key, value, { metadata })` and `env.ATTACHMENTS_KV.getWithMetadata(key, 'arrayBuffer')`

The ESA OSS adapter (R2LikeBucket) and Redis adapter (KVLikeNamespace) implement matching APIs.

---

## Task 4: Port Durable Objects → Redis + WebSocket Service

**Files:**
- Create: `src/esa/services/notifications.ts` — replaces notifications-hub.ts Durable Object pattern
- Modify: `src/durable/notifications-hub.ts` — refactor to use ESA adapter
- Modify: `src/types/index.ts` — replace DurableObjectNamespace with ESA equivalent

- [ ] **Step 1: Understand the notification architecture**

The current `NotificationsHub` Durable Object:
- Maintains WebSocket connections per user
- Broadcasts SignalR-format messages (JSON/MessagePack) to connected clients
- Has internal state: `userId → WebSocket[]` mapping via `ctx.getWebSockets()`, `ctx.acceptWebSocket()`
- Per-websocket attachment stores `userId`, `protocol`, `deviceIdentifier`
- Provides `/internal/notify` and `/internal/online` HTTP endpoints

**ESA challenge:** ESA does not have Durable Objects. WebSocket state must be managed externally (Redis) since WebSocket connections are not sticky to a single isolate.

- [ ] **Step 2: Design Redis-backed notification service**

```
┌─────────────────────────────────────────────────────┐
│  ESA Function (per-request)                        │
│  ├── NotificationsService (stateless)              │
│  │   ├── publishMessage(userId, payload) → Redis   │
│  │   ├── getOnlineDevices(userId) → Redis          │
│  │   └── broadcastToUser(userId, message) → Redis  │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│  Redis                                             │
│  ├── notifications:{userId} → pub/sub channel      │
│  ├── ws:sessions:{userId}:{deviceId} → metadata   │
│  └── ws:connections → set of active session keys   │
└─────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│  ESA WebSocket Handler (separate or same function) │
│  ├── onConnect(userId, deviceId) → Redis           │
│  ├── onMessage(ws, message) → handle handshake      │
│  ├── onClose(ws) → cleanup Redis state              │
│  └── subscribeToChannel(userId) → receive broadcasts│
└─────────────────────────────────────────────────────┘
```

- [ ] **Step 3: Create ESA notification service**

```typescript
// src/esa/services/notifications.ts

import type { ESAKVNamespace } from '../esa/types';

const SIGNALR_RECORD_SEPARATOR = 0x1e;

interface WsSessionMeta {
  userId: string;
  deviceIdentifier: string | null;
  protocol: 'json' | 'messagepack';
  handshakeComplete: boolean;
}

export class ESANotificationsService {
  constructor(
    private redis: ESAKVNamespace,
  ) {}

  // Track a new WebSocket connection in Redis
  async addConnection(userId: string, deviceIdentifier: string | null): Promise<string> {
    const sessionId = `${userId}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    const meta: WsSessionMeta = {
      userId,
      deviceIdentifier,
      protocol: 'messagepack',
      handshakeComplete: false,
    };
    await this.redis.put(`ws:session:${sessionId}`, JSON.stringify(meta));
    await this.redis.put(`ws:user:${userId}:${sessionId}`, sessionId);
    if (deviceIdentifier) {
      await this.redis.put(`ws:device:${userId}:${deviceIdentifier}`, sessionId);
    }
    return sessionId;
  }

  async removeConnection(sessionId: string): Promise<void> {
    const meta = await this.getSessionMeta(sessionId);
    if (!meta) return;
    await this.redis.delete(`ws:session:${sessionId}`);
    await this.redis.delete(`ws:user:${meta.userId}:${sessionId}`);
    if (meta.deviceIdentifier) {
      await this.redis.delete(`ws:device:${meta.userId}:${meta.deviceIdentifier}`);
    }
  }

  async getSessionMeta(sessionId: string): Promise<WsSessionMeta | null> {
    const raw = await this.redis.get(`ws:session:${sessionId}`);
    return raw ? JSON.parse(raw) : null;
  }

  async updateSessionMeta(sessionId: string, update: Partial<WsSessionMeta>): Promise<void> {
    const meta = await this.getSessionMeta(sessionId);
    if (!meta) return;
    Object.assign(meta, update);
    await this.redis.put(`ws:session:${sessionId}`, JSON.stringify(meta));
  }

  async getOnlineDeviceIdentifiers(userId: string): Promise<string[]> {
    const prefix = `ws:device:${userId}:`;
    const result = await this.redis.list({ prefix });
    return result.keys.map(k => k.name.replace(prefix, ''));
  }

  async publishNotification(
    userId: string,
    message: string | Uint8Array,
    targetDeviceIdentifier?: string | null
  ): Promise<void> {
    // Redis pub/sub: publish to user's channel
    const channel = `notifications:${userId}`;
    const payload = JSON.stringify({ message, targetDeviceIdentifier });
    await this.redis.put(`notifications:pending:${userId}:${Date.now()}`, payload);
  }
}

// NotificationHub becomes a thin adapter that delegates to ESA service
// The original notifications-hub.ts logic (SignalR encoding, etc.) stays,
// but the WebSocket state management moves to Redis.
```

- [ ] **Step 4: Update Env interface to remove Cloudflare types**

```typescript
// In src/types/index.ts, change Env from:
export interface Env {
  DB: D1Database;
  NOTIFICATIONS_HUB: DurableObjectNamespace;
  ASSETS?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>; };
  ATTACHMENTS?: R2Bucket;
  ATTACHMENTS_KV?: KVNamespace;
  JWT_SECRET: string;
}

// To:
export interface Env {
  DB: ESADatabase;  // SQLite via ESA adapter
  NOTIFICATIONS_HUB?: ESAKVNamespace;  // Redis-backed, optional
  ASSETS?: { fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>; };
  ATTACHMENTS?: ESABucket;  // OSS via ESA adapter
  ATTACHMENTS_KV?: ESAKVNamespace;  // Redis via ESA adapter
  JWT_SECRET: string;
}
```

Note: `D1Database`, `R2Bucket`, `KVNamespace`, `DurableObjectNamespace`, `ExecutionContext`, `ScheduledController` are all imported from `@cloudflare/workers-types`. All references to these types must be removed or replaced.

- [ ] **Step 5: Commit**

```bash
git add src/services/blob-store.ts src/durable/ src/types/index.ts src/esa/
git commit -m "feat(esa): port blob storage and notifications to ESA adapters"
```

---

## Task 5: Port Worker Entry Point (index.ts)

**Files:**
- Modify: `src/index.ts` — replace Cloudflare Worker export with ESA handler

- [ ] **Step 1: Analyze current index.ts structure**

```typescript
// Current exports (Cloudflare Workers):
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> { ... },
  async scheduled(controller: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> { ... },
};
export { NotificationsHub };

// ESA uses standard function exports:
export default {
  fetch(request: Request, env: Env, ctx: ESAExecutionContext): Promise<Response> | Response { ... },
};
// For scheduled/timer: either use ESA timer trigger or separate function
```

- [ ] **Step 2: Create ESA handler**

```typescript
// src/index.ts — ESA version

import { handleRequest } from './router';
import { StorageService } from './services/storage';
import { runScheduledBackupIfDue } from './handlers/backup';
import { ESANotificationsService } from './esa/services/notifications';
import type { ESAEnv } from './esa/types';

// Import ESA adapters
import { D1LikeDatabase } from './esa/adapters/database';
import { R2LikeBucket } from './esa/adapters/blob';
import { KVLikeNamespace } from './esa/adapters/kv';

let dbInitialized = false;
let dbInitError: string | null = null;
let dbInitPromise: Promise<void> | null = null;

function normalizeRequestUrl(request: Request): Request {
  const url = new URL(request.url);
  const normalizedPathname = url.pathname.length <= 1 ? url.pathname : url.pathname.replace(/\/+$/, '');
  if (normalizedPathname === url.pathname) return request;
  url.pathname = normalizedPathname;
  return new Request(url.toString(), request);
}

function isWorkerHandledPath(path: string): boolean {
  return (
    path.startsWith('/api/') ||
    path.startsWith('/identity/') ||
    path.startsWith('/icons/') ||
    path.startsWith('/notifications/') ||
    path.startsWith('/.well-known/') ||
    path === '/config' ||
    path === '/api/config' ||
    path === '/api/version'
  );
}

function adaptEnv(rawEnv: any): ESAEnv {
  return {
    DB: new D1LikeDatabase(rawEnv.DB),
    NOTIFICATIONS_HUB: rawEnv.NOTIFICATIONS_HUB ? new KVLikeNamespace(rawEnv.NOTIFICATIONS_HUB) : undefined,
    ASSETS: rawEnv.ASSETS,
    ATTACHMENTS: rawEnv.ATTACHMENTS ? new R2LikeBucket(rawEnv.ATTACHMENTS) : undefined,
    ATTACHMENTS_KV: rawEnv.ATTACHMENTS_KV ? new KVLikeNamespace(rawEnv.ATTACHMENTS_KV) : undefined,
    JWT_SECRET: rawEnv.JWT_SECRET,
  };
}

async function ensureDatabaseInitialized(env: ESAEnv): Promise<void> {
  if (dbInitialized) return;
  if (!dbInitPromise) {
    dbInitPromise = (async () => {
      const storage = new StorageService(env.DB);
      await storage.initializeDatabase();
      dbInitialized = true;
      dbInitError = null;
    })()
      .catch((error: unknown) => {
        console.error('Failed to initialize database:', error);
        dbInitError = error instanceof Error ? error.message : 'Unknown database initialization error';
      })
      .finally(() => {
        dbInitPromise = null;
      });
  }
  await dbInitPromise;
}

// ESA default export (HTTP handler)
export default {
  async fetch(request: Request, env: any, context: any): Promise<Response> {
    const adaptedEnv = adaptEnv(env);
    const normalizedRequest = normalizeRequestUrl(request);

    // Serve static assets if ASSETS binding exists
    if (adaptedEnv.ASSETS && request.method === 'GET') {
      const url = new URL(request.url);
      if (!isWorkerHandledPath(url.pathname)) {
        const response = await adaptedEnv.ASSETS.fetch(request);
        // Handle SPA routing (not_found_handling: single-page-application)
        if (response.status === 404) {
          const indexResponse = await adaptedEnv.ASSETS.fetch(
            new Request(new URL('/index.html', request.url).toString(), request)
          );
          if (indexResponse.ok) {
            return indexResponse;
          }
        }
        return response;
      }
    }

    await ensureDatabaseInitialized(adaptedEnv);
    if (dbInitError) {
      console.error('DB init error:', dbInitError);
      return new Response(JSON.stringify({
        error: 'Database not initialized',
        error_description: 'Database initialization failed. Check server logs.',
      }), { status: 500, headers: { 'Content-Type': 'application/json' } });
    }

    const resp = await handleRequest(normalizedRequest, adaptedEnv);
    return applyCors(normalizedRequest, resp);
  }
};

// ESA Timer/Scheduled handler (separate export or separate function)
// If ESA supports multiple handlers in one file:
export async function scheduled(env: any, timer: any): Promise<void> {
  const adaptedEnv = adaptEnv(env);
  await ensureDatabaseInitialized(adaptedEnv);
  if (dbInitError) {
    console.error('Skipping scheduled backup because DB init failed:', dbInitError);
    return;
  }
  // ESA background: schedule as async task
  runScheduledBackupIfDue(adaptedEnv).catch((error) => {
    console.error('Scheduled backup failed:', error);
  });
}
```

- [ ] **Step 3: Remove Cloudflare-specific imports from index.ts**

Remove: `NotificationsHub` import (no longer exported from index), check for any other CF imports.

- [ ] **Step 4: Commit**

```bash
git add src/index.ts
git commit -m "feat(esa): port worker entry point to ESA handler"
```

---

## Task 6: Port Rate Limiting Service (D1 → SQLite/Redis)

**Files:**
- Modify: `src/services/ratelimit.ts`

- [ ] **Step 1: Examine current rate limiting pattern**

Current `RateLimitService` uses D1 to store rate limit counters. The pattern should work with SQLite with minimal changes — just use ESA SQLite adapter.

- [ ] **Step 2: Verify API compatibility**

Check if `ratelimit.ts` uses `env.DB` directly or through `StorageService`. Likely it instantiates `D1Database` directly.

```typescript
// Current (likely):
constructor(private db: D1Database) {}

// After: constructor(private db: ESADatabase) {}
```

No changes to query logic — D1PreparedStatement API is matched by D1LikePreparedStatement.

---

## Task 7: Create ESA Deployment Configuration

**Files:**
- Create: `esa.yml` (or `esa.config.yaml`) — ESA deployment config (replaces wrangler.toml)
- Create: `.dev.vars.example` — environment variable template

- [ ] **Step 1: Create ESA configuration based on wrangler.toml equivalents**

```yaml
# esa.yml — ESA deployment configuration
# Replace wrangler.toml configuration

name: nodewarden
runtime: nodejs20  # or ESA's node runtime identifier

# Database binding (SQLite via ESA)
databases:
  - name: nodewarden-db
    type: sqlite
    binding: DB

# Redis binding (KV for attachments fallback, notifications state)
kv:
  - name: nodewarden-kv
    binding: NOTIFICATIONS_HUB
    type: redis

# OSS bucket (attachments/Send file storage)
oss:
  - name: nodewarden-attachments
    binding: ATTACHMENTS

# Static assets (ESA edge hosting for webapp dist)
static:
  - path: ./dist
    binding: ASSETS
    spa: true  # single-page-application routing

# Timer trigger (replaces Cloudflare cron)
triggers:
  - type: timer
    cron: "*/5 * * * *"
    handler: scheduled

# Environment variables
env:
  JWT_SECRET:
    type: secret
    description: HMAC signing key (min 32 chars)
```

- [ ] **Step 2: Commit**

```bash
git add esa.yml .dev.vars.example
git commit -m "feat(esa): add ESA deployment configuration"
```

---

## Task 8: Database Schema Compatibility

**Files:**
- Verify: `migrations/0001_init.sql` — SQLite compatibility

- [ ] **Step 1: Check schema for Cloudflare-specific features**

ESA SQLite is standard SQLite. The D1 schema in `migrations/0001_init.sql` should work directly, but verify:
- No Cloudflare-specific SQL features (D1 may have extensions)
- No `STRICT` tables if ESA SQLite doesn't support them
- Foreign keys work as expected

- [ ] **Step 2: Test schema on ESA SQLite**

If schema is standard SQLite, no changes needed. If D1-specific, adapt accordingly.

---

## Task 9: Build and Verify

**Files:**
- Modify: `package.json` — ESA build target or separate build script
- Modify: `tsconfig.json` — may need to change from `@cloudflare/workers-types` to ESA types

- [ ] **Step 1: Update TypeScript configuration**

```json
// tsconfig.json changes:
// REMOVE: "@cloudflare/workers-types"
// ADD: ESA type definitions if available, or use standard lib

{
  "compilerOptions": {
    "lib": ["ES2022", "DOM"],
    "types": [],  // Remove @cloudflare/workers-types
    // Add ESA types when available
  }
}
```

- [ ] **Step 2: Verify no remaining Cloudflare imports**

```bash
grep -r "cloudflare" src/ --include="*.ts"
# Should return no results
grep -r "@cloudflare" src/ --include="*.ts"
# Should return no results
grep -r "D1Database\|D1PreparedStatement\|D1Result" src/ --include="*.ts"
# Should only show in ESA adapter files
```

- [ ] **Step 3: Build and test**

Run the build process to verify compilation. Deploy to ESA and test:
- `/api/version` endpoint
- Registration and login flow
- Cipher CRUD
- Attachment upload/download
- Send file creation
- Backup functionality

---

## Task 10: WebSocket Notification Porting

**Files:**
- Modify: `src/durable/notifications-hub.ts` — remove DurableObject inheritance, use ESA adapter

- [ ] **Step 1: Determine ESA WebSocket capability**

ESA supports WebSocket via its HTTP handler upgrade mechanism (similar to Node.js `ws` module or native `WebSocketUpgrade` handling). The pattern:
- HTTP handler receives request with `Upgrade: websocket` header
- Return response with `status: 101` and the WebSocket object attached

```typescript
// In ESA HTTP handler:
if (request.headers.get('Upgrade')?.toLowerCase() === 'websocket') {
  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];

  // Track connection in Redis via ESANotificationsService
  const sessionId = await notificationsService.addConnection(userId, deviceId);

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}
```

- [ ] **Step 2: Restructure NotificationsHub to be stateless**

Remove all `this.ctx.*` Cloudflare DO calls and delegate to ESA notification service:
- Replace `ctx.acceptWebSocket(server, tags)` → Redis + in-memory tracking
- Replace `ctx.getWebSockets(tag)` → Redis set lookup
- Replace `ctx.setWebSocketAutoResponse()` → handle ping/pong in message handler
- Replace `waitUntil()` → `esawaitUntil()` from background.ts

```typescript
// notifications-hub.ts (ESA version) — conceptual structure
export class NotificationsHub {
  constructor(
    private redis: ESAKVNamespace,
    private wsManager: ESAWebSocketManager,
  ) {}

  async fetch(request: Request): Promise<Response> {
    // Handle /notifications/hub WebSocket upgrade
    // Handle /internal/notify HTTP endpoint
    // Handle /internal/online HTTP endpoint
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    // SignalR handshake → store protocol in Redis
    // Echo non-handshake messages to client
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string): Promise<void> {
    // Remove session from Redis
    await this.redis.delete(`ws:session:${sessionId}`);
    this.broadcastDeviceStatus(userId);
  }
}
```

- [ ] **Step 3: Fallback if ESA has no WebSocket support**

If ESA cannot handle WebSocket upgrades, implement SSE (Server-Sent Events) as fallback:
- Client connects to `/notifications/stream` endpoint
- Server maintains open connection and pushes SignalR-format messages
- Use Redis pub/sub to deliver messages to SSE connection from any function instance

Note: This is a degraded mode — Bitwarden clients expect WebSocket for real-time sync. SSE is functional but may have higher latency.

---

## Key Risks and Unknowns

| Risk | Impact | Mitigation |
|------|--------|------------|
| ESA does not support WebSocket at edge | High | Fall back to SSE or polling; deprecate real-time sync |
| ESA SQLite has different-per-request isolation model | Medium | D1 had per-isolate schema caching; ESA may need different pattern |
| ESA background tasks (`waitUntil` equivalent) | Medium | Use ESA's async/background queue API or run tasks inline |
| ESA doesn't have Durable Object equivalent | High | Redis pub/sub for notifications; acceptable tradeoff |
| R2→OSS API differences (e.g., multipart, streaming) | Medium | Adapter may need streaming adaptation |
| KV→Redis TTL/expiry differences | Low | Redis handles TTL natively |

---

## Execution Handoff

After saving the plan, offer execution choice:

**"Plan complete and saved to `docs/superpowers/plans/2026-05-25-nodewarden-esa-port.md`. Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?"**

**If Subagent-Driven chosen:**
- **REQUIRED SUB-SKILL:** Use superpowers:subagent-driven-development
- Fresh subagent per task + two-stage review

**If Inline Execution chosen:**
- **REQUIRED SUB-SKILL:** Use superpowers:executing-plans
- Batch execution with checkpoints for review
