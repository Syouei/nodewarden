/**
 * ESA Adapter Layer - Type Definitions
 *
 * These types define the ESA (Alibaba Cloud Edge Serverless Application) equivalents
 * to Cloudflare's D1/R2/KV APIs. The adapter classes wrap ESA bindings to present
 * the same interface as the Cloudflare counterparts.
 */

// ---------------------------------------------------------------------------
// Database (SQLite) Types
// ---------------------------------------------------------------------------

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

export interface ESAResult {
  success: boolean;
  error?: string;
  results?: any[];
  changes?: number;
  lastInsertRowid?: number | string;
}

// ---------------------------------------------------------------------------
// Blob Storage (OSS) Types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// KV Storage (Redis) Types
// ---------------------------------------------------------------------------

export interface ESAKVNamespace {
  put(key: string, value: string, options?: { metadata?: any; expirationTtlSeconds?: number }): Promise<void>;
  get(key: string, options?: { type?: 'string' | 'arrayBuffer' | 'json' }): Promise<any>;
  getWithMetadata<T = any>(key: string, type?: 'string' | 'arrayBuffer' | 'json'): Promise<{ value: any; metadata: T | null }>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string; limit?: number }): Promise<{ keys: { name: string }[] }>;
}

// ---------------------------------------------------------------------------
// Environment Binding Types
// ---------------------------------------------------------------------------

export interface ESAEnv {
  DB: ESADatabase;
  REDIS?: ESAKVNamespace;
  OSS?: ESABucket;
  JWT_SECRET: string;
  ATTACHMENTS?: ESABucket;
  ATTACHMENTS_KV?: ESAKVNamespace;
  NOTIFICATIONS_HUB?: ESAKVNamespace;
}