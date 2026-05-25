/**
 * ESA Redis Adapter - Wraps ESA Redis/KV to match KVNamespace API
 *
 * This adapter wraps ESA's KV (Redis) namespace to present the same interface
 * as Cloudflare KV, allowing the existing code to work unchanged with ESA.
 *
 * For metadata storage, values are serialized as JSON with metadata embedded
 * to match the pattern used with D1 metadata storage.
 */

import type { ESAKVNamespace } from '../types';

type KVGetType = 'string' | 'arrayBuffer' | 'json';

/**
 * Options for KVNamespace.put()
 */
export interface KVPutOptions {
  metadata?: any;
  expirationTtlSeconds?: number;
}

/**
 * Wrapper around ESA KV/Redis namespace that matches KVNamespace API.
 * Converts ESA's KV API to match Cloudflare KV's expected interface.
 */
export class KVLikeNamespace {
  constructor(private kv: ESAKVNamespace) {}

  /**
   * Store a value under the given key.
   * When metadata is provided, the value and metadata are stored together as JSON
   * to match the D1 metadata pattern used elsewhere in the codebase.
   */
  async put(key: string, value: string, options?: KVPutOptions): Promise<void> {
    if (options?.metadata !== undefined) {
      // Serialize value + metadata as JSON to match D1 metadata pattern
      const serialized = JSON.stringify({ v: value, m: options.metadata });
      return this.kv.put(key, serialized, {
        expirationTtlSeconds: options.expirationTtlSeconds,
      });
    }
    return this.kv.put(key, value, {
      expirationTtlSeconds: options?.expirationTtlSeconds,
    });
  }

  /**
   * Retrieve the value stored under the given key.
   * When the value was stored with metadata, extracts and returns the inner value.
   */
  async get(key: string, options?: { type?: KVGetType }): Promise<any> {
    const type = options?.type ?? 'string';
    const result = await this.kv.get(key, { type });

    if (result === null) {
      return null;
    }

    // Check if this was stored with metadata (JSON envelope)
    if (type === 'string' && typeof result === 'string') {
      try {
        const parsed = JSON.parse(result);
        if (parsed && typeof parsed === 'object' && 'v' in parsed) {
          return parsed.v;
        }
      } catch {
        // Not JSON envelope, return as-is
      }
    }

    return result;
  }

  /**
   * Retrieve the value and metadata stored under the given key.
   * Returns { value, metadata } where metadata is null if not present.
   */
  async getWithMetadata<T = any>(key: string, type: KVGetType = 'string'): Promise<{ value: any; metadata: T | null }> {
    const result = await this.kv.getWithMetadata<T>(key, type);

    if (result.value === null) {
      return { value: null, metadata: null };
    }

    // Check if this was stored with metadata (JSON envelope)
    if (type === 'string' && typeof result.value === 'string') {
      try {
        const parsed = JSON.parse(result.value);
        if (parsed && typeof parsed === 'object' && 'v' in parsed) {
          return {
            value: parsed.v,
            metadata: parsed.m ?? null,
          };
        }
      } catch {
        // Not JSON envelope, return as-is with null metadata
      }
    }

    return { value: result.value, metadata: result.metadata };
  }

  /**
   * Delete the value stored under the given key.
   */
  async delete(key: string): Promise<void> {
    return this.kv.delete(key);
  }

  /**
   * List keys in the namespace matching the given options.
   */
  async list(options?: { prefix?: string; limit?: number }): Promise<{ keys: { name: string }[] }> {
    return this.kv.list(options);
  }
}