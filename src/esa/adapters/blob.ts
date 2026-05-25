/**
 * ESA OSS Adapter - Wraps ESA OSS to match R2Bucket API
 *
 * This adapter wraps ESA's Object Storage Service (OSS) to present the same
 * interface as Cloudflare R2, allowing the existing attachment/send file
 * handling code to work unchanged with ESA.
 */

import type { ESABucket, ESABlobObject } from '../types';

/**
 * Matches R2Object API returned by R2Bucket.get()
 */
export interface R2Object {
  body: ReadableStream | null;
  size: number;
  httpMetadata?: { contentType?: string };
  customMetadata?: Record<string, string>;
}

/**
 * Options for R2Bucket.put()
 */
export interface R2PutOptions {
  httpMetadata?: { contentType?: string };
  customMetadata?: Record<string, string>;
}

/**
 * Wrapper around ESA OSS bucket that matches R2Bucket API.
 * Converts ESA's OSS API to match R2's expected interface.
 */
export class R2LikeBucket {
  constructor(private bucket: ESABucket) {}

  /**
   * Store a value (string, ArrayBuffer, or ReadableStream) under the given key.
   * ReadableStream values are converted to ArrayBuffer before upload.
   */
  async put(key: string, value: string | ArrayBuffer | ReadableStream, options?: R2PutOptions): Promise<void> {
    let body: string | ArrayBuffer;

    if (value instanceof ReadableStream) {
      // Convert ReadableStream to ArrayBuffer for OSS
      body = await this.streamToArrayBuffer(value);
    } else {
      body = value;
    }

    return this.bucket.put(key, body, options);
  }

  /**
   * Retrieve the object stored under the given key.
   * Returns null if the key does not exist.
   */
  async get(key: string): Promise<R2Object | null> {
    const result = await this.bucket.get(key);
    if (result === null) {
      return null;
    }
    return this.mapToR2Object(result);
  }

  /**
   * Delete the object stored under the given key.
   */
  async delete(key: string): Promise<void> {
    return this.bucket.delete(key);
  }

  /**
   * List objects in the bucket matching the given options.
   */
  async list(options?: { prefix?: string }): Promise<{ objects: { key: string }[] }> {
    return this.bucket.list(options);
  }

  /**
   * Convert a ReadableStream to ArrayBuffer.
   */
  private async streamToArrayBuffer(stream: ReadableStream): Promise<ArrayBuffer> {
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) {
        chunks.push(value);
      }
    }

    const totalLength = chunks.reduce((acc, chunk) => acc + chunk.length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }

    return result.buffer;
  }

  /**
   * Map ESA blob object to R2 object format.
   */
  private mapToR2Object(blob: ESABlobObject): R2Object {
    return {
      body: blob.body,
      size: blob.size,
      httpMetadata: blob.httpMetadata,
      customMetadata: blob.customMetadata,
    };
  }
}