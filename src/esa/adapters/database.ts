/**
 * ESA SQLite Adapter - Wraps ESA SQLite to match D1Database API
 *
 * This adapter wraps ESA's SQLite binding to present the same interface as
 * Cloudflare D1, allowing the StorageService to work unchanged with ESA.
 */

import type { ESADatabase, ESAPreparedStatement, ESAResult } from '../types';

type BindValue = string | number | null | undefined | ArrayBuffer | Uint8Array;

/**
 * Represents a prepared statement that can be bound with values and executed.
 * Matches D1PreparedStatement API.
 */
export class D1LikePreparedStatement {
  private boundValues: BindValue[] = [];

  constructor(
    private db: ESADatabase,
    private sql: string
  ) {}

  /**
   * Bind values to the statement. Returns the same statement for chaining.
   * Converts undefined to null to match D1 behavior.
   */
  bind(...values: BindValue[]): D1LikePreparedStatement {
    this.boundValues = values.map(v => v === undefined ? null : v);
    return this;
  }

  /**
   * Execute the statement and return the first result row.
   */
  async first<T = any>(columns?: string): Promise<T | null> {
    const results = await this.all();
    if (results.length === 0) {
      return null;
    }
    const row = results[0];
    if (!columns) {
      return row as T;
    }
    // If columns is specified, return only those fields
    const result: any = {};
    const fields = columns.split(',').map(f => f.trim());
    for (const field of fields) {
      if (field in row) {
        result[field] = row[field];
      }
    }
    return result as T;
  }

  /**
   * Execute the statement (INSERT/UPDATE/DELETE) and return result metadata.
   */
  async run(...values: (string | number | null | undefined)[]): Promise<ESAResult> {
    if (values.length > 0) {
      this.bind(...values);
    }
    const stmt = this.db.prepare(this.sql);
    // ESA prepare returns a new statement, so we need to rebind
    const esaStmt = this.db.prepare(this.sql);
    const bound = esaStmt.bind(...this.boundValues);
    return bound.run();
  }

  /**
   * Execute the statement (SELECT) and return all result rows.
   */
  async all(...values: (string | number | null | undefined)[]): Promise<any[]> {
    if (values.length > 0) {
      this.bind(...values);
    }
    const stmt = this.db.prepare(this.sql);
    const esaStmt = this.db.prepare(this.sql);
    const bound = esaStmt.bind(...this.boundValues);
    return bound.all();
  }
}

/**
 * Wrapper around ESA SQLite database that matches D1Database API.
 * Allows StorageService to work unchanged with ESA SQLite bindings.
 */
export class D1LikeDatabase {
  constructor(private db: ESADatabase) {}

  /**
   * Prepare a SQL statement for execution.
   */
  prepare(sql: string): D1LikePreparedStatement {
    return new D1LikePreparedStatement(this.db, sql);
  }

  /**
   * Execute raw SQL directly (for DDL statements, etc.).
   */
  async exec(sql: string): Promise<ESAResult> {
    return this.db.exec(sql);
  }

  /**
   * Helper to bind values safely, converting undefined to null.
   * Mirrors the safeBind pattern used in StorageService.
   */
  safeBind(values: any[]): any[] {
    return values.map(v => v === undefined ? null : v);
  }
}