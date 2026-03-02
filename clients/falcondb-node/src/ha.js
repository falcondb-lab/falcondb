'use strict';

const { Connection } = require('./connection');
const { FalconError, isFailoverCode, isRetryable } = require('./errors');

/**
 * @typedef {Object} SeedNode
 * @property {string} host
 * @property {number} port
 */

/**
 * @typedef {Object} FailoverConfig
 * @property {SeedNode[]}  seeds
 * @property {string}      [database='falcon']
 * @property {string}      [user='falcon']
 * @property {string}      [password='']
 * @property {number}      [maxRetries=3]
 * @property {number}      [baseRetryDelay=500]   - milliseconds
 * @property {number}      [maxRetryDelay=10000]  - milliseconds
 * @property {number}      [connectTimeout=10000] - milliseconds
 * @property {boolean}     [ssl=false]
 * @property {Object}      [sslOptions={}]
 * @property {boolean}     [autocommit=true]
 */

class FailoverConnection {
  /**
   * @param {FailoverConfig} config
   */
  constructor(config) {
    if (!config.seeds || config.seeds.length === 0) {
      throw new FalconError('at least one seed node is required');
    }
    this._seeds = config.seeds;
    this._database = config.database || 'falcon';
    this._user = config.user || 'falcon';
    this._password = config.password || '';
    this._maxRetries = config.maxRetries || 3;
    this._baseRetryDelay = config.baseRetryDelay || 500;
    this._maxRetryDelay = config.maxRetryDelay || 10000;
    this._connectTimeout = config.connectTimeout || 10000;
    this._ssl = config.ssl || false;
    this._sslOptions = config.sslOptions || {};
    this._autocommit = config.autocommit !== undefined ? config.autocommit : true;

    /** @type {Connection|null} */
    this._conn = null;
    this._currentSeedIdx = 0;
    this._closed = false;

    // Metrics
    this._failoverCount = 0;
    this._retryCount = 0;
  }

  /**
   * Connect to the first available seed node.
   * @returns {Promise<void>}
   */
  async connect() {
    await this._connectToAnySeed();
  }

  /**
   * Execute a query with automatic failover and retry.
   * @param {string} sql
   * @returns {Promise<import('./connection').QueryResult>}
   */
  async query(sql) {
    return this._executeWithRetry(() => {
      this._checkOpen();
      return this._conn.query(sql);
    });
  }

  /**
   * Execute a statement with automatic failover and retry.
   * @param {string} sql
   * @returns {Promise<number>}
   */
  async exec(sql) {
    const result = await this.query(sql);
    return result.rowsAffected;
  }

  /**
   * Commit the current transaction.
   * @returns {Promise<void>}
   */
  async commit() {
    this._checkOpen();
    await this._conn.commit();
  }

  /**
   * Roll back the current transaction.
   * @returns {Promise<void>}
   */
  async rollback() {
    this._checkOpen();
    await this._conn.rollback();
  }

  /**
   * Ping the current server.
   * @param {number} [timeout=5000]
   * @returns {Promise<boolean>}
   */
  async ping(timeout = 5000) {
    if (!this._conn || this._conn.closed) return false;
    return this._conn.ping(timeout);
  }

  /**
   * Close the connection.
   * @returns {Promise<void>}
   */
  async close() {
    this._closed = true;
    if (this._conn) {
      await this._conn.close().catch(() => {});
      this._conn = null;
    }
  }

  get serverEpoch() { return this._conn ? this._conn.serverEpoch : 0n; }
  get serverNodeId() { return this._conn ? this._conn.serverNodeId : 0n; }
  get closed() { return this._closed; }
  get failoverCount() { return this._failoverCount; }
  get retryCount() { return this._retryCount; }

  // ── Internal ─────────────────────────────────────────────────────────

  /**
   * @template T
   * @param {() => Promise<T>} fn
   * @returns {Promise<T>}
   */
  async _executeWithRetry(fn) {
    let lastErr = null;
    for (let attempt = 0; attempt <= this._maxRetries; attempt++) {
      try {
        // Ensure we have a connection
        if (!this._conn || this._conn.closed) {
          await this._connectToAnySeed();
        }
        return await fn();
      } catch (err) {
        lastErr = err;

        if (!(err instanceof FalconError)) throw err;

        if (isFailoverCode(err.errorCode)) {
          // Need to switch to a different node
          this._failoverCount++;
          this._retryCount++;
          if (this._conn) {
            await this._conn.close().catch(() => {});
            this._conn = null;
          }
          this._currentSeedIdx = (this._currentSeedIdx + 1) % this._seeds.length;
          const delay = Math.min(
            this._baseRetryDelay * (2 ** attempt),
            this._maxRetryDelay,
          );
          await this._sleep(delay);
          continue;
        }

        if (isRetryable(err.errorCode)) {
          // Retry on same node
          this._retryCount++;
          const delay = Math.min(
            this._baseRetryDelay * (2 ** attempt),
            this._maxRetryDelay,
          );
          await this._sleep(delay);
          continue;
        }

        // Non-retryable error
        throw err;
      }
    }
    throw lastErr || new FalconError('max retries exhausted');
  }

  async _connectToAnySeed() {
    let lastErr = null;
    for (let i = 0; i < this._seeds.length; i++) {
      const idx = (this._currentSeedIdx + i) % this._seeds.length;
      const seed = this._seeds[idx];
      try {
        const conn = new Connection({
          host: seed.host,
          port: seed.port,
          database: this._database,
          user: this._user,
          password: this._password,
          connectTimeout: this._connectTimeout,
          ssl: this._ssl,
          sslOptions: this._sslOptions,
          autocommit: this._autocommit,
        });
        await conn.connect();
        this._conn = conn;
        this._currentSeedIdx = idx;
        return;
      } catch (err) {
        lastErr = err;
      }
    }
    throw lastErr || new FalconError('all seed nodes exhausted');
  }

  _checkOpen() {
    if (this._closed) throw new FalconError('connection is closed');
  }

  /** @param {number} ms */
  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

module.exports = { FailoverConnection };
