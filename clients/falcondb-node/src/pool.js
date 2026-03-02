'use strict';

const { Connection } = require('./connection');
const { FalconError } = require('./errors');

/**
 * @typedef {Object} PoolConfig
 * @property {import('./connection').ConnectionConfig} connection
 * @property {number} [minSize=2]
 * @property {number} [maxSize=20]
 * @property {number} [acquireTimeout=10000]  - milliseconds
 * @property {number} [idleTimeout=60000]     - milliseconds
 * @property {number} [healthCheckInterval=30000] - milliseconds
 */

class ConnectionPool {
  /**
   * @param {PoolConfig} config
   */
  constructor(config) {
    this._connConfig = config.connection || {};
    this._minSize = config.minSize || 2;
    this._maxSize = config.maxSize || 20;
    this._acquireTimeout = config.acquireTimeout || 10000;
    this._idleTimeout = config.idleTimeout || 60000;
    this._healthCheckInterval = config.healthCheckInterval || 30000;

    /** @type {Array<{conn: Connection, lastUsed: number}>} */
    this._idle = [];
    this._activeCount = 0;
    this._closed = false;
    this._healthTimer = null;

    /** @type {Array<{resolve: Function, reject: Function, timer: NodeJS.Timeout}>} */
    this._waiters = [];
  }

  /**
   * Initialize the pool with minSize connections.
   * @returns {Promise<void>}
   */
  async init() {
    const promises = [];
    for (let i = 0; i < this._minSize; i++) {
      promises.push(this._createConnection());
    }
    const conns = await Promise.all(promises);
    for (const conn of conns) {
      this._idle.push({ conn, lastUsed: Date.now() });
    }

    if (this._healthCheckInterval > 0) {
      this._healthTimer = setInterval(
        () => this._healthCheck(),
        this._healthCheckInterval,
      );
      // Allow process to exit even if timer is running
      if (this._healthTimer.unref) this._healthTimer.unref();
    }
  }

  /**
   * Acquire a connection from the pool.
   * @returns {Promise<Connection>}
   */
  async acquire() {
    if (this._closed) throw new FalconError('pool is closed');

    // Try idle connections
    while (this._idle.length > 0) {
      const entry = this._idle.pop();
      if (!entry.conn.closed) {
        this._activeCount++;
        return entry.conn;
      }
    }

    // Create new if under limit
    const total = this._activeCount + this._idle.length;
    if (total < this._maxSize) {
      const conn = await this._createConnection();
      this._activeCount++;
      return conn;
    }

    // Wait for a connection
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this._waiters.findIndex((w) => w.resolve === resolve);
        if (idx >= 0) this._waiters.splice(idx, 1);
        reject(new FalconError(`acquire timeout after ${this._acquireTimeout}ms`));
      }, this._acquireTimeout);

      this._waiters.push({ resolve, reject, timer });
    });
  }

  /**
   * Release a connection back to the pool.
   * @param {Connection} conn
   */
  release(conn) {
    if (this._closed || conn.closed) {
      this._activeCount = Math.max(0, this._activeCount - 1);
      return;
    }

    this._activeCount = Math.max(0, this._activeCount - 1);

    // Serve waiting callers first
    if (this._waiters.length > 0) {
      const waiter = this._waiters.shift();
      clearTimeout(waiter.timer);
      this._activeCount++;
      waiter.resolve(conn);
      return;
    }

    this._idle.push({ conn, lastUsed: Date.now() });
  }

  /**
   * Acquire, run fn, release. Returns fn's result.
   * @template T
   * @param {(conn: Connection) => Promise<T>} fn
   * @returns {Promise<T>}
   */
  async withConnection(fn) {
    const conn = await this.acquire();
    try {
      return await fn(conn);
    } finally {
      this.release(conn);
    }
  }

  /**
   * Close all connections and the pool.
   * @returns {Promise<void>}
   */
  async close() {
    this._closed = true;
    if (this._healthTimer) {
      clearInterval(this._healthTimer);
      this._healthTimer = null;
    }

    // Reject all waiters
    for (const waiter of this._waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(new FalconError('pool is closing'));
    }
    this._waiters = [];

    // Close idle connections
    const closePromises = this._idle.map((e) => e.conn.close().catch(() => {}));
    await Promise.all(closePromises);
    this._idle = [];
  }

  /** @returns {number} */
  get size() { return this._activeCount + this._idle.length; }

  /** @returns {number} */
  get idleCount() { return this._idle.length; }

  /** @returns {number} */
  get activeCount() { return this._activeCount; }

  // ── Internal ─────────────────────────────────────────────────────────

  async _createConnection() {
    const conn = new Connection(this._connConfig);
    await conn.connect();
    return conn;
  }

  async _healthCheck() {
    const now = Date.now();
    const toRemove = [];

    // First pass: evict stale idle connections by time (but keep minSize)
    for (let i = this._idle.length - 1; i >= 0; i--) {
      const entry = this._idle[i];
      if (this._idle.length - toRemove.length > this._minSize) {
        if (now - entry.lastUsed > this._idleTimeout) {
          toRemove.push(i);
        }
      }
    }

    // Second pass: ping remaining connections in parallel (not serial)
    const pingTargets = [];
    const removeSet = new Set(toRemove);
    for (let i = 0; i < this._idle.length; i++) {
      if (!removeSet.has(i)) {
        pingTargets.push({ idx: i, entry: this._idle[i] });
      }
    }

    const pingResults = await Promise.allSettled(
      pingTargets.map((t) => t.entry.conn.ping(3000)),
    );
    for (let j = 0; j < pingResults.length; j++) {
      const ok = pingResults[j].status === 'fulfilled' && pingResults[j].value;
      if (!ok) {
        toRemove.push(pingTargets[j].idx);
      }
    }

    // Remove in reverse order to keep indices stable
    toRemove.sort((a, b) => b - a);
    for (const idx of toRemove) {
      const [entry] = this._idle.splice(idx, 1);
      entry.conn.close().catch(() => {});
    }
  }
}

module.exports = { ConnectionPool };
