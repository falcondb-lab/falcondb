'use strict';

const net = require('node:net');
const tls = require('node:tls');
const crypto = require('node:crypto');

const {
  MSG_CLIENT_HELLO, MSG_SERVER_HELLO, MSG_AUTH_REQUEST, MSG_AUTH_RESPONSE,
  MSG_AUTH_OK, MSG_AUTH_FAIL, MSG_QUERY_REQUEST, MSG_QUERY_RESPONSE,
  MSG_ERROR_RESPONSE, MSG_PING, MSG_PONG, MSG_DISCONNECT,
  FEATURE_BATCH_INGEST, FEATURE_PIPELINE, FEATURE_EPOCH_FENCING,
  FEATURE_TLS, FEATURE_BINARY_PARAMS,
  AUTH_PASSWORD, SESSION_AUTOCOMMIT,
  MAX_FRAME_SIZE, FRAME_HEADER_SIZE,
  WireWriter, WireReader, buildFrame, decodeRow,
} = require('./wire');

const { FalconError, AuthenticationError, makeError } = require('./errors');

/**
 * @typedef {Object} ConnectionConfig
 * @property {string}  [host='localhost']
 * @property {number}  [port=6543]
 * @property {string}  [database='falcon']
 * @property {string}  [user='falcon']
 * @property {string}  [password='']
 * @property {number}  [connectTimeout=10000]  - milliseconds
 * @property {boolean} [ssl=false]
 * @property {Object}  [sslOptions={}]         - options passed to tls.connect()
 * @property {boolean} [autocommit=true]
 */

/**
 * @typedef {Object} ColumnMeta
 * @property {string}  name
 * @property {number}  typeId
 * @property {boolean} nullable
 * @property {number}  precision
 * @property {number}  scale
 */

/**
 * @typedef {Object} QueryResult
 * @property {ColumnMeta[]} columns
 * @property {Array[]}      rows
 * @property {number}       rowsAffected
 */

class Connection {
  /**
   * @param {ConnectionConfig} config
   */
  constructor(config = {}) {
    this._host = config.host || 'localhost';
    this._port = config.port || 6543;
    this._database = config.database || 'falcon';
    this._user = config.user || 'falcon';
    this._password = config.password || '';
    this._connectTimeout = config.connectTimeout || 10000;
    this._ssl = config.ssl || false;
    this._sslOptions = config.sslOptions || {};
    this._autocommit = config.autocommit !== undefined ? config.autocommit : true;

    /** @type {net.Socket|tls.TLSSocket|null} */
    this._socket = null;
    this._closed = false;
    this._requestId = 0n;
    this._serverEpoch = 0n;
    this._serverNodeId = 0n;
    this._negotiatedFeatures = 0n;

    // Internal receive buffer — chunk list to avoid O(n²) Buffer.concat per data event
    this._recvChunks = [];
    this._recvLen = 0;
    this._recvBuf = Buffer.alloc(0);
    this._recvResolve = null;
    this._recvReject = null;
  }

  // ── Public API ─────────────────────────────────────────────────────────

  get autocommit() { return this._autocommit; }
  set autocommit(v) { this._autocommit = v; }
  get serverEpoch() { return this._serverEpoch; }
  get serverNodeId() { return this._serverNodeId; }
  get closed() { return this._closed; }

  /**
   * Connect to the FalconDB server.
   * @returns {Promise<void>}
   */
  async connect() {
    await this._tcpConnect();

    // Upgrade to TLS BEFORE sending any application data (handshake contains
    // database name, user name, nonce) so credentials are never sent in plaintext.
    if (this._ssl) {
      await this._upgradeTls();
    }

    this._setupRecv();
    await this._handshake();
    await this._authenticate();
  }

  /**
   * Execute a SQL query and return the result.
   * @param {string} sql
   * @returns {Promise<QueryResult>}
   */
  async query(sql) {
    this._checkOpen();
    const reqId = this._nextRequestId();
    const sessionFlags = this._autocommit ? SESSION_AUTOCOMMIT : 0;

    const w = new WireWriter();
    w.u64(reqId);
    w.u64(this._serverEpoch);
    w.stringU32(sql);
    w.u16(0); // no params
    w.u32(sessionFlags);

    this._sendFrame(MSG_QUERY_REQUEST, w.toBuffer());
    return this._readQueryResponse();
  }

  /**
   * Execute a SQL statement and return the number of rows affected.
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
    await this.exec('COMMIT');
  }

  /**
   * Roll back the current transaction.
   * @returns {Promise<void>}
   */
  async rollback() {
    await this.exec('ROLLBACK');
  }

  /**
   * Send a Ping and wait for Pong.
   * @param {number} [timeout=5000] - milliseconds
   * @returns {Promise<boolean>}
   */
  async ping(timeout = 5000) {
    try {
      this._checkOpen();
      this._sendFrame(MSG_PING, Buffer.alloc(0));
      const { msgType } = await this._recvFrameWithTimeout(timeout);
      return msgType === MSG_PONG;
    } catch {
      return false;
    }
  }

  /**
   * Gracefully close the connection.
   * @returns {Promise<void>}
   */
  async close() {
    if (this._closed || !this._socket) return;
    this._closed = true;
    try {
      this._sendFrame(MSG_DISCONNECT, Buffer.alloc(0));
      await this._recvFrameWithTimeout(1000).catch(() => {});
    } finally {
      this._socket.destroy();
      this._socket = null;
    }
  }

  // ── Internal: TCP connection ───────────────────────────────────────────

  /** @returns {Promise<void>} */
  _tcpConnect() {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new FalconError(`connect timeout after ${this._connectTimeout}ms`));
        if (this._socket) this._socket.destroy();
      }, this._connectTimeout);

      this._socket = net.createConnection(
        { host: this._host, port: this._port, noDelay: true },
        () => {
          clearTimeout(timer);
          resolve();
        },
      );

      this._socket.once('error', (err) => {
        clearTimeout(timer);
        reject(new FalconError(`connect error: ${err.message}`));
      });
    });
  }

  _setupRecv() {
    this._socket.on('data', (chunk) => {
      // Accumulate chunks in a list — O(1) per data event instead of O(n) concat
      this._recvChunks.push(chunk);
      this._recvLen += chunk.length;
      this._tryDeliver();
    });

    this._socket.on('error', (err) => {
      if (this._recvReject) {
        this._recvReject(new FalconError(`socket error: ${err.message}`));
        this._recvReject = null;
        this._recvResolve = null;
      }
    });

    this._socket.on('close', () => {
      if (this._recvReject) {
        this._recvReject(new FalconError('connection closed by server'));
        this._recvReject = null;
        this._recvResolve = null;
      }
    });
  }

  /** Consolidate chunk list into a single contiguous buffer when needed. */
  _consolidateRecv() {
    if (this._recvChunks.length === 0) return;
    if (this._recvBuf.length === 0) {
      this._recvBuf = this._recvChunks.length === 1
        ? this._recvChunks[0]
        : Buffer.concat(this._recvChunks, this._recvLen);
    } else {
      this._recvBuf = Buffer.concat(
        [this._recvBuf, ...this._recvChunks],
        this._recvBuf.length + this._recvLen,
      );
    }
    this._recvChunks = [];
    this._recvLen = 0;
  }

  _tryDeliver() {
    if (!this._recvResolve) return;

    // Only consolidate when we actually need to parse
    this._consolidateRecv();

    if (this._recvBuf.length < FRAME_HEADER_SIZE) return;

    const msgType = this._recvBuf.readUInt8(0);
    const length = this._recvBuf.readUInt32LE(1);

    if (length > MAX_FRAME_SIZE) {
      const reject = this._recvReject;
      this._recvResolve = null;
      this._recvReject = null;
      reject(new FalconError(`frame too large: ${length}`));
      return;
    }

    if (this._recvBuf.length < FRAME_HEADER_SIZE + length) return;

    const payload = this._recvBuf.subarray(FRAME_HEADER_SIZE, FRAME_HEADER_SIZE + length);
    this._recvBuf = this._recvBuf.subarray(FRAME_HEADER_SIZE + length);

    const resolve = this._recvResolve;
    this._recvResolve = null;
    this._recvReject = null;
    resolve({ msgType, payload: Buffer.from(payload) });
  }

  /**
   * @returns {Promise<{msgType: number, payload: Buffer}>}
   */
  _recvFrame() {
    return new Promise((resolve, reject) => {
      this._recvResolve = resolve;
      this._recvReject = reject;
      // Check if data already buffered
      this._tryDeliver();
    });
  }

  /**
   * @param {number} ms
   * @returns {Promise<{msgType: number, payload: Buffer}>}
   */
  _recvFrameWithTimeout(ms) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._recvResolve = null;
        this._recvReject = null;
        reject(new FalconError(`receive timeout after ${ms}ms`));
      }, ms);

      this._recvResolve = (result) => {
        clearTimeout(timer);
        resolve(result);
      };
      this._recvReject = (err) => {
        clearTimeout(timer);
        reject(err);
      };
      this._tryDeliver();
    });
  }

  /**
   * @param {number} msgType
   * @param {Buffer} payload
   */
  _sendFrame(msgType, payload) {
    const frame = buildFrame(msgType, payload);
    this._socket.write(frame);
  }

  // ── Internal: handshake ────────────────────────────────────────────────

  async _handshake() {
    const w = new WireWriter();
    w.u16(0); // version major
    w.u16(1); // version minor
    let features = FEATURE_BATCH_INGEST | FEATURE_PIPELINE |
                   FEATURE_EPOCH_FENCING | FEATURE_BINARY_PARAMS;
    if (this._ssl) features |= FEATURE_TLS;
    w.u64(features);
    w.stringU16('falcondb-node/0.2');
    w.stringU16(this._database);
    w.stringU16(this._user);
    w.raw(crypto.randomBytes(16)); // nonce
    w.u16(0); // num_params

    this._sendFrame(MSG_CLIENT_HELLO, w.toBuffer());

    // Read ServerHello
    const hello = await this._recvFrame();
    if (hello.msgType === MSG_ERROR_RESPONSE) {
      this._raiseError(hello.payload);
    }
    if (hello.msgType !== MSG_SERVER_HELLO) {
      throw new FalconError(`expected ServerHello, got 0x${hello.msgType.toString(16)}`);
    }

    const r = new WireReader(hello.payload);
    r.u16(); // server major
    r.u16(); // server minor
    this._negotiatedFeatures = r.u64();
    this._serverEpoch = r.u64();
    this._serverNodeId = r.u64();
    r.raw(16); // server nonce
    const numParams = r.u16();
    for (let i = 0; i < numParams; i++) {
      r.stringU16(); // key
      r.stringU16(); // value
    }

    // Read AuthRequest
    const authReq = await this._recvFrame();
    if (authReq.msgType !== MSG_AUTH_REQUEST) {
      throw new FalconError(`expected AuthRequest, got 0x${authReq.msgType.toString(16)}`);
    }
    // Consume auth method + challenge
    const ar = new WireReader(authReq.payload);
    ar.u8(); // auth method
    if (ar.remaining > 0) ar.raw(ar.remaining);
  }

  async _authenticate() {
    const w = new WireWriter();
    w.u8(AUTH_PASSWORD);
    w.raw(Buffer.from(this._password, 'utf-8'));
    this._sendFrame(MSG_AUTH_RESPONSE, w.toBuffer());

    const resp = await this._recvFrame();
    if (resp.msgType === MSG_AUTH_OK) return;
    if (resp.msgType === MSG_AUTH_FAIL) {
      let reason = '';
      if (resp.payload.length > 0) {
        const r = new WireReader(resp.payload);
        reason = r.stringU16();
      }
      throw new AuthenticationError(
        `authentication failed: ${reason}`, 4000, '28P01', 0n,
      );
    }
    throw new FalconError(`unexpected auth response: 0x${resp.msgType.toString(16)}`);
  }

  async _upgradeTls() {
    return new Promise((resolve, reject) => {
      const opts = {
        ...this._sslOptions,
        socket: this._socket,
        servername: this._sslOptions.servername || this._host,
      };
      const tlsSocket = tls.connect(opts, () => {
        this._socket = tlsSocket;
        this._setupRecv();
        resolve();
      });
      tlsSocket.once('error', reject);
    });
  }

  // ── Internal: query execution ──────────────────────────────────────────

  /**
   * @returns {Promise<QueryResult>}
   */
  async _readQueryResponse() {
    const { msgType, payload } = await this._recvFrame();

    if (msgType === MSG_ERROR_RESPONSE) {
      this._raiseError(payload);
    }

    if (msgType !== MSG_QUERY_RESPONSE) {
      throw new FalconError(`unexpected response type: 0x${msgType.toString(16)}`);
    }

    const reader = new WireReader(payload);
    reader.u64(); // request_id
    const numCols = reader.u16();

    /** @type {ColumnMeta[]} */
    const columns = [];
    const colTypes = [];
    for (let i = 0; i < numCols; i++) {
      const name = reader.stringU16();
      const typeId = reader.u8();
      const nullable = reader.u8() !== 0;
      const precision = reader.u16();
      const scale = reader.u16();
      columns.push({ name, typeId, nullable, precision, scale });
      colTypes.push(typeId);
    }

    const numRows = reader.u32();
    const rows = [];
    for (let i = 0; i < numRows; i++) {
      rows.push(decodeRow(reader, colTypes));
    }

    const rowsAffected = Number(reader.u64());
    return { columns, rows, rowsAffected };
  }

  /**
   * @param {Buffer} payload
   */
  _raiseError(payload) {
    // Minimum error frame: reqId(8) + code(4) + sqlstate(5) + retryable(1) + epoch(8) + msgLen(2) = 28
    if (payload.length < 28) {
      throw new FalconError(`malformed error response (${payload.length} bytes, need >= 28)`);
    }
    const r = new WireReader(payload);
    r.u64(); // request_id
    const errorCode = r.u32();
    const sqlstate = r.raw(5).toString('ascii');
    r.u8(); // retryable
    const serverEpoch = r.u64();
    const message = r.stringU16();
    throw makeError(message, errorCode, sqlstate, serverEpoch);
  }

  _nextRequestId() {
    this._requestId += 1n;
    return this._requestId;
  }

  _checkOpen() {
    if (this._closed || !this._socket) {
      throw new FalconError('connection is closed');
    }
  }
}

/**
 * Create and connect to a FalconDB server.
 * @param {ConnectionConfig} config
 * @returns {Promise<Connection>}
 */
async function connect(config = {}) {
  const conn = new Connection(config);
  await conn.connect();
  return conn;
}

module.exports = { Connection, connect };
