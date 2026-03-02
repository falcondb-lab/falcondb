/**
 * FalconDB Node.js SDK — Type Definitions
 */

// ── Connection Config ─────────────────────────────────────────────────────

export interface ConnectionConfig {
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  connectTimeout?: number;
  ssl?: boolean;
  sslOptions?: object;
  autocommit?: boolean;
}

// ── Column Metadata ───────────────────────────────────────────────────────

export interface ColumnMeta {
  name: string;
  typeId: number;
  nullable: boolean;
  precision: number;
  scale: number;
}

// ── Query Result ──────────────────────────────────────────────────────────

export interface QueryResult {
  columns: ColumnMeta[];
  rows: any[][];
  rowsAffected: number;
}

// ── Connection ────────────────────────────────────────────────────────────

export class Connection {
  constructor(config?: ConnectionConfig);
  readonly serverEpoch: bigint;
  readonly serverNodeId: bigint;
  readonly closed: boolean;
  autocommit: boolean;
  connect(): Promise<void>;
  query(sql: string): Promise<QueryResult>;
  exec(sql: string): Promise<number>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  ping(timeout?: number): Promise<boolean>;
  close(): Promise<void>;
}

export function connect(config?: ConnectionConfig): Promise<Connection>;

// ── Connection Pool ───────────────────────────────────────────────────────

export interface PoolConfig {
  connection: ConnectionConfig;
  minSize?: number;
  maxSize?: number;
  acquireTimeout?: number;
  idleTimeout?: number;
  healthCheckInterval?: number;
}

export class ConnectionPool {
  constructor(config: PoolConfig);
  readonly size: number;
  readonly idleCount: number;
  readonly activeCount: number;
  init(): Promise<void>;
  acquire(): Promise<Connection>;
  release(conn: Connection): void;
  withConnection<T>(fn: (conn: Connection) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

// ── HA Failover ───────────────────────────────────────────────────────────

export interface SeedNode {
  host: string;
  port: number;
}

export interface FailoverConfig {
  seeds: SeedNode[];
  database?: string;
  user?: string;
  password?: string;
  maxRetries?: number;
  baseRetryDelay?: number;
  maxRetryDelay?: number;
  connectTimeout?: number;
  ssl?: boolean;
  sslOptions?: object;
  autocommit?: boolean;
}

export class FailoverConnection {
  constructor(config: FailoverConfig);
  readonly serverEpoch: bigint;
  readonly serverNodeId: bigint;
  readonly closed: boolean;
  readonly failoverCount: number;
  readonly retryCount: number;
  connect(): Promise<void>;
  query(sql: string): Promise<QueryResult>;
  exec(sql: string): Promise<number>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  ping(timeout?: number): Promise<boolean>;
  close(): Promise<void>;
}

// ── Errors ────────────────────────────────────────────────────────────────

export class FalconError extends Error {
  errorCode: number;
  sqlstate: string;
  retryable: boolean;
  serverEpoch: bigint;
  constructor(
    message: string,
    errorCode?: number,
    sqlstate?: string,
    retryable?: boolean,
    serverEpoch?: bigint,
  );
}

export class ProgrammingError extends FalconError {}
export class OperationalError extends FalconError {}
export class InternalError extends FalconError {}
export class AuthenticationError extends FalconError {}

export function isRetryable(code: number): boolean;
export function isFailoverCode(code: number): boolean;
export function makeError(
  message: string,
  errorCode: number,
  sqlstate: string,
  serverEpoch: bigint,
): FalconError;

// ── Error Codes ───────────────────────────────────────────────────────────

export const ERR_SYNTAX_ERROR: number;
export const ERR_INVALID_PARAM: number;
export const ERR_NOT_LEADER: number;
export const ERR_FENCED_EPOCH: number;
export const ERR_READ_ONLY: number;
export const ERR_SERIALIZATION_CONFLICT: number;
export const ERR_INTERNAL_ERROR: number;
export const ERR_TIMEOUT: number;
export const ERR_OVERLOADED: number;
export const ERR_AUTH_FAILED: number;
export const ERR_PERMISSION_DENIED: number;
