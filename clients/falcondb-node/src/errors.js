'use strict';

// ── Error codes ───────────────────────────────────────────────────────────
const ERR_SYNTAX_ERROR           = 1000;
const ERR_INVALID_PARAM          = 1001;
const ERR_NOT_LEADER             = 2000;
const ERR_FENCED_EPOCH           = 2001;
const ERR_READ_ONLY              = 2002;
const ERR_SERIALIZATION_CONFLICT = 2003;
const ERR_INTERNAL_ERROR         = 3000;
const ERR_TIMEOUT                = 3001;
const ERR_OVERLOADED             = 3002;
const ERR_AUTH_FAILED            = 4000;
const ERR_PERMISSION_DENIED      = 4001;

/**
 * Base error class for all FalconDB errors.
 */
class FalconError extends Error {
  /**
   * @param {string} message
   * @param {number} [errorCode=0]
   * @param {string} [sqlstate='']
   * @param {boolean} [retryable=false]
   * @param {bigint} [serverEpoch=0n]
   */
  constructor(message, errorCode = 0, sqlstate = '', retryable = false, serverEpoch = 0n) {
    super(message);
    this.name = 'FalconError';
    this.errorCode = errorCode;
    this.sqlstate = sqlstate;
    this.retryable = retryable;
    this.serverEpoch = serverEpoch;
  }
}

class ProgrammingError extends FalconError {
  constructor(message, errorCode, sqlstate, serverEpoch) {
    super(message, errorCode, sqlstate, false, serverEpoch);
    this.name = 'ProgrammingError';
  }
}

class OperationalError extends FalconError {
  constructor(message, errorCode, sqlstate, retryable, serverEpoch) {
    super(message, errorCode, sqlstate, retryable, serverEpoch);
    this.name = 'OperationalError';
  }
}

class InternalError extends FalconError {
  constructor(message, errorCode, sqlstate, retryable, serverEpoch) {
    super(message, errorCode, sqlstate, retryable, serverEpoch);
    this.name = 'InternalError';
  }
}

class AuthenticationError extends FalconError {
  constructor(message, errorCode, sqlstate, serverEpoch) {
    super(message, errorCode, sqlstate, false, serverEpoch);
    this.name = 'AuthenticationError';
  }
}

/**
 * Returns true if the error code indicates the client should retry.
 * @param {number} code
 * @returns {boolean}
 */
function isRetryable(code) {
  return [
    ERR_NOT_LEADER, ERR_FENCED_EPOCH, ERR_READ_ONLY,
    ERR_SERIALIZATION_CONFLICT, ERR_TIMEOUT, ERR_OVERLOADED,
  ].includes(code);
}

/**
 * Returns true if the error requires connecting to a different node.
 * @param {number} code
 * @returns {boolean}
 */
function isFailoverCode(code) {
  return [ERR_NOT_LEADER, ERR_FENCED_EPOCH, ERR_READ_ONLY].includes(code);
}

/**
 * Create the appropriate error subclass based on the error code.
 * @param {string} message
 * @param {number} errorCode
 * @param {string} sqlstate
 * @param {bigint} serverEpoch
 * @returns {FalconError}
 */
function makeError(message, errorCode, sqlstate, serverEpoch) {
  if (errorCode >= 1000 && errorCode < 2000) {
    return new ProgrammingError(message, errorCode, sqlstate, serverEpoch);
  }
  if (errorCode >= 2000 && errorCode < 3000) {
    return new OperationalError(message, errorCode, sqlstate, isRetryable(errorCode), serverEpoch);
  }
  if (errorCode >= 3000 && errorCode < 4000) {
    return new InternalError(message, errorCode, sqlstate, isRetryable(errorCode), serverEpoch);
  }
  if (errorCode >= 4000 && errorCode < 5000) {
    return new AuthenticationError(message, errorCode, sqlstate, serverEpoch);
  }
  return new FalconError(message, errorCode, sqlstate, false, serverEpoch);
}

module.exports = {
  // Error codes
  ERR_SYNTAX_ERROR, ERR_INVALID_PARAM, ERR_NOT_LEADER, ERR_FENCED_EPOCH,
  ERR_READ_ONLY, ERR_SERIALIZATION_CONFLICT, ERR_INTERNAL_ERROR,
  ERR_TIMEOUT, ERR_OVERLOADED, ERR_AUTH_FAILED, ERR_PERMISSION_DENIED,
  // Error classes
  FalconError, ProgrammingError, OperationalError, InternalError, AuthenticationError,
  // Helpers
  isRetryable, isFailoverCode, makeError,
};
