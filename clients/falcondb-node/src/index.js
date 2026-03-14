'use strict';

const { Connection, connect } = require('./connection');
const { ConnectionPool } = require('./pool');
const { FailoverConnection } = require('./ha');
const {
  FalconError, ProgrammingError, OperationalError, InternalError,
  AuthenticationError, isRetryable, isFailoverCode, makeError,
  ERR_SYNTAX_ERROR, ERR_INVALID_PARAM, ERR_NOT_LEADER, ERR_FENCED_EPOCH,
  ERR_READ_ONLY, ERR_SERIALIZATION_CONFLICT, ERR_INTERNAL_ERROR,
  ERR_TIMEOUT, ERR_OVERLOADED, ERR_AUTH_FAILED, ERR_PERMISSION_DENIED,
} = require('./errors');

module.exports = {
  // Connection
  Connection,
  connect,
  // Pool
  ConnectionPool,
  // HA
  FailoverConnection,
  // Errors
  FalconError,
  ProgrammingError,
  OperationalError,
  InternalError,
  AuthenticationError,
  isRetryable,
  isFailoverCode,
  makeError,
  // Error codes
  ERR_SYNTAX_ERROR,
  ERR_INVALID_PARAM,
  ERR_NOT_LEADER,
  ERR_FENCED_EPOCH,
  ERR_READ_ONLY,
  ERR_SERIALIZATION_CONFLICT,
  ERR_INTERNAL_ERROR,
  ERR_TIMEOUT,
  ERR_OVERLOADED,
  ERR_AUTH_FAILED,
  ERR_PERMISSION_DENIED,
};
