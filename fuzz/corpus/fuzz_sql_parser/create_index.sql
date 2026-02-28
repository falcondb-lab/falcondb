CREATE UNIQUE INDEX idx_users_email ON users (email) WHERE deleted_at IS NULL
