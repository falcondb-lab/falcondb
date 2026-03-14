DELETE FROM logs WHERE created_at < '2024-01-01' AND id NOT IN (SELECT log_id FROM audit_trail)
