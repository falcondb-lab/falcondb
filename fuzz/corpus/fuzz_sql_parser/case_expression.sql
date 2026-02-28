SELECT id, CASE WHEN status = 1 THEN 'active' WHEN status = 2 THEN 'suspended' ELSE 'unknown' END AS status_label, COALESCE(nickname, name, 'anonymous') FROM users
