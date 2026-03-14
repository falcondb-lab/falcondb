SELECT id, name FROM employees WHERE dept = 'eng' UNION ALL SELECT id, name FROM contractors WHERE dept = 'eng' INTERSECT SELECT id, name FROM active_users
