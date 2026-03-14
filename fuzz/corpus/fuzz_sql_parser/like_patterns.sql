SELECT * FROM events WHERE name LIKE '%test\_%' ESCAPE '\' AND description ILIKE '%falcon%' AND tags IS NOT NULL
