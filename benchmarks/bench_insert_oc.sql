\set seq_id random(1, 100000000000)
INSERT INTO commit_log (seq_id, payload) VALUES (:seq_id, 'bench') ON CONFLICT DO NOTHING;