DROP TABLE IF EXISTS lsm_test;
CREATE TABLE lsm_test (id INT PRIMARY KEY, name TEXT, value INT) ENGINE=lsm;
INSERT INTO lsm_test (id, name, value) VALUES (1, 'hello', 42), (2, 'world', 99);
SELECT * FROM lsm_test;
SELECT COUNT(*) FROM lsm_test;
