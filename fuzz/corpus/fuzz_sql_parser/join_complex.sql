SELECT a.id, b.name, COUNT(*) FROM orders a LEFT JOIN customers b ON a.cust_id = b.id WHERE a.total > 100.50 GROUP BY a.id, b.name HAVING COUNT(*) > 1
