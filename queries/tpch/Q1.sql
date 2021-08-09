SELECT o_custkey, COUNT(*)
FROM orders
GROUP BY o_custkey