SELECT o_custkey, COUNT(*)
FROM orders
WHERE o_totalprice > 220388.06
GROUP BY o_custkey