-- Median
SELECT
    region,
    protocol,
    exp,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY Tr.latency) as "median",
    percentile_cont(0.99) WITHIN GROUP (ORDER BY Tr.latency) as "99th_percentile"
FROM filtered_transactions Tr
WHERE exp = ANY(ARRAY['9', '10', '11'])
GROUP BY Tr.region, Tr.protocol, Tr.exp
ORDER BY Tr.region, Tr.exp