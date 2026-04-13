-- Throughput
SELECT
    protocol,
    exp,
    AVG(latency),
    COUNT(latency) / (EXTRACT(EPOCH FROM MAX(Tr.time)) - EXTRACT(EPOCH FROM MIN(Tr.time))) as "throughput"
FROM filtered_transactions Tr
WHERE exp = ANY(ARRAY['16', '9', '12', '13', '14', '15']) AND protocol = 'swiftpaxos'
GROUP BY Tr.protocol, Tr.exp
ORDER BY Tr.exp