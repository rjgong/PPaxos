 SELECT tr.protocol,
    tr.alias,
    tr.command,
    tr.latency,
    tr.clone,
    tr."time",
    tr.exp,
    tr.region
   FROM transactions tr,
    ( SELECT times_1.exp,
            times_1.protocol,
            max(times_1.min_time) AS start_time,
            min(times_1.min_time) + '00:02:00'::interval AS finish_time
           FROM ( SELECT tr_1.exp,
                    tr_1.protocol,
                    min(tr_1."time") AS min_time
                   FROM transactions tr_1
                  GROUP BY tr_1.exp, tr_1.protocol, tr_1.alias) times_1
          GROUP BY times_1.exp, times_1.protocol) times
  WHERE tr.exp = times.exp AND tr.protocol = times.protocol AND tr."time" >= times.start_time AND tr."time" <= times.finish_time;