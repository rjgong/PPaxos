package client

import (
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

type ReqReply struct {
	Val    state.Value
	Seqnum int
	Time   time.Time
}

type BufferClient struct {
	*Client

	Reply        chan *ReqReply
	GetClientKey func() int64

	seq         bool
	psize       int
	reqNum      int
	writes      int
	window      int32
	conflict    int
	syncFreq    int
	conflictKey int64

	reqTime    []time.Time
	launchTime time.Time

	rand *rand.Rand
}

func NewBufferClient(c *Client, reqNum, psize, conflict, writes int, conflictKey int64) *BufferClient {
	bc := &BufferClient{
		Client: c,

		Reply: make(chan *ReqReply, reqNum+1),

		seq:         true,
		psize:       psize,
		reqNum:      reqNum,
		writes:      writes,
		conflict:    conflict,
		conflictKey: conflictKey,

		reqTime: make([]time.Time, reqNum+1),
	}
	source := rand.NewSource(time.Now().UnixNano() + int64(c.ClientId))
	bc.rand = rand.New(source)
	return bc
}

func (c *BufferClient) Pipeline(syncFreq int, window int32) {
	c.seq = false
	c.syncFreq = syncFreq
	c.window = window
}

func (c *BufferClient) RegisterReply(val state.Value, seqnum int32) {
	t := time.Now()
	c.Reply <- &ReqReply{
		Val:    val,
		Seqnum: int(seqnum),
		Time:   t,
	}
}

func (c *BufferClient) Write(key int64, val []byte) {
	c.SendWrite(key, val)
	<-c.Reply
	return
}

func (c *BufferClient) Read(key int64) []byte {
	c.SendRead(key)
	r := <-c.Reply
	return r.Val
}

func (c *BufferClient) Scan(key, count int64) []byte {
	c.SendScan(key, count)
	r := <-c.Reply
	return r.Val
}

// Assumed to be connected
func (c *BufferClient) Loop() {
	getKey := c.genGetKey()
	val := make([]byte, c.psize)
	c.rand.Read(val)

	var cmdM sync.Mutex
	cmdNum := int32(0)
	wait := make(chan struct{}, 0)
	go func() {
		for i := 0; i <= c.reqNum; i++ {
			r := <-c.Reply
			// Ignore first request
			if i != 0 {
				d := r.Time.Sub(c.reqTime[r.Seqnum])
				m := float64(d.Nanoseconds()) / float64(time.Millisecond)
				//c.PrintDebug("Returning:", r.Val.String())
				c.Printf("%v\n", m)
			}
			if c.window > 0 {
				cmdM.Lock()
				if cmdNum == c.window {
					cmdNum--
					cmdM.Unlock()
					wait <- struct{}{}
				} else {
					cmdNum--
					cmdM.Unlock()
				}
			}
			if c.seq || (c.syncFreq > 0 && i%c.syncFreq == 0) {
				wait <- struct{}{}
			}
		}
		if !c.seq {
			wait <- struct{}{}
		}
	}()

	for i := 0; i <= c.reqNum; i++ {
		key := getKey()
		write := c.randomTrue(c.writes)
		c.reqTime[i] = time.Now()

		// Ignore first request
		if i == 1 {
			c.launchTime = c.reqTime[i]
		}

		if write {
			c.SendWrite(key, state.Value(val))
			// TODO: if the return value != i, something's wrong
		} else {
			c.SendRead(key)
			// TODO: if the return value != i, something's wrong
		}
		if c.window > 0 {
			cmdM.Lock()
			if cmdNum == c.window-1 {
				cmdNum++
				cmdM.Unlock()
				<-wait
			} else {
				cmdNum++
				cmdM.Unlock()
			}
		}
		if c.seq || (c.syncFreq > 0 && i%c.syncFreq == 0) {
			<-wait
		}
	}

	if !c.seq {
		<-wait
	}

	c.Printf("Test took %v\n", time.Now().Sub(c.launchTime))
	c.Disconnect()
}

func (c *BufferClient) WaitReplies(waitFrom int) {
	go func() {
		for {
			r, err := c.GetReplyFrom(waitFrom)
			if err != nil {
				c.Println(err)
				break
			}
			if r.OK != defs.TRUE {
				c.Println("Faulty reply")
				break
			}
			go func(val state.Value, seqnum int32) {
				time.Sleep(c.dt.WaitDuration(c.replicas[waitFrom]))
				c.RegisterReply(val, seqnum)
			}(r.Value, r.CommandId)
		}
	}()
}

func (c *BufferClient) genGetKey() func() int64 {
	key := int64(uuid.New().Time())
	getKey := func() int64 {
		if c.randomTrue(c.conflict) {
			return c.conflictKey
		}
		if c.GetClientKey == nil {
			return key
		}
		return int64(c.GetClientKey())
	}
	return getKey
}

func (c *BufferClient) randomTrue(prob int) bool {
	if prob >= 100 {
		return true
	}
	if prob > 0 {
		return c.rand.Intn(100) <= prob
	}
	return false
}

/*

    while (1) {
        keylist.clear();

        // Begin a transaction.

        // Decide which type of retwis transaction it is going to be.

        bool inorcross = (rand() % 100) < InPer;
        Debug("ttype: %d, clientnumber : %d, inorcross: %d", ttype, regionNumber, inorcross?1:0);
        if (1) {
            if (inorcross) {
                for (int i = 0; i < tLen; i++) {
                    //inregion
                    do {
                        tem = rand_key();
                        key = keys[tem];
                    } while (key_to_shard(key, nShards) % regionNumber != myRegion);

                    keylist.push_back(std::make_pair(keys[tem], value));
                }

            }else {
                do {
                    tem = rand_key();
                    key = keys[tem];
                } while (key_to_shard(key, nShards) % regionNumber == myRegion);
                keylist.push_back(std::make_pair(keys[tem], value));

                do {
                    tem = rand_key();
                    key = keys[tem];
                } while (key_to_shard(key, nShards) % regionNumber != myRegion);
                keylist.push_back(std::make_pair(keys[tem], value));

                for (int i = 0; i < tLen - 2; i++) {
                    tem = rand_key();
                    key = keys[tem];
                    keylist.push_back(std::make_pair(keys[tem], value));
                }
            }

            if(rand() % 100 < wPer) {
                for (int i = 0; i < tLen; i++) {
                    if(rand() % 100 < 50) {
                        Debug("%d: retwisclient put: %s in region %lu", i, keylist[i].first.c_str(), key_to_shard(keylist[i].first, nShards));
                        keylist[i].second = keylist[i].first;
                    } else {
                        Debug("%d: retwisclient get: %s in region %lu", i, keylist[i].first.c_str(), key_to_shard(keylist[i].first, nShards));
                    }
                }
            } else {
                for (int i = 0; i < tLen; i++) {
                    Debug("%d: retwisclient get: %s in region %lu", i, keylist[i].first.c_str(), key_to_shard(keylist[i].first, nShards));
                }
            }
            client->Begin(keylist, inorcross);

            gettimeofday(&t1, NULL);

            status = client->PreCommit(keylist);
        }

        struct timeval t7, t8;
        gettimeofday(&t7, NULL);
        status = client->Commit();
        gettimeofday(&t8, NULL);
        long tem = (t8.tv_sec - t7.tv_sec) * 1000000 + (t8.tv_usec - t7.tv_usec);
        Debug("commit time: %ld.%06ld, commit latency: %ld", t8.tv_sec, t8.tv_usec, tem);


        gettimeofday(&t2, NULL);

        long latency = (t2.tv_sec - t1.tv_sec) * 1000000 + (t2.tv_usec - t1.tv_usec);


        int statistics;

        if (inorcross) {
            statistics = 0;
        } else {
            statistics = 2;
        }

        if (status) {
            statistics += 1;
        }

        fprintf(stderr, "%d %ld.%06ld %ld.%06ld %ld %d %d %d \n", ++nTransactions, t1.tv_sec,
            t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, statistics, ttype, 0);

        if (((t2.tv_sec-t0.tv_sec)*1000000 + (t2.tv_usec-t0.tv_usec)) > duration*1000000) {
            break;
        }

    }
    fprintf(stderr, "# Client exiting..\n");
    return 0;
}

int rand_key()
{
    if (alpha < 0) {
        // Uniform selection of keys.
        return (rand() % nKeys);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[nKeys];

            double c = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                c = c + (1.0 / pow((double) i, alpha));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                sum += (c / pow((double) i, alpha));
                zipf[i-1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand())/RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = nKeys, mid;
        while (l < r) {
            mid = (l + r) / 2;
            if (random > zipf[mid]) {
                l = mid + 1;
            } else if (random < zipf[mid]) {
                r = mid - 1;
            } else {
                break;
            }
        }
        return mid;
    }
}*/
