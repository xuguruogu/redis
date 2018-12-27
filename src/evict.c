/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across freeMemoryIfNeeded() calls.
 *
 * Entries inside the eviciton pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictionPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

static struct evictionPoolEntry *EvictionPoolLRU;
static struct evictionPoolEntry *ColdKeyPool;
static struct evictionPoolEntry *HotKeyPool;

#ifdef TEST_HOTKEY_POOL
static struct evictionPoolEntry *TestHotKeyPool;
#endif

unsigned long LFUDecrAndReturn(robj *o);

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures. */
unsigned int getLRUClock(void) {
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call. */
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    if (1000/server.hz <= LRU_CLOCK_RESOLUTION) {
        atomicGet(server.lruclock,lruclock);
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
                    LRU_CLOCK_RESOLUTION;
    }
}

/* freeMemoryIfNeeded() gets called when 'maxmemory' is set on the config
 * file to limit the max memory used by the server, before processing a
 * command.
 *
 * The goal of the function is to free enough memory to keep Redis under the
 * configured memory limit.
 *
 * The function starts calculating how many bytes should be freed to keep
 * Redis under the limit, and enters a loop selecting the best keys to
 * evict accordingly to the configured policy.
 *
 * If all the bytes needed to return back under the limit were freed the
 * function returns C_OK, otherwise C_ERR is returned, and the caller
 * should block the execution of commands that will result in more memory
 * used by the server.
 *
 * ------------------------------------------------------------------------
 *
 * LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */

/* Create a new eviction pool. */
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
    if (server.swap_mode) {
        ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
        for (j = 0; j < EVPOOL_SIZE; j++) {
            ep[j].idle = 0;
            ep[j].key = NULL;
            ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
            ep[j].dbid = 0;
        }
        ColdKeyPool = ep;

        ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
        for (j = 0; j < EVPOOL_SIZE; j++) {
            ep[j].idle = 0;
            ep[j].key = NULL;
            ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
            ep[j].dbid = 0;
        }
        HotKeyPool = ep;
    }
}

void emptyEvictionPool() {
    int i, j;
    struct evictionPoolEntry* pools[3] = {EvictionPoolLRU, ColdKeyPool, HotKeyPool};
    struct evictionPoolEntry *ep;
    for (i = 0; i < 3; i++) {
        ep = pools[i];
        for (j = 0; j < EVPOOL_SIZE; j++) {
            ep[j].idle = 0;
            if (ep[j].key != NULL && ep[j].key != ep[j].cached) {
                sdsfree(ep[j].key);
            }
            ep[j].key = NULL;
            ep[j].dbid = 0;
        }
    }
    serverAssert(EvictionPoolLRU[0].key == NULL && EvictionPoolLRU[0].idle == 0);
    serverAssert(ColdKeyPool[0].key == NULL && ColdKeyPool[0].idle == 0);
    serverAssert(HotKeyPool[0].key == NULL && HotKeyPool[0].idle == 0);
}

void tryInsertHotOrColdPool(struct evictionPoolEntry *pool, sds key, int dbid, unsigned long long idle, int pool_type) {
    /* Insert the element inside the pool.
    * First, find the first empty bucket or the first populated
    * bucket that has an idle time smaller than our idle time. */
    int k = 0;
    if (pool_type == COLD_POOL_TYPE) {
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;
    } else if (pool_type == HOT_POOL_TYPE) {
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle > idle) k++;
    }

    if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
        /* Can't insert if the element is < the worst element we have
         * and there are no empty buckets. */
        return;
    } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
        /* Inserting into empty position. No setup needed before insert. */
    } else {
        /* Inserting in the middle. Now k points to the first element
         * greater than the element to insert.  */
        if (pool[EVPOOL_SIZE-1].key == NULL) {
            /* Free space on the right? Insert at k shifting
             * all the elements from k to end to the right. */

            /* Save SDS before overwriting. */
            sds cached = pool[EVPOOL_SIZE-1].cached;
            memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
            pool[k].cached = cached;
        } else {
            /* No free space on right? Insert at k-1 */
            k--;
            /* Shift all elements on the left of k (included) to the
             * left, so we discard the element with smaller idle time. */
            sds cached = pool[0].cached; /* Save SDS before overwriting. */
            if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
            memmove(pool,pool+1,sizeof(pool[0])*k);
            pool[k].cached = cached;
        }
    }
    serverLog(LL_DEBUG, "key: %s is insert into %s pool", key, pool_type == HOT_POOL_TYPE ? "hot" : "cold");

    /* Try to reuse the cached SDS string allocated in the pool entry,
     * because allocating and deallocating this object is costly
     * (according to the profiler, not my fantasy. Remember:
     * premature optimizbla bla bla bla. */
    int klen = sdslen(key);
    if (klen > EVPOOL_CACHED_SDS_SIZE) {
        pool[k].key = sdsdup(key);
    } else {
        memcpy(pool[k].cached,key,klen+1);
        sdssetlen(pool[k].cached,klen);
        pool[k].key = pool[k].cached;
    }
    pool[k].idle = idle;
    pool[k].dbid = dbid;
}

void replaceKeyInPool(struct evictionPoolEntry *pool, sds key, int dbid, unsigned long long idle, int pool_type) {
    /* Insert the element inside the pool.
    * First, find the first empty bucket or the first populated
    * bucket that has an idle time smaller than our idle time. */
    int k = 0, i = 0, old_index = -1;
    while (i < EVPOOL_SIZE && pool[i].key) {
        if (0 == sdscmp(key, pool[i].key)) {
            /* the key should be unique in this pool before adding it. */
            serverAssert(old_index == -1);
            /* the key already exists in the pool. */
            old_index = i;
        }
        if ((pool_type == COLD_POOL_TYPE && pool[i].idle < idle) ||
            (pool_type == HOT_POOL_TYPE && pool[i].idle > idle))
            k++;
        i++;
    }

    if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
        /* Can't insert if the element is < the worst element we have
         * and there are no empty buckets. */
        return;
    } else {
        if (old_index != -1) {
            /* if the key is already in the pool, we just update the idle time, and
             * move the keys between the 'old_index' item and the 'k' item in the pool
             * to keep the order(by idle time of keys). */
            if (old_index == k) {
                /* update idle time */
                pool[old_index].idle = idle;
            } else if (old_index < k) {
                if (old_index+1 == k) {
                    k--;
                } else {
                    sds save = pool[old_index].key;
                    sds cached = pool[old_index].cached;

                    k--;
                    /* move keys forwards*/
                    memmove(pool+old_index, pool+old_index+1, (k-old_index)*sizeof(pool[0]));
                    /* re-use the buffer */
                    pool[k].cached = cached;
                    pool[k].key = save;
                }
            } else if (old_index > k) {
                sds save = pool[old_index].key;
                sds cached = pool[old_index].cached;
                /* move keys backwards*/
                memmove(pool+k+1, pool+k, (old_index-k)*sizeof(pool[0]));
                /* re-use the buffer */
                pool[k].cached = cached;
                pool[k].key = save;
            }
            /* update idle time */
            pool[k].idle = idle;

            serverLog(LL_DEBUG, "key: %s is already in %s pool, update its idle value",
                      key, pool_type == HOT_POOL_TYPE ? "hot" : "cold");
        } else {
            if (k < EVPOOL_SIZE && pool[k].key == NULL) {
                /* Inserting into empty position. No setup needed before insert. */
            } else {
                /* Inserting in the middle. Now k points to the first element
                 * greater than the element to insert.  */
                if (pool[EVPOOL_SIZE-1].key == NULL) {
                    /* Free space on the right? Insert at k shifting
                     * all the elements from k to end to the right. */

                    /* Save SDS before overwriting. */
                    sds cached = pool[EVPOOL_SIZE-1].cached;
                    memmove(pool+k+1,pool+k,
                            sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                    pool[k].cached = cached;
                } else {
                    /* No free space on right? Insert at k-1 */
                    k--;
                    /* Shift all elements on the left of k (included) to the
                     * left, so we discard the element with smaller idle time. */
                    sds cached = pool[0].cached; /* Save SDS before overwriting. */
                    if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                    memmove(pool,pool+1,sizeof(pool[0])*k);
                    pool[k].cached = cached;
                }
            }

            serverLog(LL_DEBUG, "key: %s is insert into %s pool", key, pool_type == HOT_POOL_TYPE ? "hot" : "cold");

            /* Try to reuse the cached SDS string allocated in the pool entry,
             * because allocating and deallocating this object is costly
             * (according to the profiler, not my fantasy. Remember:
             * premature optimizbla bla bla bla. */
            int klen = sdslen(key);
            if (klen > EVPOOL_CACHED_SDS_SIZE) {
                pool[k].key = sdsdup(key);
            } else {
                memcpy(pool[k].cached,key,klen+1);
                sdssetlen(pool[k].cached,klen);
                pool[k].key = pool[k].cached;
            }
            pool[k].idle = idle;
            pool[k].dbid = dbid;
        }
    }
}

void replaceKeyInHotPool(sds key, int dbid, unsigned long long idle) {
    replaceKeyInPool(HotKeyPool, key, dbid, idle, HOT_POOL_TYPE);
}

void tryInsertColdPool(struct evictionPoolEntry *pool, sds key, int dbid, unsigned long long idle) {
    tryInsertHotOrColdPool(pool, key, dbid, idle, COLD_POOL_TYPE);
}

void coldKeyPopulate(dict *sampledict, struct evictionPoolEntry *pool) {
    int j, count;
    dictEntry *samples[server.maxmemory_samples];

    /* we support cold key transfer to ssdb only if evict algorithm is LFU */
    if (!(server.maxmemory_policy & MAXMEMORY_FLAG_LFU)) {
        return;
    }
    count = dictGetSomeKeys(sampledict,samples,server.maxmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        if (dictFind(EVICTED_DATA_DB->transferring_keys, key) != NULL)
            continue;

        if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255. */
            if (server.swap_mode) {
                idle = 255-KeyLFUDecrAndReturn(key);
            } else {
                robj* o = dictGetVal(de);
                idle = 255-LFUDecrAndReturn(o);
            }
        } else {
            serverPanic("Unknown eviction policy in coldKeyPopulate()");
        }

        if (idle >= (unsigned long long)server.lowest_idle_val_of_cold_key)
            tryInsertColdPool(pool, key, 0, idle);
    }
}

/* This is an helper function for freeMemoryIfNeeded(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time smaller than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right. */
void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool) {
    int j, count;
    dictEntry *samples[server.maxmemory_samples];

    count = dictGetSomeKeys(sampledict,samples,server.maxmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);

        /* skip the keys already in "transfering" state. */
        if (server.swap_mode && dictFind(EVICTED_DATA_DB->transferring_keys, key) != NULL)
            continue;

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL) {
            if (sampledict != keydict) de = dictFind(keydict, key);
            o = dictGetVal(de);
        }

        /* Calculate the idle time according to the policy. This is called
         * idle just because the code initially handled LRU, but is in fact
         * just a score where an higher score means better candidate. */
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU) {
            idle = estimateObjectIdleTime(o);
        } else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255. */
            if (server.swap_mode) {
                idle = 255-KeyLFUDecrAndReturn(key);
            } else {
                idle = 255-LFUDecrAndReturn(o);
            }
        } else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            /* In this case the sooner the expire the better. */
            idle = ULLONG_MAX - (long)dictGetVal(de);
        } else {
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        tryInsertColdPool(pool, key, dbid, idle);
    }
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.

 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
 * when incrementing the key, so that keys starting at COUNTER_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the COUNTER_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
unsigned long LFUGetTimeInMinutes(void) {
    return (server.unixtime/60) & 65535;
}

/* Given an object last decrement time, compute the minimum number of minutes
 * that elapsed since the last decrement. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
unsigned long LFUTimeElapsed(unsigned long ldt) {
    unsigned long now = LFUGetTimeInMinutes();
    if (now >= ldt) return now-ldt;
    return 65535-ldt+now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really implemented. Saturate it at 255. */
uint8_t LFULogIncr(uint8_t counter) {
    if (counter == 255) return 255;
    double r = (double)rand()/RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0/(baseval*server.lfu_log_factor+1);
    if (r < p) counter++;
    return counter;
}

/* If the object decrement time is reached, decrement the LFU counter and
 * update the decrement time field. Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed. */
#define LFU_DECR_INTERVAL 1
unsigned long LFUDecrAndReturn(robj *o) {
    unsigned long ldt = o->lru >> 8;
    unsigned long counter = o->lru & 255;
    if (LFUTimeElapsed(ldt) >= server.lfu_decay_time && counter) {
        if (counter > LFU_INIT_VAL*2) {
            counter /= 2;
            if (counter < LFU_INIT_VAL*2) counter = LFU_INIT_VAL*2;
        } else {
            counter--;
        }
        o->lru = (LFUGetTimeInMinutes()<<8) | counter;
    }
    return counter;
}

/* in swap_mode, the lfu info is stored in the sds header of db key. */
unsigned long KeyLFUDecrAndReturn(sds key) {
    unsigned int lfu = sdsgetlfu(key);
    unsigned long ldt = lfu >> 8;
    unsigned long counter = lfu & 255;
    if (LFUTimeElapsed(ldt) >= server.lfu_decay_time && counter) {
        if (counter > LFU_INIT_VAL*2) {
            counter /= 2;
            if (counter < LFU_INIT_VAL*2) counter = LFU_INIT_VAL*2;
        } else {
            counter--;
        }
        sdssetlfu(key, (LFUGetTimeInMinutes()<<8) | counter);
    }
    return counter;
}

void cleanupEpilogOfEvicting(redisDb *db, robj *keyobj) {
    if (dictSize(EVICTED_DATA_DB->transferring_keys) > 0) {
        if (dictDelete(EVICTED_DATA_DB->transferring_keys, keyobj->ptr) == DICT_OK) {
            signalBlockingKeyAsReady(db, keyobj);
            serverLog(LL_DEBUG, "key: %s is unblocked and deleted from transferring_keys.",
                      (char *)keyobj->ptr);
        }
    }
}

int epilogOfEvictingToSSDB(robj *keyobj) {
    redisDb *evicteddb = server.db + EVICTED_DATA_DBID, *db;
    mstime_t eviction_latency;
    dictEntry *de, *ev_de;
    long long now = mstime(), expiretime;
    int dbid = 0;
    sds db_key, evdb_key;
    unsigned int lfu;
    robj* tmpargv[3];

    db = server.db + dbid;
    expiretime = getExpire(db, keyobj);

    /* Only transfer effective data. */
    if (expiretime > 0 && now > expiretime) {
        expireIfNeeded(db, keyobj);
        serverLog(LL_DEBUG, "The key: %s has expired.", (char *)keyobj->ptr);
        cleanupEpilogOfEvicting(db, keyobj);
        return C_ERR;
    }

    de = dictFind(db->dict, keyobj->ptr);

    /* The key may be deleted before the callback ssdb-resp-del. */
    if (!de) {
        cleanupEpilogOfEvicting(db, keyobj);
        return C_ERR;
    }

    /* save lfu info when transfer. */
    db_key = dictGetKey(de);
    lfu = sdsgetlfu(db_key);

    latencyStartMonitor(eviction_latency);
    if (server.lazyfree_lazy_eviction)
        dbAsyncDelete(db,keyobj);
    else
        dbSyncDelete(db,keyobj);
    latencyEndMonitor(eviction_latency);
    latencyAddSampleIfNeeded("coldkey-transfer",eviction_latency);

    /* Record the evicted keys in an extra redis db. */
    setKey(evicteddb, keyobj, shared.integers[0]);
    /* update LFU with old value. */
    ev_de = dictFind(evicteddb->dict, keyobj->ptr);
    evdb_key = dictGetKey(ev_de);
    sdssetlfu(evdb_key, lfu);

    server.dirty ++;

    notifyKeyspaceEvent(NOTIFY_STRING,"set",keyobj,evicteddb->id);

    /* propagate storetossdb command to slaves. */
    tmpargv[0] = shared.storecmdobj;
    tmpargv[1] = keyobj;
    propagate(lookupCommand(shared.storecmdobj->ptr), 0, tmpargv, 2, PROPAGATE_REPL);

    /* propagate aof to del key from redis db.*/
    robj* delCmdObj = createStringObject("del",3);
    tmpargv[0] = delCmdObj;
    tmpargv[1] = keyobj;
    propagate(server.delCommand,db->id, tmpargv, 2, PROPAGATE_AOF);
    decrRefCount(delCmdObj);

    /* propage aof to set key in evict db. */
    robj* setCmdObj = createStringObject("set",3);
    tmpargv[0] = setCmdObj;
    tmpargv[1] = keyobj;
    tmpargv[2] = shared.integers[0];
    propagate(server.setCommand,EVICTED_DATA_DBID, tmpargv, 3, PROPAGATE_AOF);
    decrRefCount(setCmdObj);

    /* Record the expire info. */
    if (expiretime > 0) {
        setExpire(NULL, evicteddb, keyobj, expiretime);
        notifyKeyspaceEvent(NOTIFY_GENERIC,
                            "expire", keyobj, evicteddb->id);

        tmpargv[0] = createStringObject("PEXPIREAT", 9);
        tmpargv[1] = keyobj;
        tmpargv[2] = createObject(OBJ_STRING, sdsfromlonglong(expiretime));

        /* propage aof to set expire info in evict db. */
        propagate(server.pexpireatCommand, EVICTED_DATA_DBID, tmpargv, 3, PROPAGATE_AOF);

        decrRefCount(tmpargv[0]);
        decrRefCount(tmpargv[2]);
    }

    cleanupEpilogOfEvicting(db, keyobj);

    server.stat_ssdbkeys++;
    notifyKeyspaceEvent(NOTIFY_EVICTED, "transfer-to-SSDB",
                        keyobj, db->id);

    /* When the memory to free starts to be big enough, we may
     * start spending so much time here that is impossible to
     * deliver data to the slaves fast enough, so we force the
     * transmission here inside the loop. */
    if (listLength(server.slaves)) flushSlavesOutputBuffers();

    return C_OK;
}

int prologOfLoadingFromSSDB(client* c, robj *keyobj) {
    rio cmd;

    if (expireIfNeeded(EVICTED_DATA_DB, keyobj)) {
        serverLog(LL_DEBUG, "key: %s is expired in redis.", (char *)keyobj->ptr);
        if (c) addReplyError(c, "this key is expired");
        return C_OK;
    }

    rioInitWithBuffer(&cmd, sdsempty());
    serverAssert(rioWriteBulkCount(&cmd, '*', 3));
    serverAssert(rioWriteBulkString(&cmd, "redis_req_dump", strlen("redis_req_dump")));
    serverAssert(sdsEncodedObject(keyobj));
    serverAssert(rioWriteBulkString(&cmd, keyobj->ptr, sdslen(keyobj->ptr)));
    server.global_transfer_id++;
    serverAssert(rioWriteBulkLongLong(&cmd, server.global_transfer_id));

    /* sendCommandToSSDB will free cmd.io.buffer.ptr. */
    if (sendCommandToSSDB(server.ssdb_client, cmd.io.buffer.ptr) != C_OK) {
        if (c) addReplyError(c, "ssdb transfer/loading connection is disconnected.");
        return C_ERR;
    }

    setLoadingDB(keyobj, server.global_transfer_id);
    if (c) addReply(c,shared.ok);

    serverLog(LL_DEBUG, "Loading key: %s from SSDB started.", (char *)(keyobj->ptr));
    return C_OK;
}

int prologOfEvictingToSSDB(robj *keyobj, redisDb *db) {
    rio cmd, payload;
    long long ttl = 0;
    long long expiretime;
    long long now = mstime();
    dictEntry* de;
    robj *o;

    de = dictFind(db->dict, keyobj->ptr);
    if (!de) {
        serverLog(LL_DEBUG, "key: %s is not existed in redis.", (char *)keyobj->ptr);
        return C_ERR;
    }
    expiretime = getExpire(db, keyobj);

    if (expireIfNeeded(db, keyobj)) {
        serverLog(LL_DEBUG, "key: %s is expired in redis, dbid: %d", (char *)keyobj->ptr, db->id);
        return C_ERR;
    }

    /* when the key was expired/evicted before but had not been deleted from SSDB. but now
     * we are sure it's a new key, so remove it from ssdb_keys_to_clean to avoid deleting
     * a key by mistake. */
    if (dictFind(EVICTED_DATA_DB->ssdb_keys_to_clean, keyobj->ptr)) {
        dictDelete(EVICTED_DATA_DB->ssdb_keys_to_clean, keyobj->ptr);
    }

    if (expiretime != -1) {
        /* todo: to optimize for keys with very little ttl time, we
         * don't transfer but let redis expire them. */
        ttl = expiretime - now;
        if (ttl < 1) ttl = 1;
    } else {
        // for 'restore' command, when ttl is 0 the key is created without any expire
        ttl = 0;
    }

    rioInitWithBuffer(&cmd, sdsempty());
    serverAssert(rioWriteBulkCount(&cmd, '*', 6));
    serverAssert(rioWriteBulkString(&cmd, "redis_req_restore", strlen("redis_req_restore")));
    serverAssert(sdsEncodedObject(keyobj));
    serverAssert(rioWriteBulkString(&cmd, keyobj->ptr, sdslen(keyobj->ptr)));
    serverAssert(rioWriteBulkLongLong(&cmd, ttl));

    o = dictGetVal(de);
    serverAssert(o);
    createDumpPayload(&payload, o);

    serverAssert(rioWriteBulkString(&cmd, payload.io.buffer.ptr,
                                    sdslen(payload.io.buffer.ptr)));
    sdsfree(payload.io.buffer.ptr);

    /* NOTE: we must use "REPLACE" option when restore a key to SSDB, because maybe there
     * is a identical dirty key in SSDB. */
    serverAssert(rioWriteBulkString(&cmd, "REPLACE", strlen("REPLACE")));
    server.global_transfer_id++;
    serverAssert(rioWriteBulkLongLong(&cmd, server.global_transfer_id));

    /* sendCommandToSSDB will free cmd.io.buffer.ptr. */
    /* Using the same connection with propagate method. */
    if (sendCommandToSSDB(server.ssdb_client, cmd.io.buffer.ptr) != C_OK) {
        serverLog(LL_DEBUG, "Failed to send the restore cmd to SSDB.");
        return C_FD_ERR;
    }

    setTransferringDB(db, keyobj, server.global_transfer_id);
    serverLog(LL_DEBUG, "Evicting key: %s to SSDB, maxmemory: %lld, zmalloc_used_memory: %lu.",
              (char *)(keyobj->ptr), server.maxmemory, zmalloc_used_memory());

    return C_OK;
}

#define OBJ_COMPUTE_SIZE_DEF_SAMPLES 5 /* Default sample size. */
size_t estimateKeyMemoryUsage(dictEntry *de) {
    size_t usage;
    usage = objectComputeSize(dictGetVal(de), OBJ_COMPUTE_SIZE_DEF_SAMPLES);
    usage += sdsAllocSize(dictGetKey(de));
    usage += sizeof(dictEntry);
    return usage;
}

int tryEvictingKeysToSSDB(size_t *mem_tofree) {
    mstime_t latency;
    int k, i;
    sds bestkey = NULL;
    int bestdbid;
    redisDb *db;
    dict *dict;
    dictEntry *de;

    latencyStartMonitor(latency);
    struct evictionPoolEntry *pool = ColdKeyPool;

    unsigned long total_keys = 0, keys;

    db = server.db;
    dict = db->dict;
    if ((keys = dictSize(dict)) != 0) {
        for (i = 0; i < server.coldkey_filter_times_everytime; i++) {
            coldKeyPopulate(dict, pool);
        }
        total_keys += keys;
    }

    /* there are no keys in ColdKeyPool to evict. */
    if (!total_keys || !ColdKeyPool[0].key) return C_ERR; /* No keys to evict. */

    /* limit concurrent number of transferring keys */
    if (dictSize(EVICTED_DATA_DB->transferring_keys) >= (unsigned long)server.master_max_concurrent_transferring_keys) return C_ERR;

    /* Go backward from best to worst element to evict. */
    for (k = EVPOOL_SIZE-1; k >= 0; k--) {
        if (pool[k].key == NULL) continue;
        bestdbid = pool[k].dbid;

        de = dictFind(server.db[bestdbid].dict,
                      pool[k].key);

        /* Remove the entry from the pool. */
        if (pool[k].key != pool[k].cached)
            sdsfree(pool[k].key);
        pool[k].key = NULL;
        pool[k].idle = 0;

        /* If the key exists, is our pick. Otherwise it is
         * a ghost and we need to try the next element. */
        if (de && dictFind(EVICTED_DATA_DB->transferring_keys, dictGetKey(de)) == NULL
            && dictFind(EVICTED_DATA_DB->visiting_ssdb_keys, dictGetKey(de)) == NULL
            && dictFind(EVICTED_DATA_DB->delete_confirm_keys, dictGetKey(de)) == NULL
            && dictFind(server.hot_keys, dictGetKey(de)) == NULL
            && dictFind(EVICTED_DATA_DB->loading_hot_keys, dictGetKey(de)) == NULL) {
            //&& !isMigratingSSDBKey(dictGetKey(de))) {
            size_t usage;
            bestkey = dictGetKey(de);
            /* Estimate the memory usage of the bestkey. */
            usage = estimateKeyMemoryUsage(de);

            serverLog(LL_DEBUG, "The best key size: %lu", usage);
            *mem_tofree = *mem_tofree - usage;
            break;
        }
    }

    /* Try to remove the selected key. */
    if (bestkey) {
        if (de) {
            unsigned int lfu_counter = 255 & sdsgetlfu(dictGetKey(de));
            unsigned int idle = 255 - lfu_counter;
            if (idle >= (unsigned long long)server.lowest_idle_val_of_cold_key) {
                robj *keyobj = createStringObject(bestkey,sdslen(bestkey));

                db = server.db+bestdbid;
                /* Try restoring the redis dumped data to SSDB. */
                if (prologOfEvictingToSSDB(keyobj, db) == C_FD_ERR)
                    return C_ERR;
                decrRefCount(keyobj);
            }
        }
    }

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("tryEvictingKeysToSSDB", latency);
    return C_OK;
}

/* ----------------------------------------------------------------------------
 * The external API for eviction: freeMemroyIfNeeded() is called by the
 * server when there is data to add in order to make space if needed.
 * --------------------------------------------------------------------------*/

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size. This function
 * returns the sum of AOF and slaves buffer. */
size_t freeMemoryGetNotCountedMemory(void) {
    size_t overhead = 0;
    int slaves = listLength(server.slaves);

    if (slaves) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = listNodeValue(ln);
            overhead += getClientOutputBufferMemoryUsage(slave);
        }
    }
    if (server.aof_state != AOF_OFF) {
        overhead += sdslen(server.aof_buf)+aofRewriteBufferSize();
    }
    return overhead;
}

int freeMemoryIfNeeded(void) {
    size_t mem_reported, mem_used, mem_tofree, mem_freed;
    mstime_t latency, eviction_latency;
    long long delta;
    int slaves = listLength(server.slaves);

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    if (clientsArePaused()) return C_OK;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    mem_reported = zmalloc_used_memory();
    if (mem_reported <= server.maxmemory) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    mem_used = mem_reported;
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;

    /* Check if we are still over the memory limit. */
    if (mem_used <= server.maxmemory) return C_OK;

    /* Compute how much memory we need to free. */
    mem_tofree = mem_used - server.maxmemory;
    mem_freed = 0;

    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION)
        goto cant_free; /* We need to free memory, but policy forbids. */

    latencyStartMonitor(latency);
    while (mem_freed < mem_tofree) {
        int j, k, i, keys_freed = 0;
        static int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            while(bestkey == NULL) {
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                for (i = 0; i < server.dbnum; i++) {
                    if (server.swap_mode && i == EVICTED_DATA_DBID) continue;

                    db = server.db+i;
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ?
                            db->dict : db->expires;
                    if ((keys = dictSize(dict)) != 0) {
                        evictionPoolPopulate(i, dict, db->dict, pool);
                        total_keys += keys;
                    }
                }
                if (!total_keys) break; /* No keys to evict. */

                /* If total_keys < dictSize of EVICTED_DATA_DB->transferring_keys,
                 the loop happens to be endless. */
                if (server.swap_mode
                    && total_keys <= dictSize(EVICTED_DATA_DB->transferring_keys))
                    break;

                /* Go backward from best to worst element to evict. */
                for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                    int key_is_transfering = 0;
                    if (pool[k].key == NULL) continue;
                    bestdbid = pool[k].dbid;

                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        de = dictFind(server.db[pool[k].dbid].dict,
                            pool[k].key);
                    } else {
                        de = dictFind(server.db[pool[k].dbid].expires,
                            pool[k].key);
                    }

                    if (server.swap_mode
                        && dictFind(EVICTED_DATA_DB->transferring_keys, pool[k].key) != NULL)
                        key_is_transfering = 1;

                    /* Remove the entry from the pool. */
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    if (server.swap_mode && key_is_transfering) {
                        continue;
                    }

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    if (de) {
                        bestkey = dictGetKey(de);
                        break;
                    } else {
                        /* Ghost... Iterate again. */

                        /* I think there are the following cases:
                         * 1. the key maybe had been deleted after pushed into eviction pool.
                         * 2. in swap_mode, the key maybe had been transfered to SSDB after
                         * pushed into eviction pool.
                         * */
                    }
                }
            }
        }

        /* volatile-random and allkeys-random policy */
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
        {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            for (i = 0; i < server.dbnum; i++) {
                j = (++next_db) % server.dbnum;
                db = server.db+j;
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ?
                        db->dict : db->expires;
                if (dictSize(dict) != 0) {
                    de = dictGetRandomKey(dict);
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }

        /* Finally remove the selected key. */
        if (bestkey) {
            db = server.db+bestdbid;
            robj *keyobj = createStringObject(bestkey,sdslen(bestkey));

            if (server.swap_mode && 0 == checkBeforeExpire(db, keyobj)) continue;

            propagateExpire(db,keyobj,server.lazyfree_lazy_eviction);
            /* We compute the amount of memory freed by db*Delete() alone.
             * It is possible that actually the memory needed to propagate
             * the DEL in AOF and replication link is greater than the one
             * we are freeing removing the key, but we can't account for
             * that otherwise we would never exit the loop.
             *
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);
            if (server.lazyfree_lazy_eviction)
                dbAsyncDelete(db,keyobj);
            else
                dbSyncDelete(db,keyobj);
            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del",eviction_latency);
            latencyRemoveNestedEvent(latency,eviction_latency);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;
            server.stat_evictedkeys++;
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                keyobj, db->id);
            decrRefCount(keyobj);
            keys_freed++;

            /* When the memory to free starts to be big enough, we may
             * start spending so much time here that is impossible to
             * deliver data to the slaves fast enough, so we force the
             * transmission here inside the loop. */
            if (slaves) flushSlavesOutputBuffers();

            /* Normally our stop condition is the ability to release
             * a fixed, pre-computed amount of memory. However when we
             * are deleting objects in another thread, it's better to
             * check, from time to time, if we already reached our target
             * memory, since the "mem_freed" amount is computed only
             * across the dbAsyncDelete() call, while the thread can
             * release the memory all the time. */
            if (server.lazyfree_lazy_eviction && !(keys_freed % 16)) {
                overhead = freeMemoryGetNotCountedMemory();
                mem_used = zmalloc_used_memory();
                mem_used = (mem_used > overhead) ? mem_used-overhead : 0;
                if (mem_used <= server.maxmemory) {
                    mem_freed = mem_tofree;
                }
            }
        }

        if (!keys_freed) {
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("eviction-cycle",latency);
            goto cant_free; /* nothing to free... */
        }
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);
    return C_OK;

cant_free:
    /* We are here if we are not able to reclaim memory. There is only one
     * last thing we can try: check if the lazyfree thread has jobs in queue
     * and wait... */
    while(bioPendingJobsOfType(BIO_LAZY_FREE)) {
        if (((mem_reported - zmalloc_used_memory()) + mem_freed) >= mem_tofree)
            break;
        usleep(1000);
    }
    return C_ERR;
}

void handleClientsBlockedOnSSDB(void) {
    while(listLength(server.ssdb_ready_keys) != 0) {
        list *l;

        /* Point server.ready_keys to a fresh list and save the current one
         * locally. This way as we run the old list we are free to call
         * signalBlockingKeyAsReady() that may push new elements in server.ready_keys
         * when handling clients blocked into SSDB loading/transferring. */
        l = server.ssdb_ready_keys;
        server.ssdb_ready_keys = listCreate();

        while (listLength(l) != 0) {
            listNode *ln = listFirst(l);
            readyList *rl = ln->value;
            dictEntry *de;

            /* First of all remove this key from db->ssdb_ready_keys so that
             * we can safely call signalBlockingKeyAsReady() against this key. */
            dictDelete(rl->db->ssdb_ready_keys, rl->key);

            /* We serve clients in the same order they blocked for
             * this key, from the first blocked to the last. */
            de = dictFind(server.db->ssdb_blocking_keys,rl->key);
            if (de) {
                list *clients = dictGetVal(de);
                int numclients = listLength(clients);

                while (numclients--) {
                    listNode *clientnode = listFirst(clients);
                    client *c = clientnode->value;
                    int retval;

                    removeClientFromListForBlockedKey(c, server.db->ssdb_blocking_keys, rl->key);

                    /* Remove this key from the blocked keys dict of this client */
                    serverLog(LL_DEBUG, "key :%s is deleted from loading_or_transfer_keys.",
                              (char *)rl->key->ptr);
                    retval = dictDelete(c->bpop.loading_or_transfer_keys, rl->key);

                    /* If the client is closed and reconnect with the same fd,
                       dictDelete will fail. */
                    if (retval) continue;

                    /* Unblock this client if all blocked keys in the command arguments
                     * are ready(load/transfer done). */
                    if (dictSize(c->bpop.loading_or_transfer_keys) == 0) {
                        dictEmpty(c->bpop.loading_or_transfer_keys,NULL);
                        /* Unblock this client and add it into "server.unblocked_clients",
                         * The blocked command will be executed in "processInputBuffer".*/
                        serverLog(LL_DEBUG, "client fd: %d is unblocked.", c->fd);

                        unblockClient(c);

                        if (c->flags & CLIENT_MASTER && server.slave_failed_retry_interrupted) {
                            confirmAndRetrySlaveSSDBwriteOp(c, server.blocked_write_op->time, server.blocked_write_op->index);
                        } else {
                            size_t prev_offset = c->reploff;
                            if (tryBlockingClient(c) == C_OK && runCommand(c) == C_OK) {
                                if (c->flags & CLIENT_MASTER) {
                                    size_t applied = c->reploff - prev_offset;
                                    if (applied) {
                                        replicationFeedSlavesFromMasterStream(server.slaves,
                                                                              c->pending_querybuf, applied);
                                        sdsrange(c->pending_querybuf,applied,-1);
                                    }
                                }
                                resetClient(c);
                            }
                            if (c->flags & CLIENT_MASTER && server.send_failed_write_after_unblock) {
                                serverAssert(c->flags & CLIENT_MASTER && !(c->ssdb_conn_flags & CONN_SUCCESS));
                                confirmAndRetrySlaveSSDBwriteOp(c, -1,-1);
                                server.send_failed_write_after_unblock = 0;
                            }
                        }
                    }
                }
            }

            /* Free this item. */
            decrRefCount(rl->key);
            zfree(rl);
            listDelNode(l,ln);
        }
        listRelease(l); /* We have the new list on place at this point. */
    }
}

void handleClientsBlockedOnCustomizedPsync(void) {
    listIter li;
    listNode *ln;
    int ret;

    listRewind(server.no_writing_ssdb_blocked_clients, &li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        listDelNode(server.no_writing_ssdb_blocked_clients, ln);
        unblockClient(c);

        /* Convert block type. */
        if ((ret = tryBlockingClient(c)) != C_OK) {
            /* C_NOTSUPPORT_ERR should be handled before. */
            serverAssert(ret != C_NOTSUPPORT_ERR);
            continue;
        }
        if (runCommand(c) == C_OK)
            resetClient(c);
        if (c->flags & CLIENT_MASTER && server.send_failed_write_after_unblock) {
            serverAssert(!(c->ssdb_conn_flags & CONN_SUCCESS));
            confirmAndRetrySlaveSSDBwriteOp(c,-1,-1);
            server.send_failed_write_after_unblock = 0;
        }
    }
}

void signalBlockingKeyAsReady(redisDb *db, robj *key) {
    readyList *rl;

    /* No clients blocking for this key? No need to queue it. */
    if (dictFind(server.db->ssdb_blocking_keys,key) == NULL) return;

    /* Key was already signaled? No need to queue it again. */
    if (dictFind(db->ssdb_ready_keys,key) != NULL) return;

    /* Ok, we need to queue this key into server.ssdb_ready_keys. */
    rl = zmalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    listAddNodeTail(server.ssdb_ready_keys,rl);

    /* We also add the key in the db->ssdb_ready_keys dictionary in order
     * to avoid adding it multiple times into a list with a simple O(1)
     * check. */
    incrRefCount(key);
    serverAssert(dictAdd(db->ssdb_ready_keys,key,NULL) == DICT_OK);
    serverLog(LL_DEBUG, "singal key: %s, dbid: %d", (char *)key->ptr, db->id);
}

void addClientToListForBlockedKey(client *c, struct redisCommand* cmd, dict* blocked_dict, robj* keyobj) {
    dictEntry* de;
    list *l;

    de = dictFind(blocked_dict, keyobj);
    if (de == NULL) {
        int retval;

        l = listCreate();
        retval = dictAdd(blocked_dict, keyobj, l);
        serverLog(LL_DEBUG, "key: %s is added to %s.", (char *)keyobj->ptr,
                  blocked_dict == server.db[0].ssdb_blocking_keys ?
                  "ssdb_blocking_keys" : "blocking_keys_write_same_ssdbkey");
        incrRefCount(keyobj);
        serverAssertWithInfo(c, keyobj, retval == DICT_OK);
        serverLog(LL_DEBUG, "client fd: %d, cmd: %s, key: %s is blocked.",
                  c->fd, cmd->name, (char *)keyobj->ptr);
    } else {
        l = dictGetVal(de);
        serverLog(LL_DEBUG, "client fd: %d, cmd: %s, key: %s is already blocked by"
                          "another write on the same key.",
                  c->fd, cmd->name, (char *)keyobj->ptr);
    }

    listAddNodeTail(l, c);
}

void removeClientFromListForBlockedKey(client* c, dict* blocked_dict, robj* key) {
    /* Remove this client from the list of clients blocking on this key. */
    list *l = dictFetchValue(blocked_dict, key);
    serverAssert(l != NULL);
    listNode* node = listSearchKey(l,c);
    if (node) {
        listDelNode(l, node);
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (listLength(l) == 0) {
            serverLog(LL_DEBUG, "key: %s  is deleted from %s.", (char *)key->ptr,
                      blocked_dict == server.db[0].ssdb_blocking_keys ?
                      "ssdb_blocking_keys" : "blocking_keys_write_same_ssdbkey");
            serverAssert(dictDelete(blocked_dict, key) == DICT_OK);
        }
    }
}

client* removeFirstClientFromListForBlockedKey(dict* blocked_dict, robj* key) {
    /* Remove the first client from the list of clients blocking on this key. */
    list *l = dictFetchValue(blocked_dict, key);
    serverAssert(l != NULL);
    listNode* node = listFirst(l);
    if (node) {
        client* c = listNodeValue(node);
        listDelNode(l, node);
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (listLength(l) == 0) {
            serverLog(LL_DEBUG, "key: %s  is deleted from %s.", (char *)key->ptr,
                      blocked_dict == server.db[0].ssdb_blocking_keys ?
                      "ssdb_blocking_keys" : "blocking_keys_write_same_ssdbkey");
            serverAssert(dictDelete(blocked_dict, key) == DICT_OK);
        }
        return c;
    }
    return NULL;
}

int blockForLoadingkeys(client *c, struct redisCommand* cmd, robj **keys, int numkeys, mstime_t timeout) {
    dictEntry *de;
    list *l;
    int j, blockednum = 0;

    c->bpop.timeout = timeout;
    for (j = 0; j < numkeys; j++) {
        if (((cmd->flags & CMD_WRITE)
             && (dictFind(EVICTED_DATA_DB->transferring_keys, keys[j]->ptr)
                 || dictFind(EVICTED_DATA_DB->loading_hot_keys, keys[j]->ptr)
                 || dictFind(server.hot_keys, keys[j]->ptr)
                 || dictFind(EVICTED_DATA_DB->delete_confirm_keys, keys[j]->ptr))
            )
            ||
            ( (cmd->flags & CMD_READONLY)
              && (dictFind(EVICTED_DATA_DB->loading_hot_keys, keys[j]->ptr)
                  || dictFind(server.hot_keys, keys[j]->ptr)
                  || dictFind(EVICTED_DATA_DB->delete_confirm_keys, keys[j]->ptr))
            )
        ) {
            if (dictAdd(c->bpop.loading_or_transfer_keys, keys[j], NULL) != DICT_OK) continue;

            serverLog(LL_DEBUG, "key: %s is added to loading_or_transfer_keys.", (char *)keys[j]->ptr);

            incrRefCount(keys[j]);

            de = dictFind(server.db->ssdb_blocking_keys, keys[j]);
            if (de == NULL) {
                int retval;

                l = listCreate();
                retval = dictAdd(server.db->ssdb_blocking_keys, keys[j], l);
                serverLog(LL_DEBUG, "key: %s is added to ssdb_blocking_keys.",
                          (char *)keys[j]->ptr);
                incrRefCount(keys[j]);
                serverAssertWithInfo(c, keys[j], retval == DICT_OK);
                serverLog(LL_DEBUG, "client fd: %d, cmd: %s, key: %s is blocked.",
                          c->fd, cmd->name, (char *)keys[j]->ptr);
            } else {
                l = dictGetVal(de);
                serverLog(LL_DEBUG, "client fd: %d, cmd: %s, key: %s is already blocked.",
                                             c->fd, cmd->name, (char *)keys[j]->ptr);
            }

            listAddNodeTail(l, c);
            blockednum ++;
        }
    }

    if (blockednum) {
        blockClient(c, BLOCKED_SSDB_LOADING_OR_TRANSFER);
    }

    return blockednum;
}

void removeBlockedKeysFromTransferOrLoadingKeys(client* c) {
    dictIterator *di = dictGetIterator(c->bpop.loading_or_transfer_keys);
    dictEntry *de;
    int found = 1;

    while((de = dictNext(di)) != NULL) {
        robj * keyobj = dictGetKey(de);
        /* remove the key from transferring/loading keys, and signal */
        if (dictFind(EVICTED_DATA_DB->transferring_keys, keyobj->ptr))
            dictDelete(EVICTED_DATA_DB->transferring_keys, keyobj->ptr);
        else if (dictFind(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr))
            dictDelete(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr);
        else if (dictFind(server.hot_keys, keyobj->ptr))
            dictDelete(server.hot_keys, keyobj->ptr);
        else if (dictFind(EVICTED_DATA_DB->delete_confirm_keys, keyobj->ptr))
            dictDelete(EVICTED_DATA_DB->delete_confirm_keys, keyobj->ptr);
        else
            found = 0;

        if (found) {
            signalBlockingKeyAsReady(&server.db[0], keyobj);
            serverLog(LL_DEBUG, "key: %s is unblocked and deleted from loading/"
                    "transferring/delete_confirm_keys.", (char *)keyobj->ptr);
        }
        /* remove myself(this client) from blocked client list of this key, so we can
         * safely unblock this client and continue to process immediately. */
        removeClientFromListForBlockedKey(c, server.db->ssdb_blocking_keys, keyobj);
    }
    dictReleaseIterator(di);
    dictEmpty(c->bpop.loading_or_transfer_keys, NULL);
}

void transferringOrLoadingBlockedClientTimeOut(client *c) {
    removeBlockedKeysFromTransferOrLoadingKeys(c);

    /* process timeout case in our way. */
    unblockClient(c);
    if (c->flags & CLIENT_MASTER && server.slave_failed_retry_interrupted) {
        confirmAndRetrySlaveSSDBwriteOp(c, server.blocked_write_op->time, server.blocked_write_op->index);
    } else {
        if (c->flags & CLIENT_MASTER) {
            runCommand(c);
        } else {
            addReplyError(c, "timeout");
            resetClient(c);
        }
    }
}

/* must reply to SSDB avoid SSDB blocked. */
void ssdbRespDelCommand(client *c) {
    int numdel = 0;
    robj* keyobj = c->argv[1];
    dictEntry* de;

    preventCommandPropagation(c);

    if (!server.swap_mode) {
        addReplyErrorFormat(c,"Command only supported in swap-mode '%s'",
                            (char *)c->argv[0]->ptr);
        return;
    }

    if (!(de = dictFind(EVICTED_DATA_DB->transferring_keys, keyobj->ptr))) {
        addReplyError(c, "key is already unblocked");
        return;
    }
    long long resp_transfer_id;
    unsigned long long transfer_id = dictGetUnsignedIntegerVal(de);

    if (string2ll(c->argv[2]->ptr, sdslen(c->argv[2]->ptr), &resp_transfer_id) != 1 ||
            resp_transfer_id != (long long)transfer_id) {
        addReplyError(c, "transfer id is not match");
        return;;
    }

    if (server.is_doing_flushall) {
        addReplyError(c, "flushall is going");
        return;
    } else {
        if (epilogOfEvictingToSSDB(keyobj) == C_OK) {
            serverLog(LL_DEBUG, "ssdbRespDelCommand fd:%d key: %s dictDelete ok.",
                      c->fd, (char *)keyobj->ptr);
            numdel = 1;
        } else {
            /* the key is deleted when transfer is on-going.*/
            serverLog(LL_DEBUG, "ssdbRespDelCommand fd:%d key: %s is deleted when process transferring.",
                      c->fd, (char *)keyobj->ptr);
            numdel = 0;
        }
    }
    addReplyLongLong(c, numdel);
}

/* must reply to SSDB avoid SSDB blocked. */
void ssdbRespRestoreCommand(client *c) {
    robj * key = c->argv[1];
    long long old_dirty = server.dirty;
    dictEntry* de;
    serverAssert(c->db->id == 0 && c->argc == 6);

    preventCommandPropagation(c);

    if (!(de = dictFind(EVICTED_DATA_DB->loading_hot_keys, key->ptr))) {
        addReplyError(c, "key is already unblocked");
        return;
    }
    long long resp_transfer_id;
    unsigned long long transfer_id = dictGetUnsignedIntegerVal(de);

    if (string2ll(c->argv[5]->ptr, sdslen(c->argv[5]->ptr), &resp_transfer_id) != 1 ||
            resp_transfer_id != (long long)transfer_id) {
        addReplyError(c, "transfer id is not match");
        return;;
    }

    if (server.is_doing_flushall) {
        addReplyError(c, "flushall is going");
        return;
    } else {
        if (expireIfNeeded(EVICTED_DATA_DB, key)) {
            serverLog(LL_DEBUG, "key: %s is expired in redis.", (char *)key->ptr);
            if (dictDelete(EVICTED_DATA_DB->loading_hot_keys, key->ptr) == DICT_OK)
                signalBlockingKeyAsReady(c->db, key);
            addReplyError(c, "key expired");
            return;
        }

        /* remove transfer id before call restore command. */
        c->argc = 5;
        restoreCommand(c);

       /* Delete key from EVICTED_DATA_DB if restoreCommand is OK. */
        if (server.dirty == old_dirty + 1) {
            mstime_t when;
            dictEntry* ev_de = dictFind(EVICTED_DATA_DB->dict, c->argv[1]->ptr);
            dictEntry* de = dictFind(c->db->dict, c->argv[1]->ptr);
            robj *argv[5];

            /* copy lfu info when load ssdb key to redis.*/
            sds evdb_key = dictGetKey(ev_de);
            unsigned int lfu = sdsgetlfu(evdb_key);
            sds db_key = dictGetKey(de);
            sdssetlfu(db_key, lfu);

            when = getExpire(EVICTED_DATA_DB, c->argv[1]);

            /* remove the key from db 16 */
            dictDelete(EVICTED_DATA_DB->expires,key->ptr);
            dictDelete(EVICTED_DATA_DB->dict,key->ptr);

            /* propagate aof */
            argv[0] = createStringObject("restore", 7);
            memcpy(argv+1,c->argv+1,(c->argc-1) * sizeof(robj*));

            propagate(server.restoreCommand,c->db->id,argv,c->argc,PROPAGATE_AOF);
            decrRefCount(argv[0]);

            argv[0] = createStringObject("del",3);
            argv[1] = key;
            propagate(server.delCommand,EVICTED_DATA_DBID,argv,2,PROPAGATE_AOF);
            decrRefCount(argv[0]);

            /* Restore ttl info if needed. */
            if (when >= 0) {
                setExpire(c, server.db, c->argv[1], when);

                argv[0] = createStringObject("PEXPIREAT", 9);
                argv[1] = c->argv[1];
                argv[2] = createStringObjectFromLongLong(when);
                propagate(server.pexpireatCommand, 0, argv, 3, PROPAGATE_AOF);
                decrRefCount(argv[0]);
                decrRefCount(argv[2]);
            }

            // progate dumpfromssdb to slaves
            argv[0] = shared.dumpcmdobj;
            argv[1] = key;
            propagate(lookupCommand(shared.dumpcmdobj->ptr), 0, argv, 2, PROPAGATE_REPL);
            serverLog(LL_DEBUG, "ssdbRespRestoreCommand succeed.");
        } else
            serverLog(LL_WARNING, "ssdbRespRestoreCommand failed.");

        /* Queue the ready key to ssdb_ready_keys. and unblock the
         * clients blocked by the loading_hot key. */
        /* Restart redis will lose data in loading_hot_keys. */
        if (dictDelete(EVICTED_DATA_DB->loading_hot_keys, key->ptr) == DICT_OK) {
            signalBlockingKeyAsReady(c->db, key);
            serverLog(LL_DEBUG, "key: %s is deleted from loading_hot_keys.", (char *)key->ptr);
        }
        c->argc = 6;
    }
}

void ssdbRespNotfoundCommand(client *c) {
    dictEntry* de;
    robj *cmd = c->argv[1];
    robj *keyobj = c->argv[2];

    /* TODO: make sds vars shared. */
    sds fail_restore = sdsnew("ssdb-resp-restore");

    serverAssert(c->db->id == 0);

    preventCommandPropagation(c);

    if (!(de = dictFind(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr))) {
        addReplyError(c, "key is already unblocked");
        return;
    }
    long long resp_transfer_id;
    unsigned long long transfer_id = dictGetUnsignedIntegerVal(de);

    if (string2ll(c->argv[3]->ptr, sdslen(c->argv[3]->ptr), &resp_transfer_id) != 1 ||
            resp_transfer_id != (long long)transfer_id) {
        addReplyError(c, "transfer id is not match");
        return;;
    }

    if (!sdscmp(cmd->ptr, fail_restore)) {
        if (server.is_doing_flushall) {
            addReplyError(c, "flushall is going");
            return;
        } else {
            if (getExpire(EVICTED_DATA_DB, keyobj) != -1)
                dictDelete(EVICTED_DATA_DB->expires, keyobj->ptr);
            if (dictDelete(EVICTED_DATA_DB->dict, keyobj->ptr) == DICT_OK)
                serverLog(LL_DEBUG, "key: %s is deleted from EVICTED_DATA_DB->db.", (char *)keyobj->ptr);

            robj* tmpargv[2];
            robj* delCmdObj = createStringObject("del",3);
            tmpargv[0] = delCmdObj;
            tmpargv[1] = keyobj;

            /* propagate to delete key in evict db and in slave redis, at slave side, the key is maybe in
             * redis or ssdb, so we just use db 0 to make sure we can delete it. */
            propagate(server.delCommand,0,tmpargv,2,PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(tmpargv[0]);

            if (dictDelete(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr) == DICT_OK) {
                serverLog(LL_DEBUG, "key: %s is unblocked and deleted from loading_hot_keys.", (char *)keyobj->ptr);
                signalBlockingKeyAsReady(c->db, keyobj);
            }
        }
    } else {
        serverPanic("cmd is not supported.");
    }
    server.dirty ++;

    addReply(c, shared.ok);

    sdsfree(fail_restore);
}

void ssdbRespFailCommand(client *c) {
    dictEntry* de = NULL;
    robj *cmd = c->argv[1];
    robj *keyobj = c->argv[2];

    /* TODO: make sds vars shared. */
    sds fail_restore = sdsnew("ssdb-resp-restore");
    sds fail_dump = sdsnew("ssdb-resp-dump");

    preventCommandPropagation(c);

    if ((!sdscmp(cmd->ptr, fail_restore) && !(de = dictFind(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr))) ||
        (!sdscmp(cmd->ptr, fail_dump) && !(de = dictFind(EVICTED_DATA_DB->transferring_keys, keyobj->ptr)))) {
        addReplyError(c, "key is already unblocked");
        return;
    }

    serverAssert(de);
    long long resp_transfer_id;
    unsigned long long transfer_id = dictGetUnsignedIntegerVal(de);

    if (string2ll(c->argv[3]->ptr, sdslen(c->argv[3]->ptr), &resp_transfer_id) != 1 ||
            resp_transfer_id != (long long)transfer_id) {
        addReplyError(c, "transfer id is not match");
        return;;
    }

    serverAssert(c->db->id == 0);

    if (!sdscmp(cmd->ptr, fail_restore)) {
        if (dictDelete(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr) == DICT_OK) {
            signalBlockingKeyAsReady(c->db, keyobj);
            serverLog(LL_DEBUG, "key: %s is unblocked and deleted from loading_hot_keys.", (char *)keyobj->ptr);
        }
    } else if (!sdscmp(cmd->ptr, fail_dump)) {
        if (dictDelete(EVICTED_DATA_DB->transferring_keys, keyobj->ptr) == DICT_OK) {
            signalBlockingKeyAsReady(c->db, keyobj);
            serverLog(LL_DEBUG, "key: %s is unblocked and deleted from transferring_keys.", (char *)keyobj->ptr);
        }
    } else {
        serverPanic("cmd is not supported.");
    }
    addReply(c, shared.ok);

    sdsfree(fail_restore);
    sdsfree(fail_dump);
}

void storetossdbCommand(client *c) {
    robj *keyobj;
    int ret;

    preventCommandPropagation(c);

    if (!server.swap_mode) {
        addReplyErrorFormat(c,"Command only supported in swap-mode '%s'",
                            (char*)c->argv[0]->ptr);
        return;
    }

    keyobj = c->argv[1];

    if (c->argc != 2) {
        addReply(c, shared.syntaxerr);
        return;
    }

    if (dictFind(EVICTED_DATA_DB->transferring_keys, keyobj->ptr)) {
        addReplyError(c, "In transferring_keys.");
        server.cmdNotDone = 1;
        return;
    } else if (dictFind(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr)) {
        addReplyError(c, "In loading_hot_keys.");
        server.cmdNotDone = 1;
        return;
    } else if (dictFind(EVICTED_DATA_DB->visiting_ssdb_keys, keyobj->ptr)) {
        addReplyError(c, "In visiting_ssdb_keys.");
        server.cmdNotDone = 1;
        return;
    } else if (dictFind(EVICTED_DATA_DB->delete_confirm_keys, keyobj->ptr)) {
        addReplyError(c, "In delete_confirm_keys.");
        server.cmdNotDone = 1;
        return;
    }

    if (lookupKeyReadWithFlags(c->db, keyobj, LOOKUP_NOTOUCH) == NULL) {
        addReply(c, shared.nullbulk);
        /* The key is not existed any more. */
        serverLog(LL_DEBUG, "Not existed in redis. c->db->id:%d", c->db->id);
        return;
    }

    /* Try restoring the redis dumped data to SSDB. */
    if ((ret = prologOfEvictingToSSDB(keyobj, c->db)) != C_OK) {
        if (ret == C_FD_ERR)
            addReplyError(c,"ssdb connection for key transfer/load is disconnected");
        else if (ret == C_ERR)
            addReplyError(c,"key is expired or not exist");
        return;
    }

    addReply(c,shared.ok);
}

void dumpfromssdbCommand(client *c) {
    robj *keyobj = c->argv[1];

    if (!server.swap_mode) {
        addReplyErrorFormat(c,"Command only supported in swap-mode '%s'",
                            (char*)c->argv[0]->ptr);
        return;
    }

    if (dictFind(EVICTED_DATA_DB->transferring_keys, keyobj->ptr)) {
        addReplyError(c, "In transferring_keys.");
        server.cmdNotDone = 1;
        return;
    } else if (dictFind(EVICTED_DATA_DB->loading_hot_keys, keyobj->ptr)) {
        addReplyError(c, "In loading_hot_keys.");
        server.cmdNotDone = 1;
        return;
    } else if (dictFind(EVICTED_DATA_DB->visiting_ssdb_keys, keyobj->ptr)) {
        addReplyError(c, "In visiting_ssdb_keys.");
        server.cmdNotDone = 1;
        return;
    } else if (dictFind(EVICTED_DATA_DB->delete_confirm_keys, keyobj->ptr)) {
        addReplyError(c, "In delete_confirm_keys.");
        server.cmdNotDone = 1;
        return;
    }

    if ((lookupKeyReadWithFlags(EVICTED_DATA_DB, keyobj, LOOKUP_NOTOUCH)) == NULL) {
        /* The key is not existed any more. */
        addReply(c, shared.nullbulk);
        serverLog(LL_DEBUG, "Not existed in ssdb.");
        return;
    }

    prologOfLoadingFromSSDB(c, keyobj);
}

int isSSDBrespCmd(struct redisCommand *cmd) {
    if (server.swap_mode && cmd
            && (cmd->proc == ssdbRespDelCommand
                || cmd->proc == ssdbRespRestoreCommand
                || cmd->proc == ssdbRespFailCommand
                || cmd->proc == ssdbRespNotfoundCommand)) {
        return C_OK;
    } else
        return C_ERR;
}
