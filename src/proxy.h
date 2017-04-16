//
//  proxy.h
//  redis-server
//
//  Created by kent on 22/04/2017.
//  Copyright © 2017 kent. All rights reserved.
//

#ifndef _PROXY_H
#define _PROXY_H

#include "backend.h"
#include "cluster.h"

#define REDIS_PROXY_PORT 36379

/* Note: times are in milliseconds. */
#define PROXY_RECONNECT_PERIOD 1000
#define PROXY_DEFAULT_UPDATE_SLOT_MIN_LIMIT 1000
#define PROXY_DEFAULT_REDIRECT_CNT_MAX_LIMIT 3
#define PROXY_MAX_PENDING_COMMANDS 10000
#define PROXT_DEFAULT_POOLSIZE 1


typedef struct proxyRedisInstance {
    char *name; /* ip:port */
    char *ip;  /* IP address of redis */
    int port;                 /* port of redis */
    bkLink **pool;  /* Link pool to the instance. */
    int poolsize;       /* instance link pool size. */
    int connected_num;
    int disconnected_num;
    char *auth_pass;    /* Password to use for AUTH against master & slaves. */
    int slots_num;
} proxyRedisInstance;

/* Main proxy state. */
struct proxyState {
    char myid[CONFIG_RUN_ID_SIZE+1]; /* This proxy ID. */
    proxyRedisInstance *slots[CLUSTER_SLOTS];
    dict *instances;      /* Hash table of redis ip:port -> proxyRedisInstance structures */
    mstime_t update_slots_last_time;/* Last time we ran the cluster slots. */
    mstime_t update_slots_min_limit;/* min time limit ran the cluster slots. */
    int default_poolsize;
    int redirect_max_limit;
    list *backend_pending_write; /* There is to write or install handler. */
    int todo_before_sleep; /* Things to do in proxyBeforeSleep(). */
};

extern struct proxyState proxy;

void proxyDoBeforeSleep(int flags);

/* clusterState todo_before_sleep flags. */
#define PROXY_TODO_UPDATE_SLOT (1<<0)
#define PROXY_TODO_SAVE_CONFIG (1<<1)

#endif /* _PROXY_H */
