//
//  proxy.c
//  redis-server
//
//  Created by kent on 17/04/2017.
//  Copyright © 2017 kent. All rights reserved.
//

#include "server.h"
#include "backend.h"
#include "cluster.h"
#include "slowlog.h"
#include "proxy.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

/* ======================== Proxy global state =========================== */

struct proxyRedisInstance;
typedef struct proxyRedisInstance proxyRedisInstance;
struct backendLink;
typedef struct backendLink backendLink;
struct proxyAsyncCommand;
typedef struct proxyAsyncCommand proxyAsyncCommand;

struct proxyState proxy;

/* ============================= Prototypes ================================= */

void proxyBkLinkConnectCallback(bkLink *link);
void proxyBkDisconnectCallback(bkLink *link);
void proxyRouterCallback(bkLink *link, bkReply *reply, void *privdata);
void proxyFlushConfig(void);
void proxyReconnectBackendLink(proxyRedisInstance *pi);
bkLink *getBackendLinkBySlot(client *c, int slot);
bkLink *getBackendLinkByKey(client *c, sds key);

void proxySendAuthIfNeeded(bkLink *link);
void proxySetClientName(backendLink *link);
void proxySendAsking(backendLink *link);
void proxySendClusterNodes(backendLink *link);
void proxyUpdateClusterNodes();
void releaseProxyRedisInstance(proxyRedisInstance *pi);

static void _initBkLink(bkLink *link);
static bkLink *_createBkLink(proxyRedisInstance *pi);
void _proxySetSlot(int slot, proxyRedisInstance *pi);


/* ========================= Dictionary types =============================== */

unsigned int dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
void dictSdsDestructor(void *privdata, void *val);

void dictProxyRedisInstancesValDestructor(void *privdata, void *obj) {
    DICT_NOTUSED(privdata);
    releaseProxyRedisInstance(obj);
}

/* Instance ip:port (sds) -> proxyRedisInstance pointer. */
dictType proxyRedisInstancesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictProxyRedisInstancesValDestructor /* val destructor */
};

/* =========================== Initialization =============================== */

/* router command and callback*/
void proxyRouterNotSupportedCommand(client *c);
void proxyRouterNotImplementedCommand(client *c);
void proxyRouterSelectCommand(client *c);
void proxyRouterFirstKeyCommand(client *c);
void proxyRouterExistsCommand(client *c);
void proxyRouterDelCommand(client *c);
void proxyRouterMsetCommand(client *c);
void proxyRouterMgetCommand(client *c);

/* proxy specified cmds */
void proxyCommand(client *c);
void proxyInfoCommand(client *c);
void proxySetCommand(client *c);

struct redisCommand proxycmds[] = {
    /* special handler */
    {"select",proxyRouterSelectCommand,2,"lF",0,NULL,0,0,0,0,0},
    
    /* first key router cmds */
    {"get",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"set",proxyRouterFirstKeyCommand,-3,"wm",0,NULL,1,1,1,0,0},
    {"setnx",proxyRouterFirstKeyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"setex",proxyRouterFirstKeyCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"psetex",proxyRouterFirstKeyCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"append",proxyRouterFirstKeyCommand,3,"wm",0,NULL,1,1,1,0,0},
    {"strlen",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"setbit",proxyRouterFirstKeyCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"getbit",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"bitfield",proxyRouterFirstKeyCommand,-2,"wm",0,NULL,1,1,1,0,0},
    {"setrange",proxyRouterFirstKeyCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"getrange",proxyRouterFirstKeyCommand,4,"r",0,NULL,1,1,1,0,0},
    {"substr",proxyRouterFirstKeyCommand,4,"r",0,NULL,1,1,1,0,0},
    {"incr",proxyRouterFirstKeyCommand,2,"wmF",0,NULL,1,1,1,0,0},
    {"decr",proxyRouterFirstKeyCommand,2,"wmF",0,NULL,1,1,1,0,0},
    {"rpush",proxyRouterFirstKeyCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"lpush",proxyRouterFirstKeyCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"rpushx",proxyRouterFirstKeyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"lpushx",proxyRouterFirstKeyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"linsert",proxyRouterFirstKeyCommand,5,"wm",0,NULL,1,1,1,0,0},
    {"rpop",proxyRouterFirstKeyCommand,2,"wF",0,NULL,1,1,1,0,0},
    {"lpop",proxyRouterFirstKeyCommand,2,"wF",0,NULL,1,1,1,0,0},
    {"llen",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"lindex",proxyRouterFirstKeyCommand,3,"r",0,NULL,1,1,1,0,0},
    {"lset",proxyRouterFirstKeyCommand,4,"wm",0,NULL,1,1,1,0,0},
    {"lrange",proxyRouterFirstKeyCommand,4,"r",0,NULL,1,1,1,0,0},
    {"ltrim",proxyRouterFirstKeyCommand,4,"w",0,NULL,1,1,1,0,0},
    {"lrem",proxyRouterFirstKeyCommand,4,"w",0,NULL,1,1,1,0,0},
    {"rpoplpush",proxyRouterFirstKeyCommand,3,"wm",0,NULL,1,2,1,0,0},
    {"sadd",proxyRouterFirstKeyCommand,-3,"wmF",0,NULL,1,1,1,0,0},
    {"srem",proxyRouterFirstKeyCommand,-3,"wF",0,NULL,1,1,1,0,0},
    {"smove",proxyRouterFirstKeyCommand,4,"wF",0,NULL,1,2,1,0,0},
    {"sismember",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"scard",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"spop",proxyRouterFirstKeyCommand,-2,"wRF",0,NULL,1,1,1,0,0},
    {"srandmember",proxyRouterFirstKeyCommand,-2,"rR",0,NULL,1,1,1,0,0},
    {"sinter",proxyRouterFirstKeyCommand,-2,"rS",0,NULL,1,-1,1,0,0},
    {"sinterstore",proxyRouterFirstKeyCommand,-3,"wm",0,NULL,1,-1,1,0,0},
    {"sunion",proxyRouterFirstKeyCommand,-2,"rS",0,NULL,1,-1,1,0,0},
    {"sunionstore",proxyRouterFirstKeyCommand,-3,"wm",0,NULL,1,-1,1,0,0},
    {"sdiff",proxyRouterFirstKeyCommand,-2,"rS",0,NULL,1,-1,1,0,0},
    {"sdiffstore",proxyRouterFirstKeyCommand,-3,"wm",0,NULL,1,-1,1,0,0},
    {"smembers",proxyRouterFirstKeyCommand,2,"rS",0,NULL,1,1,1,0,0},
    {"sscan",proxyRouterFirstKeyCommand,-3,"rR",0,NULL,1,1,1,0,0},
    {"zadd",proxyRouterFirstKeyCommand,-4,"wmF",0,NULL,1,1,1,0,0},
    {"zincrby",proxyRouterFirstKeyCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"zrem",proxyRouterFirstKeyCommand,-3,"wF",0,NULL,1,1,1,0,0},
    {"zremrangebyscore",proxyRouterFirstKeyCommand,4,"w",0,NULL,1,1,1,0,0},
    {"zremrangebyrank",proxyRouterFirstKeyCommand,4,"w",0,NULL,1,1,1,0,0},
    {"zremrangebylex",proxyRouterFirstKeyCommand,4,"w",0,NULL,1,1,1,0,0},
    {"zunionstore",proxyRouterFirstKeyCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0},
    {"zinterstore",proxyRouterFirstKeyCommand,-4,"wm",0,zunionInterGetKeys,0,0,0,0,0},
    {"zrange",proxyRouterFirstKeyCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zrangebyscore",proxyRouterFirstKeyCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zrevrangebyscore",proxyRouterFirstKeyCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zrangebylex",proxyRouterFirstKeyCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zrevrangebylex",proxyRouterFirstKeyCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zcount",proxyRouterFirstKeyCommand,4,"rF",0,NULL,1,1,1,0,0},
    {"zlexcount",proxyRouterFirstKeyCommand,4,"rF",0,NULL,1,1,1,0,0},
    {"zrevrange",proxyRouterFirstKeyCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"zcard",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"zscore",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"zrank",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"zrevrank",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"zscan",proxyRouterFirstKeyCommand,-3,"rR",0,NULL,1,1,1,0,0},
    {"hset",proxyRouterFirstKeyCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"hsetnx",proxyRouterFirstKeyCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"hget",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"hmset",proxyRouterFirstKeyCommand,-4,"wm",0,NULL,1,1,1,0,0},
    {"hmget",proxyRouterFirstKeyCommand,-3,"r",0,NULL,1,1,1,0,0},
    {"hincrby",proxyRouterFirstKeyCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"hincrbyfloat",proxyRouterFirstKeyCommand,4,"wmF",0,NULL,1,1,1,0,0},
    {"hdel",proxyRouterFirstKeyCommand,-3,"wF",0,NULL,1,1,1,0,0},
    {"hlen",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"hstrlen",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"hkeys",proxyRouterFirstKeyCommand,2,"rS",0,NULL,1,1,1,0,0},
    {"hvals",proxyRouterFirstKeyCommand,2,"rS",0,NULL,1,1,1,0,0},
    {"hgetall",proxyRouterFirstKeyCommand,2,"r",0,NULL,1,1,1,0,0},
    {"hexists",proxyRouterFirstKeyCommand,3,"rF",0,NULL,1,1,1,0,0},
    {"hscan",proxyRouterFirstKeyCommand,-3,"rR",0,NULL,1,1,1,0,0},
    {"incrby",proxyRouterFirstKeyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"decrby",proxyRouterFirstKeyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"incrbyfloat",proxyRouterFirstKeyCommand,3,"wmF",0,NULL,1,1,1,0,0},
    {"getset",proxyRouterFirstKeyCommand,3,"wm",0,NULL,1,1,1,0,0},
    {"expire",proxyRouterFirstKeyCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"expireat",proxyRouterFirstKeyCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"pexpire",proxyRouterFirstKeyCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"pexpireat",proxyRouterFirstKeyCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"type",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"sort",proxyRouterFirstKeyCommand,-2,"wm",0,sortGetKeys,1,1,1,0,0},
    {"ttl",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"touch",proxyRouterFirstKeyCommand,-2,"rF",0,NULL,1,1,1,0,0},
    {"pttl",proxyRouterFirstKeyCommand,2,"rF",0,NULL,1,1,1,0,0},
    {"persist",proxyRouterFirstKeyCommand,2,"wF",0,NULL,1,1,1,0,0},
    {"dump",proxyRouterFirstKeyCommand,2,"r",0,NULL,1,1,1,0,0},
    {"object",proxyRouterFirstKeyCommand,3,"r",0,NULL,2,2,2,0,0},
    {"eval",proxyRouterFirstKeyCommand,-3,"s",0,evalGetKeys,0,0,0,0,0},
    {"evalsha",proxyRouterFirstKeyCommand,-3,"s",0,evalGetKeys,0,0,0,0,0},
    {"bitcount",proxyRouterFirstKeyCommand,-2,"r",0,NULL,1,1,1,0,0},
    {"bitpos",proxyRouterFirstKeyCommand,-3,"r",0,NULL,1,1,1,0,0},
    {"geoadd",proxyRouterFirstKeyCommand,-5,"wm",0,NULL,1,1,1,0,0},
    {"georadius",proxyRouterFirstKeyCommand,-6,"w",0,NULL,1,1,1,0,0},
    {"georadiusbymember",proxyRouterFirstKeyCommand,-5,"w",0,NULL,1,1,1,0,0},
    {"geohash",proxyRouterFirstKeyCommand,-2,"r",0,NULL,1,1,1,0,0},
    {"geopos",proxyRouterFirstKeyCommand,-2,"r",0,NULL,1,1,1,0,0},
    {"geodist",proxyRouterFirstKeyCommand,-4,"r",0,NULL,1,1,1,0,0},
    {"pfadd",proxyRouterFirstKeyCommand,-2,"wmF",0,NULL,1,1,1,0,0},
    {"pfcount",proxyRouterFirstKeyCommand,-2,"r",0,NULL,1,-1,1,0,0},
    {"pfmerge",proxyRouterFirstKeyCommand,-2,"wm",0,NULL,1,-1,1,0,0},
    
    /* multiply key router cmds */
    {"del",proxyRouterDelCommand,-2,"w",0,NULL,1,-1,1,0,0},
    {"exists",proxyRouterExistsCommand,-2,"rF",0,NULL,1,-1,1,0,0},
    {"mget",proxyRouterMgetCommand,-2,"r",0,NULL,1,-1,1,0,0},
    {"mset",proxyRouterMsetCommand,-3,"wm",0,NULL,1,-1,2,0,0},
    
    /* no router cmds */
    {"ping",pingCommand,-1,"tF",0,NULL,0,0,0,0,0},
    {"echo",echoCommand,2,"F",0,NULL,0,0,0,0,0},
    {"auth",authCommand,2,"sltF",0,NULL,0,0,0,0,0},
    {"readonly",readonlyCommand,1,"F",0,NULL,0,0,0,0,0},
    {"readwrite",readwriteCommand,1,"F",0,NULL,0,0,0,0,0},
    {"time",timeCommand,1,"RF",0,NULL,0,0,0,0,0},
    {"wait",waitCommand,3,"s",0,NULL,0,0,0,0,0},
    {"command",commandCommand,0,"lt",0,NULL,0,0,0,0,0},
    
    /* admin cmds */
    {"shutdown",shutdownCommand,-1,"alt",0,NULL,0,0,0,0,0},
    {"slowlog",slowlogCommand,-2,"a",0,NULL,0,0,0,0,0},
    {"debug",debugCommand,-1,"as",0,NULL,0,0,0,0,0},
    {"config",configCommand,-2,"lat",0,NULL,0,0,0,0,0},
    {"client",clientCommand,-2,"as",0,NULL,0,0,0,0,0},
    {"latency",latencyCommand,-2,"aslt",0,NULL,0,0,0,0,0},
    {"monitor",monitorCommand,1,"as",0,NULL,0,0,0,0,0},
    
    /* proxy specified cmds */
    {"proxy",proxyCommand,-2,"lt",0,NULL,0,0,0,0,0},
    {"info",proxyInfoCommand,-1,"lt",0,NULL,0,0,0,0,0},
    
    /* not supported cmds */
    {"keys",proxyRouterNotSupportedCommand,2,"rS",0,NULL,0,0,0,0,0},
    {"move",proxyRouterNotSupportedCommand,3,"wF",0,NULL,1,1,1,0,0},
    {"randomkey",proxyRouterNotSupportedCommand,1,"rR",0,NULL,0,0,0,0,0},
    {"scan",proxyRouterNotSupportedCommand,-2,"rR",0,NULL,0,0,0,0,0},
    {"dbsize",proxyRouterNotSupportedCommand,1,"rF",0,NULL,0,0,0,0,0},
    {"rename",proxyRouterNotSupportedCommand,3,"w",0,NULL,1,2,1,0,0},
    {"renamenx",proxyRouterNotSupportedCommand,3,"wF",0,NULL,1,2,1,0,0},
    {"bitop",proxyRouterNotSupportedCommand,-4,"wm",0,NULL,2,-1,1,0,0},
    {"msetnx",proxyRouterNotSupportedCommand,-3,"wm",0,NULL,1,-1,2,0,0},
    {"migrate",proxyRouterNotSupportedCommand,-6,"w",0,migrateGetKeys,0,0,0,0,0},
    {"asking",proxyRouterNotSupportedCommand,1,"F",0,NULL,0,0,0,0,0},
    {"restore",proxyRouterNotSupportedCommand,-4,"wm",0,NULL,1,1,1,0,0},
    {"restore-asking",proxyRouterNotSupportedCommand,-4,"wmk",0,NULL,1,1,1,0,0},
    {"brpop",proxyRouterNotSupportedCommand,-3,"ws",0,NULL,1,1,1,0,0},
    {"brpoplpush",proxyRouterNotSupportedCommand,4,"wms",0,NULL,1,2,1,0,0},
    {"blpop",proxyRouterNotSupportedCommand,-3,"ws",0,NULL,1,-2,1,0,0},
    {"subscribe",proxyRouterNotSupportedCommand,-2,"pslt",0,NULL,0,0,0,0,0},
    {"unsubscribe",proxyRouterNotSupportedCommand,-1,"pslt",0,NULL,0,0,0,0,0},
    {"psubscribe",proxyRouterNotSupportedCommand,-2,"pslt",0,NULL,0,0,0,0,0},
    {"punsubscribe",proxyRouterNotSupportedCommand,-1,"pslt",0,NULL,0,0,0,0,0},
    {"publish",proxyRouterNotSupportedCommand,3,"pltF",0,NULL,0,0,0,0,0},
    {"pubsub",proxyRouterNotSupportedCommand,-2,"pltR",0,NULL,0,0,0,0,0},
    {"multi",proxyRouterNotSupportedCommand,1,"sF",0,NULL,0,0,0,0,0},
    {"exec",proxyRouterNotSupportedCommand,1,"sM",0,NULL,0,0,0,0,0},
    {"discard",proxyRouterNotSupportedCommand,1,"sF",0,NULL,0,0,0,0,0},
    {"watch",proxyRouterNotSupportedCommand,-2,"sF",0,NULL,1,-1,1,0,0},
    {"unwatch",proxyRouterNotSupportedCommand,1,"sF",0,NULL,0,0,0,0,0},
    {"script",proxyRouterNotSupportedCommand,-2,"s",0,NULL,0,0,0,0,0},
    {"save",proxyRouterNotSupportedCommand,1,"as",0,NULL,0,0,0,0,0},
    {"bgsave",proxyRouterNotSupportedCommand,-1,"a",0,NULL,0,0,0,0,0},
    {"bgrewriteaof",proxyRouterNotSupportedCommand,1,"a",0,NULL,0,0,0,0,0},
    {"flushdb",proxyRouterNotSupportedCommand,1,"w",0,NULL,0,0,0,0,0},
    {"flushall",proxyRouterNotSupportedCommand,1,"w",0,NULL,0,0,0,0,0},
    {"sync",proxyRouterNotSupportedCommand,1,"ars",0,NULL,0,0,0,0,0},
    {"psync",proxyRouterNotSupportedCommand,3,"ars",0,NULL,0,0,0,0,0},
    {"replconf",proxyRouterNotSupportedCommand,-1,"aslt",0,NULL,0,0,0,0,0},
    {"lastsave",proxyRouterNotSupportedCommand,1,"RF",0,NULL,0,0,0,0,0},
    {"slaveof",proxyRouterNotSupportedCommand,3,"ast",0,NULL,0,0,0,0,0},
    {"cluster",proxyRouterNotSupportedCommand,-2,"a",0,NULL,0,0,0,0,0},
    {"role",proxyRouterNotSupportedCommand,1,"lst",0,NULL,0,0,0,0,0},
    {"pfdebug",proxyRouterNotSupportedCommand,-3,"w",0,NULL,0,0,0,0,0},
    {"pfselftest",proxyRouterNotSupportedCommand,1,"a",0,NULL,0,0,0,0,0},
};

/* This function overwrites a few normal Redis config default with Proxy
 * specific defaults. */
void initProxyConfig(void) {
    server.port = REDIS_PROXY_PORT;
}

/* Perform the initProxy mode initialization. */
void initProxy(void) {
    unsigned int j;
    
    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command. */
    dictEmpty(server.commands,NULL);
    for (j = 0; j < sizeof(proxycmds)/sizeof(proxycmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = proxycmds+j;
        
        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);
    }
    
    /* Initialize various data structures. */
    proxy.instances = dictCreate(&proxyRedisInstancesDictType, NULL);
    proxy.update_slots_last_time = 0;
    proxy.update_slots_min_limit = PROXY_DEFAULT_UPDATE_SLOT_MIN_LIMIT;
    proxy.redirect_max_limit = PROXY_DEFAULT_REDIRECT_CNT_MAX_LIMIT;
    proxy.default_poolsize = PROXT_DEFAULT_POOLSIZE;
    memset(proxy.slots,0,sizeof(proxy.slots));
    memset(proxy.myid,0,sizeof(proxy.myid));
    proxy.backend_pending_write = listCreate();
    proxy.todo_before_sleep = 0;
}

/* This function gets called when the server is in Proxy mode, started,
 * loaded the configuration, and is ready for normal operations. */
void proxyIsRunning(void) {
    int j;
    
    if (server.configfile == NULL) {
        serverLog(LL_WARNING,
                  "Proxy started without a config file. Exiting...");
        exit(1);
    } else if (access(server.configfile,W_OK) == -1) {
        serverLog(LL_WARNING,
                  "Proxy config file %s is not writable: %s. Exiting...",
                  server.configfile,strerror(errno));
        exit(1);
    }
    
    /* If this Proxy has yet no ID set in the configuration file, we
     * pick a random one and persist the config on disk. From now on this
     * will be this Proxy ID across restarts. */
    for (j = 0; j < CONFIG_RUN_ID_SIZE; j++)
        if (proxy.myid[j] != 0) break;
    
    if (j == CONFIG_RUN_ID_SIZE) {
        /* Pick ID and presist the config. */
        getRandomHexChars(proxy.myid,CONFIG_RUN_ID_SIZE);
        proxyDoBeforeSleep(PROXY_TODO_SAVE_CONFIG);
    }
    
    /* Log its ID to make debugging of issues simpler. */
    serverLog(LL_NOTICE,"Proxy ID is %s", proxy.myid);
    
    /* backend connection attach to server event loop. */
    dictIterator *di = dictGetIterator(proxy.instances);
    dictEntry *de;
    while((de = dictNext(di)) != NULL) {
        proxyRedisInstance *pi = dictGetVal(de);
        for (j = 0; j < pi->poolsize; j++) {
            _initBkLink(pi->pool[j]);
        }
    }
    dictReleaseIterator(di);
    
    /* update cluster nodes. */
    proxyDoBeforeSleep(PROXY_TODO_UPDATE_SLOT);
    
    /* slot random */
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        _proxySetSlot(j, dictGetVal(dictGetRandomKey(proxy.instances)));
    }
}

/* =============================== backendLink ============================= */

/* Hiredis connection established / disconnected callbacks. We need them
 * just to cleanup our link state. */
void proxyBkLinkConnectCallback(bkLink *link) {
    serverAssert(!(link->flags & BACKEND_ERR));
    proxyRedisInstance *pi = link->data;
    pi->disconnected_num--;
    pi->connected_num++;
    
    serverLog(LL_NOTICE, "+backend-link-connected %s",link->name);
}

void proxyBkDisconnectCallback(bkLink *link) {
    serverAssert(link->flags & BACKEND_ERR);
    proxyRedisInstance *pi = link->data;
    pi->disconnected_num++;
    pi->connected_num--;
    
    serverLog(LL_NOTICE, "-backend-link-disconnected %s", link->name);
}

static sds getNameByIpPort(char *ip, int port) {
    char buf[NET_PEER_ID_LEN];
    anetFormatAddr(buf,sizeof(buf),ip,port);
    return sdsnew(buf);
}

static bkLink *_createBkLink(proxyRedisInstance *pi) {
    bkLink *link = bkConnectBind(pi->ip, pi->port, NET_FIRST_BIND_ADDR);
    link->data = pi;
    bkSetConnectCallback(link, proxyBkLinkConnectCallback);
    bkSetDisconnectCallback(link, proxyBkDisconnectCallback);
    return link;
}

static void _initBkLink(bkLink *link) {
    bkAttachEventLoop(link, server.el);
    proxySendAuthIfNeeded(link);
    proxySetClientName(link);
}

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master with the same name, a slave with the same address, or a sentinel
 * with the same ID already exists. */
static proxyRedisInstance *_createProxyRedisInstance(char *hostname, int port, int poolsize) {
    proxyRedisInstance *pi;
    sds name;
    char ip[NET_IP_STR_LEN];
    
    if (port < 0 || port > 65535 || poolsize <= 0) {
        errno = EINVAL;
        return NULL;
    }
    if (anetResolve(NULL,hostname,ip,sizeof(ip)) == ANET_ERR) {
        errno = ENOENT;
        return NULL;
    }
    
    name = getNameByIpPort(hostname, port);
    if (dictFind(proxy.instances, name)) {
        sdsfree(name);
        errno = EBUSY;
        return NULL;
    }
    
    pi = zmalloc(sizeof(*pi));
    pi->auth_pass=NULL;
    pi->name = name;
    pi->ip = sdsnew(ip);
    pi->port = port;
    pi->connected_num = 0;
    pi->disconnected_num = 0;
    pi->poolsize = poolsize;
    pi->pool = zmalloc(poolsize * sizeof(backendLink *));
    for (int i = 0; i < poolsize; i++) {
        pi->pool[i] = _createBkLink(pi);
    }
    pi->slots_num = 0;
    
    /* Add into the right table. */
    dictAdd(proxy.instances, sdsdup(name), pi);
    proxyDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
    serverLog(LL_NOTICE, "+proxy-router-add %s", name);
    return pi;
}

proxyRedisInstance *createProxyRedisInstance(char *hostname, int port, int poolsize) {
    proxyRedisInstance *pi;
    pi = _createProxyRedisInstance(hostname, port, poolsize);
    for (int j = 0; j < poolsize; j++) {
        _initBkLink(pi->pool[j]);
    }
    return pi;
}

proxyRedisInstance *createProxyRedisInstanceByAddr(const char *addr, int poolsize) {
    sds *argv = NULL;
    int argc = 0, port;
    proxyRedisInstance *pi = NULL;
    
    argv = sdssplitlen(addr, (int) strlen(addr), ":", 1, &argc);
    if (argc != 2) goto err;
    
    port = atoi(argv[1]);
    pi = createProxyRedisInstance(argv[0], port, poolsize);
    
err:
    sdsfreesplitres(argv, argc);
    return pi;
}

/* Release this instance and all its hiredis connections. */
void releaseProxyRedisInstance(proxyRedisInstance *pi) {
    serverLog(LL_NOTICE, "-proxy-router-delete %s", pi->name);
    serverAssert(pi->slots_num == 0);
    for (int i = 0; i < pi->poolsize; i++) {
        bkLinkFree(pi->pool[i]);
    }
    zfree(pi->pool);
    
    sdsfree(pi->name);
    sdsfree(pi->auth_pass);
    sdsfree(pi->ip);
    
    zfree(pi);
    proxyDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
}

/* Lookup a proxy Redis instance, by ip and port. */
proxyRedisInstance *getProxyRedisInstanceByIpPort(char *ip, int port)
{
    proxyRedisInstance *inst;
    sds name;
    name = getNameByIpPort(ip, port);
    inst = dictFetchValue(proxy.instances, name);
    sdsfree(name);
    return inst;
}

/* Lookup a proxy Redis instance, by ip and port. */
proxyRedisInstance *getProxyRedisInstanceByAddr(const char *addr)
{
    sds *argv = NULL;
    int argc = 0, port;
    proxyRedisInstance *pi = NULL;
    
    argv = sdssplitlen(addr, (int) strlen(addr), ":", 1, &argc);
    if (argc != 2) goto err;
    
    port = atoi(argv[1]);
    pi = getProxyRedisInstanceByIpPort(argv[0], port);
    
err:
    sdsfreesplitres(argv, argc);
    return pi;
}

proxyRedisInstance *getOrCreateProxyRedisInstanceByAddr(const char *addr)
{
    proxyRedisInstance *pi = NULL;
    
    pi = getProxyRedisInstanceByAddr(addr);
    if (!pi) {
        pi = createProxyRedisInstanceByAddr(addr, proxy.default_poolsize);
    }
    return pi;
}

/* This function remove the Proxy with the specified addr.
 *
 * The function returns 1 if the matching Proxy was removed, otherwise
 * 0 if there was no Proxy with this addr. */
int removeProxyRedisInstanceIpPort(char *ip, int port) {
    int removed = 0;
    sds name;
    
    name = getNameByIpPort(ip, port);
    if (dictFetchValue(proxy.instances, name)) {
        dictDelete(proxy.instances, name);
        removed++;
    }
    
    sdsfree(name);
    return removed;
}

/* Create the async connections for the instance link if the link
 * is disconnected. */
void proxyReconnectBackendLink(proxyRedisInstance *pi) {
    if (pi->port == 0) return; /* port == 0 means invalid address. */
    for (int i = 0; i < pi->poolsize; i++) {
        bkLink *link = pi->pool[i];
        
        if (link->flags & BACKEND_ERR) {
            mstime_t now = mstime();
            if (now - link->conn_time < PROXY_RECONNECT_PERIOD) return;
            
            bkLinkFree(link);
            link = _createBkLink(pi);
            _initBkLink(link);
            pi->pool[i] = link;
            serverLog(LL_NOTICE, "-backend-link-reconnection %s", link->name);
        }
    }
}

proxyRedisInstance * _proxyGetBySlot(int slot) {
    return proxy.slots[slot];
}

void _proxySetSlot(int slot, proxyRedisInstance *pi) {
    if (proxy.slots[slot])
        proxy.slots[slot]->slots_num--;
    
    pi->slots_num++;
    proxy.slots[slot] = pi;
}

void proxyClearUnusedInstance() {
    dictIterator *di;
    dictEntry *de;
    /* For every master emit a "proxy router" config entry. */
    di = dictGetIterator(proxy.instances);
    while((de = dictNext(di)) != NULL) {
        proxyRedisInstance *pi;
        pi = dictGetVal(de);
        if (pi->slots_num == 0) {
            dictDelete(proxy.instances, dictGetKey(de));
        }
    }
    
    dictReleaseIterator(di);
}

/* random select one and send cluster nodes. */
void proxyUpdateClusterNodes() {
    if (mstime() - proxy.update_slots_last_time < proxy.update_slots_min_limit)
        return;
    dictEntry *de = dictGetRandomKey(proxy.instances);
    proxyRedisInstance *pi = dictGetVal(de);
    proxySendClusterNodes(pi->pool[0]);
    proxy.update_slots_last_time = mstime();
}

/* ============================ Config handling ============================= */
char *proxyHandleConfiguration(char **argv, int argc) {
    proxyRedisInstance *pi;
    
    proxyDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
    
    if (!strcasecmp(argv[0],"router") && argc >= 3) {
        /* router <host> <port> [<poolsize>] */
        int port = atoi(argv[2]);
        if (port < 0 || port > 65535) {
            return "port must between 0 and 65535.";
        }
        if (argc == 4) {
            pi = _createProxyRedisInstance(argv[1], port, atoi(argv[3]));
        } else {
            pi = _createProxyRedisInstance(argv[1], port, 1);
        }
        if (pi == NULL) {
            switch(errno) {
                case EBUSY: return "Duplicated master name.";
                case ENOENT: return "Can't resolve master instance hostname.";
                case EINVAL: return "Invalid port number";
            }
        }
    } else if (!strcasecmp(argv[0],"myid") && argc == 2) {
        if (strlen(argv[1]) != CONFIG_RUN_ID_SIZE)
            return "Malformed Sentinel id in myid option.";
        memcpy(proxy.myid,argv[1],CONFIG_RUN_ID_SIZE);
    } else if (!strcasecmp(argv[0],"auth-pass") && argc == 3) {
        /* auth-pass <host> <port> <password> */
        pi = getProxyRedisInstanceByIpPort(argv[1], atoi(argv[2]));
        if (!pi) return "No such master with specified name.";
        pi->auth_pass = sdsnew(argv[2]);
    } else {
        return "Unrecognized proxy configuration statement.";
    }
    return NULL;
}

/* Implements CONFIG REWRITE for "proxy" option.
 * This is used to rewrite the configuration given by the user
 * (the configured redis instances). */
void rewriteConfigProxyOption(struct rewriteConfigState *state) {
    dictIterator *di;
    dictEntry *de;
    sds line;
    
    /* proxy unique ID. */
    line = sdscatprintf(sdsempty(), "proxy myid %s", proxy.myid);
    rewriteConfigRewriteLine(state,"proxy",line,1);
    
    /* For every master emit a "proxy router" config entry. */
    di = dictGetIterator(proxy.instances);
    while((de = dictNext(di)) != NULL) {
        proxyRedisInstance *pi;
        pi = dictGetVal(de);
        line = sdscatprintf(sdsempty(),"proxy router %s %d %d",
                            pi->ip, pi->port, pi->poolsize);
        rewriteConfigRewriteLine(state,"proxy",line,1);
        
        /* proxy auth-pass */
        if (pi->auth_pass) {
            line = sdscatprintf(sdsempty(),
                                "proxy auth-pass %s %d %s",
                                pi->ip, pi->port, pi->auth_pass);
            rewriteConfigRewriteLine(state,"proxy",line,1);
        }
    }
    
    dictReleaseIterator(di);
}

/* This function uses the config rewriting Redis engine in order to persist
 * the state of the Proxy in the current configuration file.
 *
 * Before returning the function calls fsync() against the generated
 * configuration file to make sure changes are committed to disk.
 *
 * On failure the function logs a warning on the Redis log. */
void proxyFlushConfig(void) {
    int fd = -1;
    int saved_hz = server.hz;
    int rewrite_status;
    
    server.hz = CONFIG_DEFAULT_HZ;
    rewrite_status = rewriteConfig(server.configfile);
    server.hz = saved_hz;
    
    if (rewrite_status == -1) goto werr;
    if ((fd = open(server.configfile,O_RDONLY)) == -1) goto werr;
    if (fsync(fd) == -1) goto werr;
    if (close(fd) == EOF) goto werr;
    
    return;
    
werr:
    if (fd != -1) close(fd);
    serverLog(LL_WARNING,"WARNING: Proxy was not able to save the new configuration on disk!!!: %s", strerror(errno));
}

/* ====================== proxy cmds to backend ======================= */

/* Send the AUTH command with the specified master password if needed.
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails will detect the instance down,
 * will disconnect and reconnect the link and so forth. */
void proxySendAuthIfNeeded(bkLink *link) {
    proxyRedisInstance *pi = link->data;
    if (pi->auth_pass) {
        bkAddReplyMultiBulkLen(link, 2);
        bkAddRequestBulkCString(link, "AUTH");
        bkAddRequestBulkCString(link, pi->auth_pass);
        bkAddCallback(link, NULL, NULL);
    }
}

/* Use CLIENT SETNAME to name the connection in the Redis instance as
 * proxy-<first_8_chars_of_runid>-<connection_type>
 *
 * This makes it possible to list all the proxy instances connected
 * to a Redis servewr with CLIENT LIST, grepping for a specific name format. */
void proxySetClientName(backendLink *link) {
    char name[64];
    
    snprintf(name,sizeof(name),"proxy-%s", link->name);
    bkAddReplyMultiBulkLen(link, 3);
    bkAddRequestBulkCString(link, "CLIENT");
    bkAddRequestBulkCString(link, "SETNAME");
    bkAddRequestBulkCString(link, name);
    bkAddCallback(link, NULL, NULL);
}

/* used in redirection error received. */
void proxySendAsking(backendLink *link) {
    bkAddReplyMultiBulkLen(link, 1);
    bkAddRequestBulkCString(link, "ASKING");
    bkAddCallback(link, NULL, NULL);
}

/* update slots info */
void proxyClusterNodesCallback(bkLink *link, bkReply *r, void *privdata) {
    UNUSED(privdata);
    int linenum = 0, totlines, i, j;
    sds *lines;
    
    if (r->type == PROTO_REPLY_ERROR) {
        serverLog(LL_WARNING, "-cluster-nodes-error: -%s", r->str);
        return;
    }
    if (r->type != PROTO_REPLY_STRING) {
        serverLog(LL_WARNING, "-cluster-nodes-error: -unrecognized return type: %d", r->type);
        return;
    }
    
    serverLog(LL_VERBOSE, "%s", r->str);
    
    lines = sdssplitlen(r->str, (int) r->len, "\n", 1, &totlines);
    for (i = 0; i < totlines; i++) {
        sds *argv;
        int argc;
        proxyRedisInstance *pi;
        
        linenum = i+1;
        lines[i] = sdstrim(lines[i]," \t\r\n");
        
        /* Skip comments and blank lines */
        if (lines[i][0] == '#' || lines[i][0] == '\0') continue;
        
        /* skip
         * 1: argument not valid
         * 2: slave
         */
        argv = sdssplitargs(lines[i],&argc);
        if (argc < 8 ||
            argv == NULL ||
            sdslen(argv[0]) != CLUSTER_NAMELEN ||
            strstr(argv[2], "slave"))
            goto end;
        
        // myself
        if (strstr(argv[2], "myself")) {
            pi = link->data;
        } else {
            pi = getOrCreateProxyRedisInstanceByAddr(argv[1]);
            if (!pi){
                char *msg;
                switch(errno) {
                    case EBUSY:
                        msg = "Duplicated master name";
                        break;
                    case EINVAL:
                        msg = "Invalid port number";
                        break;
                    default:
                        msg = "Unspecified error adding the instance";
                        break;
                }
                serverLog(LL_WARNING, "-backend-link-new failed %s %s", argv[1], msg);
                goto end;
            }
        }
        
        /* Populate hash slots served by this instance. */
        for (j = 8; j < argc; j++) {
            int start, stop;
            char *p;
            
            if (argv[j][0] == '[') {
                /* Here we handle migrating / importing slots */
                continue;
            } else if ((p = strchr(argv[j],'-')) != NULL) {
                *p = '\0';
                start = atoi(argv[j]);
                stop = atoi(p+1);
            } else {
                start = stop = atoi(argv[j]);
            }
            while(start <= stop) _proxySetSlot(start++, pi);
        }

    end:
        sdsfreesplitres(argv,argc);
    }
    
    sdsfreesplitres(lines,totlines);
    
    /* remove unused instances. */
    proxyClearUnusedInstance();
}

/* used in redirection error received. */
void proxySendClusterNodes(backendLink *link) {
    bkAddReplyMultiBulkLen(link, 2);
    bkAddRequestBulkCString(link, "CLUSTER");
    bkAddRequestBulkCString(link, "NODES");
    bkAddCallback(link, proxyClusterNodesCallback, NULL);
}

/* ======================== Proxy handle requst ======================== */

typedef void (redisReplyCoalesceFn)(client *c, proxyAsyncCommand **children, int children_num);

typedef struct proxyAsyncCommand{
    /* basic info */
    int refcount;
    client *c;
    int argc;
    robj **argv;
    bkReply *reply;
    
    /* stat info */
    mstime_t start;
    int redirect_cnt;
    
    /* this is for splited commands */
    int children_num;
    proxyAsyncCommand **children;
    int children_finished_num;
    struct proxyAsyncCommand *parent;
    redisReplyCoalesceFn *fn;
} proxyAsyncCommand;


proxyAsyncCommand *createProxyAsyncCommandWithoutArg(client *c) {
    proxyAsyncCommand *cmd = zmalloc(sizeof(*cmd));
    
    /* basic info */
    cmd->refcount = 1;
    cmd->c = c;
    cmd->argc = 0;
    cmd->argv = NULL;
    cmd->reply = NULL;
    
    /* stat info */
    cmd->start = mstime();
    cmd->redirect_cnt = 0;
    
    /* this is for splited commands */
    cmd->children = NULL;
    cmd->children_finished_num = 0;
    cmd->children_num = 0;
    cmd->parent = NULL;
    cmd->fn = NULL;
    return cmd;
}

proxyAsyncCommand *createProxyAsyncCommand(client *c) {
    int j;
    proxyAsyncCommand *cmd = createProxyAsyncCommandWithoutArg(c);
    cmd->argc = c->argc;
    cmd->argv = zmalloc(sizeof(robj*)*c->argc);
    for (j = 0; j < c->argc; j++) {
        incrRefCount(c->argv[j]);
        cmd->argv[j] = c->argv[j];
    }
    return cmd;
}

/* use this function when push cmd to hiredis, as cmd is accturelly 
 * reffered from c->request list and hiredis replies list. */
void incrProxyAsyncCommandRefCount(void *ptr) {
    proxyAsyncCommand *cmd = ptr;
    cmd->refcount++;
}

/* used to free cmd object, this called when free cmd list and 
 * called back from hiredis. this should be called every time 
 * registered into hiredis. */
static void _decrProxyAsyncCommandRefCount(void *ptr, int clean_client) {
    int j;
    proxyAsyncCommand *cmd = ptr;
    
    if (cmd->refcount <= 0) serverPanic("decrProxyAsyncCommandRefCount against refcount <= 0");
    if (cmd->refcount == 1) {
        cmd->refcount = 0;
        cmd->c = NULL;
        for (j = 0; j < cmd->argc; j++) {
            decrRefCount(cmd->argv[j]);
        }
        zfree(cmd->argv);
        cmd->argc = 0;
        
        if (cmd->reply) {
            bkDecrReplyObject(cmd->reply);
            cmd->reply = NULL;
        }
        
        if (cmd->children_num) {
            for (j = 0; j < cmd->children_num; j++) {
                proxyAsyncCommand *ccmd = cmd->children[j];
                _decrProxyAsyncCommandRefCount(ccmd, 0);
            }
            zfree(cmd->children);
            cmd->children_finished_num = 0;
            cmd->children = NULL;
        }
        cmd->parent = NULL;
        cmd->fn = NULL;
        
        zfree(cmd);
    } else {
        cmd->refcount--;
        if (clean_client) {
            cmd->c = NULL;
            if (cmd->children_num) {
                for (j = 0; j < cmd->children_num; j++) {
                    proxyAsyncCommand *ccmd = cmd->children[j];
                    ccmd->c = NULL;
                }
            }
        }
    }
}

void decrProxyAsyncCommandRefCountCleanClient(void *ptr) {
    _decrProxyAsyncCommandRefCount(ptr, 1);
}

void decrProxyAsyncCommandRefCount(void *ptr) {
    _decrProxyAsyncCommandRefCount(ptr, 0);
}

void *dupProxyAsyncCommand(void *o) {
    incrProxyAsyncCommandRefCount(o);
    return o;
}

void addReplyRedisReply(client *c, bkReply *r) {
    switch (r->type) {
        case PROTO_REPLY_STRING:
            addReplyBulkCBuffer(c, r->str, r->len);
            break;
        case PROTO_REPLY_ARRAY:
            addReplyMultiBulkLen(c, r->elements);
            for (int i = 0; i < r->elements; i++)
                if (r->element[i] != NULL)
                    addReplyRedisReply(c, r->element[i]);
            break;
        case PROTO_REPLY_INTEGER:
            addReplyLongLong(c, r->integer);
            break;
        case PROTO_REPLY_NIL:
            addReply(c, shared.nullbulk);
            break;
        case PROTO_REPLY_STATUS:
            addReplyStatus(c, r->str);
            break;
        case PROTO_REPLY_ERROR:
            addReplyError(c, r->str);
            break;
        default:
            serverPanic("unrecognized bkReply type");
            break;
    }
}

void addReplyProxyWaitingList(client *c) {
    listNode *ln;
    listIter li;
    
    listRewind(c->request, &li);
    while((ln = listNext(&li))) {
        proxyAsyncCommand *cmd = listNodeValue(ln);
        if (cmd->children_num) {
            if (cmd->children_finished_num == cmd->children_num) {
                cmd->fn(c, cmd->children, cmd->children_num);
                listDelNode(c->request, ln);
            } else break;
        } else {
            if (cmd->reply) {
                addReplyRedisReply(cmd->c, cmd->reply);
                listDelNode(c->request, ln);
            } else break;
        }
    }
}

void logProxyAsyncCommand(int level, proxyAsyncCommand *cmd) {
    sds s = sdsempty();
    int j;
    for (j = 0; j < cmd->argc; j++) {
        s = sdscatfmt(s, "%s ", cmd->argv[j]->ptr);
    }
    serverLog(level, "%s", s);
    sdsfree(s);
}

void sendProxyAsyncCommand(bkLink *link, proxyAsyncCommand *cmd, bkCallbackFn *fn) {
    int j;
    serverAssert(!cmd->children);
    logProxyAsyncCommand(LL_DEBUG, cmd);
    
    bkAddReplyMultiBulkLen(link, cmd->argc);
    for (j = 0; j < cmd->argc; j++) {
        bkAddRequestBulk(link, cmd->argv[j]);
    }
    
    bkAddCallback(link, fn, cmd);
    incrProxyAsyncCommandRefCount(cmd);
}

/* this function is called when -moved or -ask received.
 * -MOVED 16383 127.0.0.1:8001
 * -ASK 16383 127.0.0.1:8001
 */
int handleSlotRedirection(proxyAsyncCommand *cmd, bkReply *r) {
    int moved = 0, ask = 0;
    proxyRedisInstance *pi;
    int slot;
    char *addr;
    sds *argv = NULL;
    int argc = 0;
    bkLink *link;
    
    if (r->type != PROTO_REPLY_ERROR ||
        cmd->c == NULL ||
        cmd->redirect_cnt > proxy.redirect_max_limit) return C_ERR;
    
    if (strncasecmp(r->str, "MOVED", 5)) {
        /* MOVED 16383 127.0.0.1:8001 */
        moved = 1;
        proxyDoBeforeSleep(PROXY_TODO_UPDATE_SLOT);
    } else if (strncasecmp(r->str, "ASK", 3)) {
        /* ASKING 16383 127.0.0.1:8001 */
        ask = 1;
    } else
        return C_ERR;
    
    argv = sdssplitargs(r->str, &argc);
    slot = atoi(argv[1]);
    addr = argv[2];
    
    if (argc != 3) {
        sdsfreesplitres(argv, argc);
        return C_ERR;
    }
    
    
    pi = getOrCreateProxyRedisInstanceByAddr(addr);
    sdsfreesplitres(argv, argc);
    if (!pi){
        char *msg;
        switch(errno) {
            case EBUSY:
                msg = "Duplicated master name";
                break;
            case EINVAL:
                msg = "Invalid port number";
                break;
            default:
                msg = "Unspecified error adding the instance";
                break;
        }
        serverLog(LL_DEBUG, "+redirection %s %s", r->str, msg);
        return C_ERR;
    }
    
    link = getBackendLinkBySlot(cmd->c, slot);
    if (ask) {
        proxySendAsking(link);
        sendProxyAsyncCommand(link, cmd, proxyRouterCallback);
    } else if (moved) {
        proxyDoBeforeSleep(PROXY_TODO_UPDATE_SLOT);
    }
    
    sendProxyAsyncCommand(link, cmd, proxyRouterCallback);
    cmd->redirect_cnt++;
    return C_OK;
}

static void logBkReply(int level, bkReply *r) {
    switch (r->type) {
        case PROTO_REPLY_ARRAY:
            serverLog(level, "*%zu", r->elements);
            for (int i = 0; i < r->elements; i++)
                if (r->element[i] != NULL)
                    logBkReply(level, r->element[i]);
            break;
        case PROTO_REPLY_INTEGER:
            serverLog(level, "+%lld", r->integer);
            break;
        case PROTO_REPLY_STRING:
            serverLog(level, "$%s", r->str);
            break;
        case PROTO_REPLY_STATUS:
            serverLog(level, ":%s", r->str);
            break;
        case PROTO_REPLY_ERROR:
            serverLog(level, "-%s", r->str);
            break;
        case PROTO_REPLY_NIL:
            serverLog(level, "(nil)");
            break;
        default:
            serverPanic("unrecognized bkReply type");
            break;
    }
}

void proxyRouterCallback(bkLink *link, bkReply *r, void *privdata) {
    proxyAsyncCommand *cmd = privdata;
    
    logBkReply(LL_DEBUG, r);
    
    /* has been redirected. */
    if (handleSlotRedirection(cmd, r) == C_OK) return;
    
    cmd->reply = r;
    bkIncrReplyObject(r);
    if (cmd->parent) {
        cmd->parent->children_finished_num++;
        if ((cmd->parent->children_num == cmd->parent->children_finished_num)
            && (cmd->parent->c)) {
            /* decr parent refcount due to all children has response. */
            decrProxyAsyncCommandRefCount(cmd->parent);
            addReplyProxyWaitingList(cmd->c);
        }
    } else if (cmd->c) {
        addReplyProxyWaitingList(cmd->c);
    }
    
    /* as cmd no longger registered in callback list, decr refcount. */
    decrProxyAsyncCommandRefCount(cmd);
}

/* get link by slot hash */
bkLink *getBackendLinkBySlot(client *c, int slot) {
    proxyRedisInstance *pi = _proxyGetBySlot(slot);
    return pi->pool[c->id % pi->poolsize];
}

/* get link by slot hash */
bkLink *getBackendLinkByKey(client *c, sds key) {
    int slot = keyHashSlot(key, (int) sdslen(key));
    proxyRedisInstance *pi = _proxyGetBySlot(slot);
    return pi->pool[c->id % pi->poolsize];
}

void proxyRouterNotSupportedCommand(client *c) {
    addReplyErrorFormat(c, "not supported command %s.", c->argv[0]->ptr);
}

void proxyRouterNotImplementedCommand(client *c) {
    addReplyErrorFormat(c, "not implemented command %s, this will be supported later.", c->argv[0]->ptr);
}

void proxyRouterSelectCommand(client *c) {
    if (atoi(c->argv[1]->ptr) != 0) {
        addReplyError(c, "only select 0 is allowed");
    } else {
        addReply(c, shared.ok);
    }
}

void proxyRouterFirstKeyCommand(client *c) {
    backendLink *link = NULL;
    int *keyindex, numkeys;
    
    keyindex = getKeysFromCommand(c->cmd,c->argv,c->argc,&numkeys);
    if (numkeys == 0) {
        addReplyError(c, "no key specified while reached proxyRouterFirstKeyCommand, this should "
                      "never happen, please report this issue as I do not want to panic server.");
        goto err;
    }
    
    /* create command */
    proxyAsyncCommand *cmd = createProxyAsyncCommand(c);
    listAddNodeTail(c->request, cmd);
    
    /* send command */
    robj *key = c->argv[keyindex[0]];
    link = getBackendLinkByKey(c, key->ptr);
    sendProxyAsyncCommand(link, cmd, proxyRouterCallback);
    
err:
    getKeysFreeResult(keyindex);
}

/* del exists */
void proxyRouterMultiKeysSumCoalesce(client *c, proxyAsyncCommand **children, int children_num) {
    int sum = 0, j;
    
    for (j = 0; j < children_num; j++)  {
        proxyAsyncCommand *ccmd = children[j];
        bkReply * cr = ccmd->reply;
        switch (cr->type) {
            case PROTO_REPLY_ERROR:
                addReplyError(c, cr->str);
                return;
            case PROTO_REPLY_INTEGER:
                sum += cr->integer;
                break;
            case PROTO_REPLY_ARRAY:
            case PROTO_REPLY_NIL:
            case PROTO_REPLY_STATUS:
            case PROTO_REPLY_STRING:
                addReplyErrorFormat(c, "unexpected reply type from server %d.", cr->type);
                return;
            default:
                serverPanic("unrecognized bkReply type");
        }
    }
    
    addReplyLongLong(c, sum);
}

void _proxyRouterMultiKeysCommand(client *c, redisReplyCoalesceFn *fn) {
    backendLink *link = NULL;
    int *keyindex, numkeys, j, i;
    int step = c->cmd->keystep;
    
    keyindex = getKeysFromCommand(c->cmd,c->argv,c->argc,&numkeys);
    if (numkeys == 0) {
        addReplyError(c, "no key specified while reached _proxyRouterMultiKeysCommand, this should "
                      "never happen, please report this issue as I do not want to panic server.");
        goto err;
    }
    
    /* generate parent cmd */
    proxyAsyncCommand *pcmd = createProxyAsyncCommand(c);
    pcmd->children_num = numkeys;
    pcmd->children = zmalloc(numkeys * sizeof(proxyAsyncCommand *));
    pcmd->fn = fn;
    listAddNodeTail(c->request, pcmd);
    /* this is acturally not referenced by bklink. but we should keep it.
     * the time to decr refcount is when all children has response.
     * see proxyRouterCallback(). */
    incrProxyAsyncCommandRefCount(pcmd);
    
    for (j = 0; j < numkeys; j++) {
        int keysrcindex = keyindex[j], keydestindex = keyindex[0];
        robj *key = c->argv[keysrcindex];
        
        /* generate child cmd */
        proxyAsyncCommand *ccmd = createProxyAsyncCommandWithoutArg(c);
        ccmd->argc = keydestindex + step;
        ccmd->argv = zmalloc(sizeof(robj*) * ccmd->argc);
        for (i = 0; i < keydestindex; i++) {
            incrRefCount(c->argv[i]);
            ccmd->argv[i] = c->argv[i];
        }
        for (i = 0; i < step; i++) {
            robj *item = c->argv[keysrcindex + i];
            incrRefCount(item);
            ccmd->argv[keydestindex + i] = item;
        }
        ccmd->parent = pcmd;
        pcmd->children[j] = ccmd;
        
        /* send cmd */
        link = getBackendLinkByKey(c, key->ptr);
        sendProxyAsyncCommand(link, ccmd, proxyRouterCallback);
    }
    
err:
    getKeysFreeResult(keyindex);
}


void proxyRouterExistsCommand(client *c) {
    _proxyRouterMultiKeysCommand(c, proxyRouterMultiKeysSumCoalesce);
}
void proxyRouterDelCommand(client *c) {
    _proxyRouterMultiKeysCommand(c, proxyRouterMultiKeysSumCoalesce);
}

void proxyRouterMultiKeysStatusCoalesce(client *c, proxyAsyncCommand **children, int children_num) {
    int j;
    
    for (j = 0; j < children_num; j++)  {
        proxyAsyncCommand *ccmd = children[j];
        bkReply * cr = ccmd->reply;
        switch (cr->type) {
            case PROTO_REPLY_ERROR:
                addReplyError(c, cr->str);
                return;
            case PROTO_REPLY_STATUS:
                if (strncmp(cr->str, "ok", 2)) {
                    addReplyStatus(c, cr->str);
                    return;
                }
                break;
            case PROTO_REPLY_INTEGER:
            case PROTO_REPLY_ARRAY:
            case PROTO_REPLY_NIL:
            case PROTO_REPLY_STRING:
                addReplyErrorFormat(c, "unexpected reply type from server %d.", cr->type);
                return;
            default:
                serverPanic("unrecognized bkReply type");
        }
    }
    
    addReply(c, shared.ok);
}

void proxyRouterMsetCommand(client *c) {
    _proxyRouterMultiKeysCommand(c, proxyRouterMultiKeysStatusCoalesce);
}

/* mget */
void proxyRouterMultiKeysMultiBulksCoalesce(client *c, proxyAsyncCommand **children, int children_num) {
    int j;
    
    for (j = 0; j < children_num; j++)  {
        proxyAsyncCommand *ccmd = children[j];
        bkReply * cr = ccmd->reply;
        switch (cr->type) {
            case PROTO_REPLY_ERROR:
                addReplyError(c, cr->str);
                return;
            case PROTO_REPLY_ARRAY:
                if (cr->elements <= 0) {
                    addReplyErrorFormat(c, "upexpected elements length %zu", cr->elements);
                    return;
                }
                break;
            case PROTO_REPLY_STATUS:
            case PROTO_REPLY_INTEGER:
            case PROTO_REPLY_STRING:
            case PROTO_REPLY_NIL:
                addReplyErrorFormat(c, "unexpected reply type from server %d.", cr->type);
                return;
            default:
                serverPanic("unrecognized bkReply type");
        }
    }
    
    addReplyMultiBulkLen(c, children_num);
    for (j = 0; j < children_num; j++)  {
        proxyAsyncCommand *ccmd = children[j];
        bkReply * cr = ccmd->reply;
        serverAssert(cr->type == PROTO_REPLY_ARRAY);
        addReplyRedisReply(c, cr->element[0]);
    }
}

void proxyRouterMgetCommand(client *c) {
    _proxyRouterMultiKeysCommand(c, proxyRouterMultiKeysMultiBulksCoalesce);
}

/* =========================== Proxy command ============================= */

/* Proxy redis instance to Redis protocol representation. */
void addReplyProxyRedisInstance(client *c, proxyRedisInstance *pi) {
    void *mbl;
    int fields = 0, j;
    
    mbl = addDeferredMultiBulkLength(c);
    
    addReplyBulkCString(c,"name");
    addReplyBulkCString(c,pi->name);
    fields++;
    
    addReplyBulkCString(c,"ip");
    addReplyBulkCString(c,pi->ip);
    fields++;
    
    addReplyBulkCString(c,"port");
    addReplyBulkLongLong(c,pi->port);
    fields++;
    
    addReplyBulkCString(c,"link-poolsize");
    addReplyBulkLongLong(c,pi->poolsize);
    fields++;
    
    addReplyBulkCString(c,"link-connected");
    addReplyBulkLongLong(c,pi->connected_num);
    fields++;
    
    addReplyBulkCString(c,"link-pending-commands");
    addReplyMultiBulkLen(c, pi->poolsize);
    for (j = 0; j < pi->poolsize; j++) {
        addReplyBulkLongLong(c, bkPendingCommands(pi->pool[j]));
    }
    fields++;
    
    setDeferredMultiBulkLength(c,mbl,fields*2);
}

/* Output a number of instances contained inside a dictionary as
 * Redis protocol. */
void addReplyDictOfProxyRedisInstances(client *c) {
    dictIterator *di;
    dictEntry *de;
    
    di = dictGetIterator(proxy.instances);
    addReplyMultiBulkLen(c,dictSize(proxy.instances));
    while((de = dictNext(di)) != NULL) {
        proxyRedisInstance *pi = dictGetVal(de);
        addReplyProxyRedisInstance(c,pi);
    }
    dictReleaseIterator(di);
}

proxyRedisInstance *getProxyRedisInstanceByIpPortOrReplyError(client *c, char *ip, int port)
{
    proxyRedisInstance *pi;
    
    pi = getProxyRedisInstanceByIpPort(ip, port);
    if (!pi) {
        addReplyError(c,"No such master with that name");
        return NULL;
    }
    return pi;
}

void proxyCommand(client *c) {
    if (!strcasecmp(c->argv[1]->ptr,"instances")) {
        /* PROXY INSTANCES */
        if (c->argc != 2) goto numargserr;
        addReplyDictOfProxyRedisInstances(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"instance")) {
        /* PROXY INSTANCE <ip> <port> */
        proxyRedisInstance *pi;
        
        if (c->argc != 4) goto numargserr;
        if ((pi = getProxyRedisInstanceByIpPortOrReplyError(c,c->argv[2]->ptr,atoi(c->argv[3]->ptr))) == NULL)
            return;
        addReplyProxyRedisInstance(c, pi);
    } else if (!strcasecmp(c->argv[1]->ptr,"router")) {
        /* SENTINEL ROUTER <ip> <port> [<poolsize>] */
        proxyRedisInstance *pi;
        long port, poolsize;
        char ip[NET_IP_STR_LEN];
        if (c->argc == 4) {
            poolsize = 1;
        } else if (c->argc == 5) {
            if (getLongFromObjectOrReply(c,c->argv[4],&poolsize,"Invalid poolsize")
                != C_OK) return;
            if (poolsize <= 0) {
                addReplyError(c, "poolsize must be 1 or greater.");
                return;
            }
        } else {
            goto numargserr;
        }
        if (getLongFromObjectOrReply(c,c->argv[3],&port,"Invalid port")
            != C_OK) return;
        
        
        /* Make sure the IP field is actually a valid IP before passing it
         * to createProxyRedisInstance(), otherwise we may trigger a
         * DNS lookup at runtime. */
        if (anetResolveIP(NULL,c->argv[2]->ptr,ip,sizeof(ip)) == ANET_ERR) {
            addReplyError(c,"Invalid IP address specified");
            return;
        }
        
        /* Parameters are valid. Try to create the master instance. */
        pi = createProxyRedisInstance(ip, (int) port, (int) poolsize);
        if (pi == NULL) {
            switch(errno) {
                case EBUSY:
                    addReplyError(c,"Duplicated master name");
                    break;
                case EINVAL:
                    addReplyError(c,"Invalid port number");
                    break;
                default:
                    addReplyError(c,"Unspecified error adding the instance");
                    break;
            }
        } else {
            addReply(c,shared.ok);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"flushconfig")) {
        if (c->argc != 2) goto numargserr;
        proxyDoBeforeSleep(PROXY_TODO_SAVE_CONFIG);
        addReply(c,shared.ok);
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"set")) {
        if (c->argc < 4) goto numargserr;
        proxySetCommand(c);
    } else {
        addReplyErrorFormat(c,"Unknown proxy subcommand '%s'",
                            (char*)c->argv[1]->ptr);
    }
    return;
    
numargserr:
    addReplyErrorFormat(c,"Wrong number of arguments for 'proxy %s'",
                        (char*)c->argv[1]->ptr);
}

#define info_section_from_redis(section_name) do { \
if (defsections || allsections || !strcasecmp(section,section_name)) { \
sds redissection; \
if (sections++) info = sdscat(info,"\r\n"); \
redissection = genRedisInfoString(section_name); \
info = sdscatlen(info,redissection,sdslen(redissection)); \
sdsfree(redissection); \
} \
} while(0)

long long getInstantaneousMetric(int metric);

/* PROXY INFO [section] */
void proxyInfoCommand(client *c) {
    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }
    
    int defsections = 0, allsections = 0;
    char *section = c->argc == 2 ? c->argv[1]->ptr : NULL;
    if (section) {
        allsections = !strcasecmp(section,"all");
        defsections = !strcasecmp(section,"default");
    } else {
        defsections = 1;
    }
    
    int sections = 0;
    sds info = sdsempty();
    
    info_section_from_redis("server");
    info_section_from_redis("clients");
    info_section_from_redis("cpu");
    
    /* Stats */
    if (allsections || defsections || !strcasecmp(section,"stats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
                            "# Stats\r\n"
                            "total_connections_received:%lld\r\n"
                            "total_commands_processed:%lld\r\n"
                            "instantaneous_ops_per_sec:%lld\r\n"
                            "total_net_input_bytes:%lld\r\n"
                            "total_net_output_bytes:%lld\r\n"
                            "instantaneous_input_kbps:%.2f\r\n"
                            "instantaneous_output_kbps:%.2f\r\n"
                            "rejected_connections:%lld\r\n",
                            server.stat_numconnections,
                            server.stat_numcommands,
                            getInstantaneousMetric(STATS_METRIC_COMMAND),
                            server.stat_net_input_bytes,
                            server.stat_net_output_bytes,
                            (float)getInstantaneousMetric(STATS_METRIC_NET_INPUT)/1024,
                            (float)getInstantaneousMetric(STATS_METRIC_NET_OUTPUT)/1024,
                            server.stat_rejected_conn);
    }
    
    if (defsections || allsections || !strcasecmp(section,"proxy")) {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;
        
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
                            "# Proxy\r\n"
                            "proxy_redis_instances:%lu\r\n",
                            dictSize(proxy.instances));
        
        di = dictGetIterator(proxy.instances);
        while((de = dictNext(di)) != NULL) {
            proxyRedisInstance *pi = dictGetVal(de);
            
            info = sdscatprintf(info,
                                "master%d:name=%s,connected=%d,address=%s:%d\r\n",
                                master_id++, pi->name, pi->connected_num, pi->ip, pi->port);
        }
        dictReleaseIterator(di);
    }
    
    addReplyBulkSds(c, info);
}

/* PROXY SET <option> <value> ... */
void proxySetCommand(client *c) {
    if (!strcasecmp(c->argv[2]->ptr,"auth-pass") && c->argc == 6) {
        // PROXY SET auth-pass <ip> <port> <pass>
        proxyRedisInstance *pi;
        pi = getProxyRedisInstanceByIpPortOrReplyError(c, c->argv[3]->ptr, atoi(c->argv[4]->ptr));
        if (!pi) {
            addReplyErrorFormat(c, "can not find instance %s:%s",
                                c->argv[3]->ptr, c->argv[4]->ptr);
            return;
        }
        
        sdsfree(pi->auth_pass);
        pi->auth_pass = sdsdup(sdslen(c->argv[5]->ptr) ? c->argv[5]->ptr : NULL);
    } else {
        addReplyErrorFormat(c,"Invalid argument for PROXY SET");
        return;
    }
    
    proxyDoBeforeSleep(PROXY_TODO_SAVE_CONFIG);
    addReply(c,shared.ok);
    return;
}

void proxyDoBeforeSleep(int flags) {
    proxy.todo_before_sleep |= flags;
}

/* This function is called before the event handler returns to sleep for
 * events. It is useful to perform operations that must be done ASAP in
 * reaction to events fired but that are not safe to perform inside event
 * handlers, or to perform potentially expansive tasks that we need to do
 * a single time before replying to clients. */
void proxyBeforeSleep(void) {
    /* Update the cluster state. */
    if (proxy.todo_before_sleep & PROXY_TODO_UPDATE_SLOT)
        proxyUpdateClusterNodes();
    
    /* Save the config using fsync. */
    if (proxy.todo_before_sleep & CLUSTER_TODO_SAVE_CONFIG)
        proxyFlushConfig();
    
    /* Reset our flags (not strictly needed since every single function
     * called for flags set should be able to clear its flag). */
    proxy.todo_before_sleep = 0;
    
    /* Handle writes with pending output buffers. */
    bkHandleLinkssWithPendingWrites();
}

/* ======================== PROXY timer handler ========================== */

/* Perform scheduled operations for all the instances in the dictionary. */
void proxyHandleDictOfRedisInstances() {
    dictIterator *di;
    dictEntry *de;
    
    di = dictGetIterator(proxy.instances);
    while((de = dictNext(di)) != NULL) {
        proxyRedisInstance *pi = dictGetVal(de);
        proxyReconnectBackendLink(pi);
    }
    dictReleaseIterator(di);
}

void proxyTimer(void) {
    proxyHandleDictOfRedisInstances();
}
