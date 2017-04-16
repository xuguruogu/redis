/*
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2011, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 *
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

#ifndef __BACKEND_H
#define __BACKEND_H
#include <stdio.h> /* for size_t */
#include <stdarg.h> /* for va_list */
#include <sys/time.h> /* for struct timeval */

#define BACKEND_CONNECTED (1<<0)
#define BACKEND_ERR (1<<1)
#define BACKEND_PENDING_WRITE (1<<2)
#define BACKEND_CLOSE_LAZY (1<<3)

#define PROTO_REPLY_STRING 1
#define PROTO_REPLY_ARRAY 2
#define PROTO_REPLY_INTEGER 3
#define PROTO_REPLY_NIL 4
#define PROTO_REPLY_STATUS 5
#define PROTO_REPLY_ERROR 6

#define PROTO_IOBUF_MAX_LEN (1024*1024*4)  /* Default max unused reader buffer. */

#define PROXY_MAX_PENDING_COMMANDS 10000

#define bkPendingCommands(link) (listLength((link)->callbacks))

struct backendLink;
typedef struct backendLink bkLink;
struct bkReply;
typedef struct bkReply bkReply;

/* Reply callback prototype and container */
typedef void (bkCallbackFn)(bkLink *link, bkReply *reply, void*);
typedef struct bkCallback {
    bkCallbackFn *fn;
    void *privdata;
} bkCallback;

/* This is the reply object returned by redisCommand() */
typedef struct bkReply {
    int refcount;
    int type; /* BACKEND_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    size_t len; /* Length of string */
    char *str; /* Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING */
    size_t elements; /* number of elements, for BACKEND_REPLY_ARRAY */
    struct bkReply **element; /* elements vector for BACKEND_REPLY_ARRAY */
} bkReply;

void bkIncrReplyObject(void *reply);
void bkDecrReplyObject(void *reply);

typedef struct bkReadTask {
    int type;
    int elements; /* number of elements in multibulk container */
    int idx; /* index in parent (array) object */
    void *obj; /* holds user-generated value for a read task */
    struct bkReadTask *parent; /* parent task */
} bkReadTask;

/* Connection callback prototypes */
typedef void (bkDisconnectCallback)(bkLink*);
typedef void (bkConnectCallback)(bkLink*);

typedef struct backendLink {
    int fd;
    int flags;
    char errstr[128]; /* String representation of error when applicable */
    
    char name[NET_PEER_ID_LEN*2+2];
    
    /* reader */
    char *rbuf; /* Read buffer */
    size_t rpos; /* Buffer cursor */
    size_t rlen; /* Buffer length */
    size_t rmaxbuf; /* Max length of unused buffer */
    size_t rbuf_peak;
    
    /* Reader task */
    bkReadTask rstack[9];
    int ridx; /* Index of current read task */
    void *reply; /* Temporary reply pointer */
    
    /* Writer buffer */
    int wbufpos;
    int wsentlen;
    char wbuf[PROTO_REPLY_CHUNK_BYTES];
    list *requests;            /* List of request *robj to send to the backend. */

    /* Regular command callbacks: bkCallback. */
    list *callbacks;
    
    /* Called when either the connection is terminated due to an error or per
     * user request. The status is set accordingly (C_OK, C_ERR). */
    bkDisconnectCallback *onDisconnect;
    
    /* Called when the first write event was received. */
    bkConnectCallback *onConnect;
    
    void *replyOnFree; /* Temporary reply pointer */
    
    /* Not used by backend link. */
    void *data;
    
    /* statsd */
    size_t reconn_num;
    mstime_t conn_time; /* cc connection time. */
} bkLink;

void bkLinkFree(bkLink *link);
bkLink *bkConnectBind(char *ip, int port, char *source_addr);
void bkAttachEventLoop(bkLink *link, aeEventLoop *el);

void bkSetConnectCallback(bkLink *link, bkConnectCallback *fn);
void bkSetDisconnectCallback(bkLink *link, bkDisconnectCallback *fn);

/**/
void bkHandleLinkssWithPendingWrites(void);
robj *dupLastObjectIfNeeded(list *reply);


/* queue data on the bklink output buffer. */
void bkAddRequest(bkLink *link, robj *obj);
void bkAddRequestSds(bkLink *link, sds s);
void bkAddRequestString(bkLink *link, const char *s, size_t len);
void *bkAddDeferredMultiBulkLength(bkLink *link);
void bkSetDeferredMultiBulkLength(bkLink *link, void *node, long length);
void bkAddRequstLongLongWithPrefix(bkLink *link, long long ll, char prefix);
void bkAddReplyMultiBulkLen(bkLink *link, long length);
void bkAddRequestBulk(bkLink *link, robj *obj);
void bkAddRequestBulkCBuffer(bkLink *link, const void *p, size_t len);
void bkAddRequestBulkSds(bkLink *link, sds s);
void bkAddRequestBulkCString(bkLink *link, const char *s);
void bkAddRequestBulkLongLong(bkLink *link, long long ll);

/* add callback */
void bkAddCallback(bkLink *link, bkCallbackFn *fn, void *privdata);

#endif
