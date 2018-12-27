/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "atomicvar.h"
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>
#include "slowlog.h"

static void setProtocolError(const char *errstr, client *c, int pos);

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
    void *sh = sdsAllocPtr(s);
    return zmalloc_size(sh);
}

/* Return the amount of memory used by the sds string at object->ptr
 * for a string object. */
size_t getStringObjectSdsUsedMemory(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    case OBJ_ENCODING_RAW: return sdsZmallocSize(o->ptr);
    case OBJ_ENCODING_EMBSTR: return zmalloc_size(o)-sizeof(robj);
    default: return 0; /* Just integer encoding for now. */
    }
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
    return sdsdup(o);
}

void freeClientReplyValue(void *o) {
    sdsfree(o);
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

client *createClient(int fd) {
    client *c = zmalloc(sizeof(client));

    /* passing -1 as fd it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    if (fd != -1) {
        anetNonBlock(NULL,fd);
        anetEnableTcpNoDelay(NULL,fd);
        if (server.tcpkeepalive)
            anetKeepAlive(NULL,fd,server.tcpkeepalive);
        if (aeCreateFileEvent(server.el,fd,AE_READABLE,
            readQueryFromClient, c) == AE_ERR)
        {
            close(fd);
            zfree(c);
            return NULL;
        }
    }

    selectDb(c,0);
    uint64_t client_id;
    atomicGetIncr(server.next_client_id,client_id,1);
    c->id = client_id;
    c->fd = fd;
    c->name = NULL;
    c->bufpos = 0;
    c->querybuf = sdsempty();
    c->pending_querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->cmd = c->lastcmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->ctime = c->lastinteraction = server.unixtime;
    c->authenticated = 0;
    c->replstate = REPL_STATE_NONE;
    c->repl_put_online_on_ack = 0;
    c->reploff = 0;
    c->read_reploff = 0;
    c->repl_ack_off = 0;
    c->repl_ack_time = 0;
    c->slave_listening_port = 0;
    c->slave_ip[0] = '\0';
    c->slave_capa = SLAVE_CAPA_NONE;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    listSetFreeMethod(c->reply,freeClientReplyValue);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->btype = BLOCKED_NONE;
    c->bpop.timeout = 0;
    c->bpop.keys = dictCreate(&objectKeyPointerValueDictType,NULL);
    if (server.swap_mode) {
        c->context = NULL;
        c->repl_timer_id = -1;
        c->ssdb_status = SSDB_NONE;
        c->transfer_snapshot_last_keepalive_time = -1;
        c->bpop.loading_or_transfer_keys = dictCreate(&objectKeyPointerValueDictType,NULL);
        c->ssdb_conn_flags = 0;
        c->ssdb_replies[0] = NULL;
        c->ssdb_replies[1] = NULL;
        c->revert_len = 0;
        c->first_key_index = 0;
    }
    c->bpop.target = NULL;
    c->bpop.numreplicas = 0;
    c->bpop.reploffset = 0;
    c->woff = 0;
    c->watched_keys = listCreate();
    c->pubsub_channels = dictCreate(&objectKeyPointerValueDictType,NULL);
    c->pubsub_patterns = listCreate();
    c->peerid = NULL;
    listSetFreeMethod(c->pubsub_patterns,decrRefCountVoid);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    if (fd != -1) listAddNodeTail(server.clients,c);
    initClientMultiState(c);
    return c;
}

/* This function is called every time we are going to transmit new data
 * to the client. The behavior is the following:
 *
 * If the client should receive new data (normal clients will) the function
 * returns C_OK, and make sure to install the write handler in our event
 * loop so that when the socket is writable new data gets written.
 *
 * If the client should not receive new data, because it is a fake client
 * (used to load AOF in memory), a master or because the setup of the write
 * handler failed, the function returns C_ERR.
 *
 * The function may return C_OK without actually installing the write
 * event handler in the following cases:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contains something.
 * 2) The client is a slave but not yet online, so we want to just accumulate
 *    writes in the buffer but not actually sending them yet.
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns C_ERR no
 * data should be appended to the output buffers. */
int prepareClientToWrite(client *c) {
    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    if (c->flags & (CLIENT_LUA|CLIENT_MODULE)) return C_OK;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies. */
    if (c->flags & (CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP)) return C_ERR;

    /* Masters don't receive replies, unless CLIENT_MASTER_FORCE_REPLY flag
     * is set. */
    if ((c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_MASTER_FORCE_REPLY)) return C_ERR;

    if (c->fd <= 0) return C_ERR; /* Fake client for AOF loading. */

    if (server.swap_mode && c == server.slave_ssdb_load_evict_client) return C_ERR;

    /* Schedule the client to write the output buffers to the socket only
     * if not already done (there were no pending writes already and the client
     * was yet not flagged), and, for slaves, if the slave can actually
     * receive writes at this stage. */
    if (!clientHasPendingReplies(c) &&
        !(c->flags & CLIENT_PENDING_WRITE) &&
            (c->replstate == REPL_STATE_NONE ||
             (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack) ||
             (c->flags & CLIENT_SLAVE_FORCE_PROPAGATE)))
    {
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        c->flags |= CLIENT_PENDING_WRITE;
        listAddNodeHead(server.clients_pending_write,c);
    }

    /* Authorize the caller to queue in the output buffer of this client. */
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

int _addReplyToBuffer(client *c, const char *s, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return C_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return C_ERR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return C_ERR;

    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return C_OK;
}

void _addReplyObjectToList(client *c, robj *o) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        sds s = sdsdup(o->ptr);
        listAddNodeTail(c->reply,s);
        c->reply_bytes += sdslen(s);
    } else {
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        if (tail && sdslen(tail)+sdslen(o->ptr) <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatsds(tail,o->ptr);
            listNodeValue(ln) = tail;
            c->reply_bytes += sdslen(o->ptr);
        } else {
            sds s = sdsdup(o->ptr);
            listAddNodeTail(c->reply,s);
            c->reply_bytes += sdslen(s);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* This method takes responsibility over the sds. When it is no longer
 * needed it will be free'd, otherwise it ends up in a robj. */
void _addReplySdsToList(client *c, sds s) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
        sdsfree(s);
        return;
    }

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,s);
        c->reply_bytes += sdslen(s);
    } else {
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        if (tail && sdslen(tail)+sdslen(s) <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatsds(tail,s);
            listNodeValue(ln) = tail;
            c->reply_bytes += sdslen(s);
            sdsfree(s);
        } else {
            listAddNodeTail(c->reply,s);
            c->reply_bytes += sdslen(s);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

void _addReplyStringToList(client *c, const char *s, size_t len) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        sds node = sdsnewlen(s,len);
        listAddNodeTail(c->reply,node);
        c->reply_bytes += len;
    } else {
        listNode *ln = listLast(c->reply);
        sds tail = listNodeValue(ln);

        /* Append to this object when possible. If tail == NULL it was
         * set via addDeferredMultiBulkLength(). */
        if (tail && sdslen(tail)+len <= PROTO_REPLY_CHUNK_BYTES) {
            tail = sdscatlen(tail,s,len);
            listNodeValue(ln) = tail;
            c->reply_bytes += len;
        } else {
            sds node = sdsnewlen(s,len);
            listAddNodeTail(c->reply,node);
            c->reply_bytes += len;
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

void addReply(client *c, robj *obj) {
    if (prepareClientToWrite(c) != C_OK) return;

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (sdsEncodedObject(obj)) {
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != C_OK)
            _addReplyObjectToList(c,obj);
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* Optimization: if there is room in the static buffer for 32 bytes
         * (more than the max chars a 64 bit integer can take as string) we
         * avoid decoding the object and go for the lower level approach. */
        if (listLength(c->reply) == 0 && (sizeof(c->buf) - c->bufpos) >= 32) {
            char buf[32];
            int len;

            len = ll2string(buf,sizeof(buf),(long)obj->ptr);
            if (_addReplyToBuffer(c,buf,len) == C_OK)
                return;
            /* else... continue with the normal code path, but should never
             * happen actually since we verified there is room. */
        }
        obj = getDecodedObject(obj);
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != C_OK)
            _addReplyObjectToList(c,obj);
        decrRefCount(obj);
    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

void addReplySds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) == C_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _addReplySdsToList(c,s);
    }
}

/* This low level function just adds whatever protocol you send it to the
 * client buffer, trying the static buffer initially, and using the string
 * of objects if not possible.
 *
 * It is efficient because does not create an SDS object nor an Redis object
 * if not needed. The object will only be created by calling
 * _addReplyStringToList() if we fail to extend the existing tail object
 * in the list of objects. */
void addReplyString(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    if (_addReplyToBuffer(c,s,len) != C_OK)
        _addReplyStringToList(c,s,len);
}

void addReplyErrorLength(client *c, const char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c,err,strlen(err));
}

void addReplyErrorFormat(client *c, const char *fmt, ...) {
    size_t l, j;
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Make sure there are no newlines in the string, otherwise invalid protocol
     * is emitted. */
    l = sdslen(s);
    for (j = 0; j < l; j++) {
        if (s[j] == '\r' || s[j] == '\n') s[j] = ' ';
    }
    addReplyErrorLength(c,s,sdslen(s));
    sdsfree(s);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c,status,strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    addReplyStatusLength(c,s,sdslen(s));
    sdsfree(s);
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addDeferredMultiBulkLength(client *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredMultiBulkLength() will be called. */
    if (prepareClientToWrite(c) != C_OK) return NULL;
    listAddNodeTail(c->reply,NULL); /* NULL is our placeholder. */
    return listLast(c->reply);
}

/* Populate the length object and try gluing it to the next chunk. */
void setDeferredMultiBulkLength(client *c, void *node, long length) {
    listNode *ln = (listNode*)node;
    sds len, next;

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addDeferredMultiBulkLength() */
    if (node == NULL) return;

    len = sdscatprintf(sdsnewlen("*",1),"%ld\r\n",length);
    listNodeValue(ln) = len;
    c->reply_bytes += sdslen(len);
    if (ln->next != NULL) {
        next = listNodeValue(ln->next);

        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next != NULL) {
            len = sdscatsds(len,next);
            listDelNode(c->reply,ln->next);
            listNodeValue(ln) = len;
            /* No need to update c->reply_bytes: we are just moving the same
             * amount of bytes from one node to another. */
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* Add a double as a bulk reply */
void addReplyDouble(client *c, double d) {
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if (isinf(d)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
        addReplyBulkCString(c, d > 0 ? "inf" : "-inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
        addReplyString(c,sbuf,slen);
    }
}

/* Add a long double as a bulk reply, but uses a human readable formatting
 * of the double instead of exposing the crude behavior of doubles to the
 * dear user. */
void addReplyHumanLongDouble(client *c, long double d) {
    robj *o = createStringObjectFromLongDouble(d,1);
    addReplyBulk(c,o);
    decrRefCount(o);
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        addReplyLongLongWithPrefix(c,ll,':');
}

void addReplyMultiBulkLen(client *c, long length) {
    if (length < OBJ_SHARED_BULKHDR_LEN)
        addReply(c,shared.mbulkhdr[length]);
    else
        addReplyLongLongWithPrefix(c,length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(client *c, robj *obj) {
    size_t len;

    if (sdsEncodedObject(obj)) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }

    if (len < OBJ_SHARED_BULKHDR_LEN)
        addReply(c,shared.bulkhdr[len]);
    else
        addReplyLongLongWithPrefix(c,len,'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    addReplyLongLongWithPrefix(c,len,'$');
    addReplyString(c,p,len);
    addReply(c,shared.crlf);
}

/* Add sds to reply (takes ownership of sds and frees it) */
void addReplyBulkSds(client *c, sds s)  {
    addReplyLongLongWithPrefix(c,sdslen(s),'$');
    addReplySds(c,s);
    addReply(c,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

/* Copy 'src' client output buffers into 'dst' client output buffers.
 * The function takes care of freeing the old output buffers of the
 * destination client. */
void copyClientOutputBuffer(client *dst, client *src) {
    listRelease(dst->reply);
    dst->reply = listDup(src->reply);
    memcpy(dst->buf,src->buf,src->bufpos);
    dst->bufpos = src->bufpos;
    dst->reply_bytes = src->reply_bytes;
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
int clientHasPendingReplies(client *c) {
    return c->bufpos || listLength(c->reply);
}

void handleConnectSSDBok(client* c) {
    serverLog(LL_DEBUG, "connect ssdb success");
    if (server.ssdb_is_down) {
        serverLog(LL_NOTICE, "[!!!]SSDB is up now");
        server.ssdb_is_down = 0;
    }
    c->revert_len = 0;

    if (c == server.master && server.master->ssdb_conn_flags & CONN_RECEIVE_INCREMENT_UPDATES) {
        /* do nothing */
    } else if (c->flags & CLIENT_MASTER && listLength(server.ssdb_write_oplist) > 0) {
        /* NOTE: to ensure data consistency between the slave myself and our master,
        * for server.master/server.cached_master connection, if there are unconfirmed
        * write operations in server.ssdb_write_oplist, which had been send to SSDB
        * successfully but we don't know whether they are executed actually, we must
        * confirm with SSDB and re-send all failed writes to SSDB at first, then we change
        * server.master/server.cached_master->ssdb_conn_flags to CONN_SUCCESS, after that,
        * SSDB write commands can be send and processed normally by 'processCommandMaybeInSSDB'. */

        serverLog(LL_DEBUG, "master/cached_master connect ssdb success, check repopid...");
        if (sendRepopidCheckToSSDB(c) == C_OK)
            c->ssdb_conn_flags |= CONN_CHECK_REPOPID;
    } else {
        c->ssdb_conn_flags |= CONN_SUCCESS;
    }
}

void ssdbConnectCallback(aeEventLoop *el, int fd, void *privdata, int mask) {
    int sockerr = 0;
    socklen_t errlen = sizeof(sockerr);
    UNUSED(el);
    UNUSED(mask);
    client* c = privdata;

    c->ssdb_conn_flags &= ~CONN_CONNECTING;
    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    if (sockerr) {
        serverLog(LL_WARNING,"Error condition on socket for connect ssdb: %s",
            strerror(sockerr));
        goto error;
    }

    /* Remove the events added before. */
    aeDeleteFileEvent(server.el, c->context->fd, AE_READABLE|AE_WRITABLE);

    if (aeCreateFileEvent(server.el, c->context->fd,
                          AE_READABLE, ssdbClientUnixHandler, c) == AE_ERR) {
        serverLog(LL_VERBOSE, "Unrecoverable error creating ssdbFd file event.");
        goto error;
    }
    handleConnectSSDBok(c);
    return;
error:
    c->ssdb_conn_flags |= CONN_CONNECT_FAILED;
    aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
    redisFree(c->context);
    c->context = NULL;
}

/* Connecting to SSDB unix socket. */
int nonBlockConnectToSsdbServer(client *c) {
    redisContext *context = NULL;
    if (c->context) return C_OK;

    if (server.ssdb_server_unixsocket != NULL) {
        /* reset connect failed flag before re-connect. */
        c->ssdb_conn_flags &= ~CONN_CONNECT_FAILED;

        context = redisConnectUnixNonBlock(server.ssdb_server_unixsocket);

        if (!context) {
            c->ssdb_conn_flags |= CONN_CONNECT_FAILED;
            return C_ERR;
        }

        if (context->err) {
            c->ssdb_conn_flags |= CONN_CONNECT_FAILED;
            serverLog(LL_VERBOSE, "Could not connect to SSDB server:%s", context->errstr);
            redisFree(context);
            return C_ERR;
        }

        if (errno == EINPROGRESS) {
            if (aeCreateFileEvent(server.el,context->fd,AE_READABLE|AE_WRITABLE,
                                  ssdbConnectCallback,c) == AE_ERR)
            {
                c->ssdb_conn_flags |= CONN_CONNECT_FAILED;
                redisFree(context);
                return C_ERR;
            }
            c->ssdb_conn_flags |= CONN_CONNECTING;
            c->context = context;
        } else {
            /* connect success */
            if (aeCreateFileEvent(server.el, context->fd,
                                  AE_READABLE, ssdbClientUnixHandler, c) == AE_ERR) {
                c->ssdb_conn_flags |= CONN_CONNECT_FAILED;
                redisFree(context);
                return C_ERR;
            }
            c->context = context;
            handleConnectSSDBok(c);
        }
        return C_OK;
    }
    return C_ERR;
}

/* Caller should free finalcmd. */
sds composeRedisCmd(int argc, const char **argv, const size_t *argvlen) {
    char *cmd = NULL;
    sds finalcmd = NULL;
    int len = 0;

    len = redisFormatCommandArgv(&cmd, argc, argv, argvlen);

    if (len == -1) {
        serverLog(LL_WARNING, "Out of Memory for redisFormatCommandArgv.");
        return NULL;
    }

    finalcmd = sdsnewlen(cmd, len);
    zlibc_free(cmd);

    return finalcmd;
}

/* Caller should free finalcmd. */
sds composeCmdFromArgs(int argc, robj** obj_argv) {
    int i;
    sds finalcmd;
    char ** argv;
    size_t * argvlen;
    if (argc > SSDB_CMD_DEFAULT_MAX_ARGC) {
        argv = zmalloc(sizeof(char *) * argc);
        argvlen = zmalloc(sizeof(size_t) * argc);
    } else {
        argv = server.ssdbargv;
        argvlen = server.ssdbargvlen;
    }

    for (i = 0; i < argc; i ++) {
        argv[i] = obj_argv[i]->ptr;
        argvlen[i] = sdslen((sds)(obj_argv[i]->ptr));
    }

    finalcmd = composeRedisCmd(argc, (const char **)argv, (const size_t *)argvlen);

    if (argc > SSDB_CMD_DEFAULT_MAX_ARGC) {
        zfree(argv);
        zfree(argvlen);
    }
    return finalcmd;
}

void handleSSDBconnectionDisconnect(client* c) {
    if ((c->ssdb_conn_flags & CONN_WAIT_FLUSH_CHECK_REPLY) && server.flush_check_begin_time != -1) {
        server.flush_check_unresponse_num -= 1;
        c->ssdb_conn_flags &= ~CONN_WAIT_FLUSH_CHECK_REPLY;

        serverLog(LL_DEBUG, "[flushall]connection(c->context->fd:%d, c->fd:%d) with ssdb disconnected, unresponse num:%d",
                  c->context ? c->context->fd : -1, c->fd, server.flush_check_unresponse_num);
        if (0 == server.flush_check_unresponse_num) {
            if (c != server.current_flushall_client)
                doSSDBflushIfCheckDone();
            else
                server.flush_check_begin_time = 0;
        }
    } else if (c->ssdb_conn_flags & CONN_WAIT_WRITE_CHECK_REPLY && server.check_write_begin_time != -1) {
        server.check_write_unresponse_num -= 1;
        c->ssdb_conn_flags &= ~CONN_WAIT_WRITE_CHECK_REPLY;
        if (0 == server.check_write_unresponse_num) {
            if (c != server.ssdb_replication_client)
                makeSSDBsnapshotIfCheckOK();
            else
                resetCustomizedReplication();
        }
    }

    c->ssdb_conn_flags &= ~CONN_SUCCESS;
    c->ssdb_conn_flags |= CONN_CONNECT_FAILED;

    if (c->context) {
         /* Unlink resources used in connecting to SSDB. */
        if (c->context->fd > 0)
            aeDeleteFileEvent(server.el, c->context->fd, AE_READABLE|AE_WRITABLE);
        redisFree(c->context);
        c->context = NULL;
    }

    /* for server.master/server.cached_master only */
    if (c->flags & CLIENT_MASTER) {
        c->ssdb_conn_flags &= ~CONN_CHECK_REPOPID;
        server.send_failed_write_after_unblock = 0;
        /* if the replication connection(server.master) disconnect, we must clean visiting_ssdb_keys to
        * avoid wrong visiting count. */
        dictEmpty(EVICTED_DATA_DB->visiting_ssdb_keys, NULL);
    }
}

int closeAndReconnectSSDBconnection(client* c) {
    if (c->context) {
        serverLog(LL_DEBUG, "ssdb connection disconnect! c->fd:%d,c->context->fd:%d",
                  c->fd, c->context ? c->context->fd : -1);
    }
    handleSSDBconnectionDisconnect(c);

    if (nonBlockConnectToSsdbServer(c) == C_ERR) {
        return C_ERR;
    }
    return C_OK;
}

/* don't call this function directly, use sendCommandToSSDB instead. */
static int internalSendCommandToSSDB(client *c, sds finalcmd) {
    int nwritten;
    serverAssert(c && finalcmd);
    if (!c->context)
        return C_FD_ERR;

    while (finalcmd && sdslen(finalcmd) > 0) {
        nwritten = write(c->context->fd, finalcmd, sdslen(finalcmd));
        if (nwritten == -1) {
            if (errno == EAGAIN || errno == EINTR) {
                /* Try again later. */
            } else {
                if (isSpecialConnection(c))
                    freeClient(c);
                else {
                    serverLog(LL_WARNING, "Error writing to SSDB server: %s", strerror(errno));
                    closeAndReconnectSSDBconnection(c);
                }
                sdsfree(finalcmd);
                return C_FD_ERR;
            }
        } else if (nwritten > 0) {
            if (nwritten == (signed) sdslen(finalcmd)) {
                sdsfree(finalcmd);
                finalcmd = NULL;
            } else {
                sdsrange(finalcmd, nwritten, -1);
            }
        }
    }

    return C_OK;
}

int sendFailedRetryCommandToSSDB(client* c, sds finalcmd) {
    serverAssert(finalcmd != NULL);
    return internalSendCommandToSSDB(c, finalcmd);
}

int sendRepopidCheckToSSDB(client* c) {
    if (!(c->flags & CLIENT_MASTER)) return C_ERR;

    const char* tmpargv[2];
    tmpargv[0] = "repopid";
    tmpargv[1] = "get";

    sds cmd = composeRedisCmd(2, tmpargv, NULL);

    return internalSendCommandToSSDB(c, cmd);
}

/* for server.master, we must send all failed writes to SSDB at first, and then
 * we can set server.master->ssdb_conn_flags to CONN_SUCCESS, if is_slave_retry
 * is true, we don't check CONN_SUCCESS flag. */
int sendRepopidToSSDB(client* c, time_t op_time, int op_id, int is_slave_retry) {
    const char* tmpargv[4];
    char time[64];
    char index[32];
    int ret;

    tmpargv[0] = "repopid";
    tmpargv[1] = "set";
    ll2string(time, sizeof(time), op_time);
    tmpargv[2] = time;
    ll2string(index, sizeof(index), op_id);
    tmpargv[3] = index;

    sds cmd = composeRedisCmd(4, tmpargv, NULL);

    if (is_slave_retry)
        ret = sendFailedRetryCommandToSSDB(c, cmd);
    else
        ret = sendCommandToSSDB(c, cmd);
    return ret;
}

/* Querying SSDB server if querying redis fails, Compose the finalcmd if finalcmd is NULL. */
int sendCommandToSSDB(client *c, sds finalcmd) {
    struct redisCommand *cmd = NULL;

    if (!c) {
        sdsfree(finalcmd);
        return C_ERR;
    }

    /* only if ssdb connection status is CONN_SUCCESS, it's safe to send commands to SSDB.
     * for example, on server.master connection, we may need to re-send some failed writes
     * to SSDB after re-connect success, before that, we can't send command to SSDB directly.*/
    if (!(c->ssdb_conn_flags & CONN_SUCCESS) ||
        !c->context || c->context->fd <= 0) {
        if (isSpecialConnection(c))
            freeClient(c);
        else {
            if ((c->ssdb_conn_flags & CONN_CONNECTING) ||
                (c->flags & CLIENT_MASTER && (c->ssdb_conn_flags & CONN_CHECK_REPOPID)))
                serverLog(LL_DEBUG, "ssdb connection status is connecting");
            else
                serverLog(LL_DEBUG, "ssdb connection status is disconnected");
        }
        sdsfree(finalcmd);
        return C_FD_ERR;
    }

    if (!finalcmd) {
        cmd = lookupCommand(c->argv[0]->ptr);
        if (!cmd || !(cmd->flags & CMD_SWAP_MODE)
            || (c->flags & CLIENT_MULTI))
            return C_ERR;

        finalcmd = composeCmdFromArgs(c->argc, c->argv);
    }

    if (!finalcmd) {
        serverLog(LL_WARNING, "out of memory!");
        return C_ERR;
    }

    serverLog(LL_DEBUG, "sendCommandToSSDB context fd: %d, redis fd:%d",
              c->context ? c->context->fd : -1, c->fd);

    return internalSendCommandToSSDB(c, finalcmd);
}

void sendFlushCheckCommandToSSDB(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *c = (client *)privdata;
    UNUSED(mask);
    UNUSED(el);
    UNUSED(fd);

    sds finalcmd = sdsnew("*1\r\n$17\r\nrr_flushall_check\r\n");
    if (sendCommandToSSDB(c, finalcmd) != C_OK) {
        if ((c->ssdb_conn_flags & CONN_WAIT_FLUSH_CHECK_REPLY) && server.flush_check_begin_time != -1) {
            server.flush_check_unresponse_num -= 1;
            c->ssdb_conn_flags &= ~CONN_WAIT_FLUSH_CHECK_REPLY;
            serverLog(LL_DEBUG, "[flushall]connection(c->context->fd:%d,c->fd:%d) with ssdb disconnected, unresponse num:%d",
                      c->context ? c->context->fd : -1, c->fd, server.flush_check_unresponse_num);
        }
    } else {
        aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
        serverLog(LL_DEBUG, "[flushall]send flush check sucess, c->flags:%d, c->ssdb_conn_flags:%d",
                  c->flags, c->ssdb_conn_flags);
    }
}

void sendCheckWriteCommandToSSDB(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *c = (client *)privdata;
    UNUSED(mask);
    UNUSED(el);
    UNUSED(fd);

    /* Expect the response of rr_check_write as 'rr_check_write ok/nok'. */
    sds finalcmd = sdsnew("*1\r\n$14\r\nrr_check_write\r\n");

    if (sendCommandToSSDB(c, finalcmd) != C_OK) {
        serverLog(LL_DEBUG, "Sending rr_check_write to SSDB failed.");
        if (c->ssdb_conn_flags & CONN_WAIT_WRITE_CHECK_REPLY && server.check_write_begin_time != -1) {
            server.check_write_unresponse_num -= 1;
            c->ssdb_conn_flags &= ~CONN_WAIT_WRITE_CHECK_REPLY;
            serverLog(LL_DEBUG, "[replication check write]connection with ssdb disconnected, unresponse num:%d",
                      server.check_write_unresponse_num);
        }
    } else {
        serverLog(LL_DEBUG, "Replication log: Sending rr_check_write to SSDB, fd: %d, rr_check_write counter: %d",
                  fd, server.check_write_unresponse_num);
        aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
    }
}


#define MAX_ACCEPTS_PER_CALL 1000
static void acceptCommonHandler(int fd, int flags, char *ip) {
    client *c;
    if ((c = createClient(fd)) == NULL) {
        serverLog(LL_WARNING,
            "Error registering fd event for the new client: %s (fd=%d)",
            strerror(errno),fd);
        close(fd); /* May be already closed, just ignore errors */
        return;
    }
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in non-blocking
     * mode and we can send an error for free using the Kernel I/O */
    if (listLength(server.clients) > server.maxclients) {
        char *err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors */
        if (write(c->fd,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        server.stat_rejected_conn++;
        freeClient(c);
        return;
    }

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    if (server.protected_mode &&
        server.bindaddr_count == 0 &&
        server.requirepass == NULL &&
        !(flags & CLIENT_UNIX_SOCKET) &&
        ip != NULL)
    {
        if (strcmp(ip,"127.0.0.1") && strcmp(ip,"::1")) {
            char *err =
                "-DENIED Redis is running in protected mode because protected "
                "mode is enabled, no bind address was specified, no "
                "authentication password is requested to clients. In this mode "
                "connections are only accepted from the loopback interface. "
                "If you want to connect from external computers to Redis you "
                "may adopt one of the following solutions: "
                "1) Just disable protected mode sending the command "
                "'CONFIG SET protected-mode no' from the loopback interface "
                "by connecting to Redis from the same host the server is "
                "running, however MAKE SURE Redis is not publicly accessible "
                "from internet if you do so. Use CONFIG REWRITE to make this "
                "change permanent. "
                "2) Alternatively you can just disable the protected mode by "
                "editing the Redis configuration file, and setting the protected "
                "mode option to 'no', and then restarting the server. "
                "3) If you started the server manually just for testing, restart "
                "it with the '--protected-mode no' option. "
                "4) Setup a bind address or an authentication password. "
                "NOTE: You only need to do one of the above things in order for "
                "the server to start accepting connections from the outside.\r\n";
            if (write(c->fd,err,strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            server.stat_rejected_conn++;
            freeClient(c);
            return;
        }
    }

    server.stat_numconnections++;
    c->flags |= flags;

    if (server.swap_mode) {
        strncpy(c->client_ip, ip, NET_IP_STR_LEN);

        if (server.is_doing_flushall) {
            /* maybe redis is doing flush check before flushall, will connect SSDB later.*/
            c->ssdb_conn_flags |= CONN_CONNECT_FAILED;
            serverLog(LL_DEBUG, "is doing flushall, will connnect SSDB later.");
        } else if (server.ssdb_status > SSDB_NONE && server.ssdb_status < MASTER_SSDB_SNAPSHOT_PRE) {
            /* redis is doing write check before replication, will connect SSDB later. */
            c->ssdb_conn_flags |= CONN_CONNECT_FAILED;
            serverLog(LL_DEBUG, "is doing write check for replication, will connnect SSDB later.");
        } else if (C_OK != nonBlockConnectToSsdbServer(c))
            serverLog(LL_DEBUG, "connect ssdb failed, will retry to connect.");
    }
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);
        acceptCommonHandler(cfd,0,cip);
    }
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd, max = MAX_ACCEPTS_PER_CALL;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetUnixAccept(server.neterr, fd);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", server.neterr);
            return;
        }
        serverLog(LL_VERBOSE,"Accepted connection to %s", server.unixsocket);
        acceptCommonHandler(cfd,CLIENT_UNIX_SOCKET,NULL);
    }
}

void handleClientsBlockedOnFlushall(void) {
    listIter li;
    listNode *ln;
    int ret;

    listRewind(server.ssdb_flushall_blocked_clients, &li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        listDelNode(server.ssdb_flushall_blocked_clients, ln);

        serverLog(LL_DEBUG, "[!!!!]unblocked by handleClientsBlockedOnFlushall:%p", (void*)c);
        unblockClient(c);

        /* Convert block type. */
        if ((ret = tryBlockingClient(c)) != C_OK) {
            /* C_NOTSUPPORT_ERR should be handled before. */
            serverAssert(ret != C_NOTSUPPORT_ERR);
            continue;
        }

        if (runCommand(c) == C_OK)
            resetClient(c);
    }
}

void handleClientsBlockedOnMigrate(void) {
    listIter li;
    listNode *ln;

    listRewind(server.delayed_migrate_clients, &li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        if (checkKeysForMigrate(c) == C_OK) {
            listDelNode(server.delayed_migrate_clients, ln);
            unblockClient(c);
            serverLog(LL_DEBUG, "client migrate list del: %ld", (long)c);
            if (runCommand(c) == C_OK)
                resetClient(c);
        }
    }
}

static void revertClientBufReply(client *c, size_t revertlen) {
    listNode *ln;
    sds tail;

    if (c->flags & CLIENT_MASTER) return;

    while (revertlen > 0) {
        if (listLength(c->reply) > 0
            && (ln = listLast(c->reply))
            && (tail = listNodeValue(ln))) {
            size_t length = sdslen(tail);

            if (length > revertlen) {
                sdsrange(tail, 0, length - revertlen - 1);
                c->reply_bytes -= revertlen;
                break;
            } else if (length == revertlen) {
                listDelNode(c->reply, ln);
                c->reply_bytes -= length;
                break;
            } else {
                listDelNode(c->reply, ln);
                c->reply_bytes -= length;
                revertlen -= length;
            }
        } else {
            /* Only need to handle c->buf. */
            serverAssert(c->bufpos >= (int)revertlen);
            c->bufpos -= revertlen;
            break;
        }
    }
}

#define IsReplyEqual(reply, sds_response) (sdslen(sds_response) == (size_t)(reply)->len && \
    0 == memcmp((reply)->str, sds_response, (reply)->len))

int handleResponseOfSlaveSSDBflush(client *c, redisReply* reply) {
    if (server.master == c || server.cached_master == c) {
        if (c->cmd && c->btype == BLOCKED_BY_FLUSHALL && c->cmd->proc == flushallCommand) {
            struct ssdb_write_op* op;
            listNode *ln;
            redisReply* reply2, *repoid_response;
            time_t resp_op_time;
            int resp_op_index;
            int ret;

            ln = listFirst(server.ssdb_write_oplist);
            op = ln->value;
            if (op->cmd->proc != flushallCommand) {
                 /* this is not a response of this "flushall" command. */
                serverLog(LL_DEBUG, "this is not a response of this 'flushall' command");
                return C_OK;
            }

            reply2 = c->ssdb_replies[1];
            repoid_response = reply2->element[1];
            ret = sscanf(repoid_response->str, "repopid %ld %d", &resp_op_time, &resp_op_index);
            serverAssert(2 == ret);
            if (resp_op_time == op->time && resp_op_index == op->index) {
                /* this is the response of 'flushall' */
                unblockClient(c);
                resetClient(c);
                if (IsReplyEqual(reply, shared.flushdoneok)){
                    /* ssdb flushall success, we can remove it from ssdb_write_oplist. */
                    serverLog(LL_DEBUG, "received ssdb flushall response");
                    listDelNode(server.ssdb_write_oplist, ln);
                    /* if conn status of server.master/server.cached_master is not CONN_SUCCESS,
                     * continue to process the rest failed writes. */
                    if (c->flags & CLIENT_MASTER && !(c->ssdb_conn_flags & CONN_SUCCESS))
                        confirmAndRetrySlaveSSDBwriteOp(c, -1, -1);
                } else {
                    /* close SSDB connection and we will retry flushall after connected. */
                    closeAndReconnectSSDBconnection(c);
                }
                serverLog(LL_DEBUG, "server.master/server.cached_master client is unblocked");
                return C_RETURN;
            } else {
                /* this is not a response of this "flushall" command. */
                serverLog(LL_DEBUG, "this is not a response of this 'flushall' command");
                return C_OK;
            }
        }
        return C_OK;
    } else {
        return C_RETURN;
    }
}

int handleResponseOfSSDBflushDone(client *c, redisReply* reply, int revert_len) {
    int process_status;
    client* cur_flush_client;
    UNUSED(c);

    if (IsReplyEqual(reply, shared.flushdoneok) || IsReplyEqual(reply, shared.flushdonenok)) {
        if (server.is_doing_flushall) {
            /* clean the ssdb reply*/
            revertClientBufReply(c, revert_len);

            cur_flush_client = server.current_flushall_client;

            /* unblock the client doing flushall. */
            unblockClient(cur_flush_client);
            resetClient(cur_flush_client);
            if (IsReplyEqual(reply, shared.flushdoneok)) {
                serverLog(LL_DEBUG, "[flushall] receive do flush ok");
            } else if (IsReplyEqual(reply, shared.flushdonenok)) {
                serverLog(LL_DEBUG, "[flushall] receive do flush nok, ssdb flushall failed");
            }
            handleClientsBlockedOnFlushall();
        } else {
            /* unexpected response, revert it to avoid be send to user client. */
            revertClientBufReply(c, revert_len);
            serverLog(LL_DEBUG, "unexpected response:%s", reply->str);
        }
        process_status = C_OK;
    } else
        process_status = C_ERR;

    return process_status;
}

void doSSDBflushIfCheckDone() {
    if (server.flush_check_unresponse_num == 0) {
        server.flush_check_begin_time = -1;
        server.flush_check_unresponse_num = -1;

        serverLog(LL_DEBUG, "[flushall]all flush check responses received, check ok");

        sds finalcmd = sdsnew("*1\r\n$14\r\nrr_do_flushall\r\n");
        if (sendCommandToSSDB(server.current_flushall_client, finalcmd) != C_OK) {
            /* set to 0 so flush check will be timeout. exception case
             * of flush check will be processed in serverCron.*/
            server.flush_check_begin_time = 0;
            serverLog(LL_WARNING, "Sending rr_do_flushall to SSDB failed.");
        } else {
            serverLog(LL_WARNING, "Sending rr_do_flushall to SSDB success.");

            /* just empty redis before we receive response from SSDB, avoid dirty data issue.*/
            call(server.current_flushall_client,CMD_CALL_FULL);
            server.current_flushall_client->woff = server.master_repl_offset;
        }
    }
}

int handleResponseOfFlushCheck(client *c, redisReply* reply, int revert_len) {
    int process_status;
    UNUSED(c);

    if (IsReplyEqual(reply, shared.flushcheckok)) {
        revertClientBufReply(c, revert_len);
        if (server.is_doing_flushall) {
            server.flush_check_unresponse_num -= 1;

            /* unset this flag here, if this client disconnect after we receive flushall-check response,
             * we can ignore this client in freeClient and don't decrease server.flush_check_unresponse_num. */
            c->ssdb_conn_flags &= ~CONN_WAIT_FLUSH_CHECK_REPLY;

            serverLog(LL_DEBUG, "[flushall]receive flush check ok(c->context->fd:%d), unresponse num:%d",
                      c->context ? c->context->fd : -1, server.flush_check_unresponse_num);
            doSSDBflushIfCheckDone();
        } else {
            serverLog(LL_DEBUG, "unexpected response:%s", shared.flushcheckok);
        }
        process_status = C_OK;
    } else if (IsReplyEqual(reply, shared.flushchecknok)) {
        revertClientBufReply(c, revert_len);
        if (server.is_doing_flushall) {
            serverLog(LL_DEBUG, "[flushall]receive flush check failed response, check failed and abort");
            /* set to 0 so flush check will be timeout. exception case
             * of flush check will be processed in serverCron.*/
            server.flush_check_begin_time = 0;
        } else {
            serverLog(LL_DEBUG, "unexpected response:%s", shared.flushchecknok);
        }
        process_status = C_OK;
    } else
        process_status = C_ERR;

    return process_status;
}

void makeSSDBsnapshotIfCheckOK() {
    if (server.check_write_unresponse_num == 0) {
        server.check_write_begin_time = -1;
        server.check_write_unresponse_num = -1;
        server.ssdb_status = MASTER_SSDB_SNAPSHOT_PRE;

        sds finalcmd = sdsnew("*1\r\n$16\r\nrr_make_snapshot\r\n");
        if (sendCommandToSSDB(server.ssdb_replication_client, finalcmd) != C_OK) {
            resetCustomizedReplication();
            serverLog(LL_WARNING, "Replication log: Sending rr_make_snapshot to SSDB failed.");
        } else {
            /* will handle timeout case in replicationCron. */
            server.make_snapshot_begin_time = server.unixtime;
            serverLog(LL_DEBUG, "Replication log: Sending rr_make_snapshot to SSDB sucess.");
        }
    }
}

int handleResponseOfCheckWrite(client *c, redisReply* reply) {
    int process_status;
    UNUSED(c);

    if (IsReplyEqual(reply, shared.checkwriteok)) {
        if (server.ssdb_status == MASTER_SSDB_SNAPSHOT_CHECK_WRITE
            && !isSpecialConnection(c)
            && (c->ssdb_conn_flags & CONN_WAIT_WRITE_CHECK_REPLY)) {

            /* Update check_write_unresponse_num. */
             server.check_write_unresponse_num -= 1;

             /* unset this flag here, if this client disconnect after we receive write-check response,
              * we can ignore this client in freeClient and don't decrease server.check_write_unresponse_num. */
             c->ssdb_conn_flags &= ~CONN_WAIT_WRITE_CHECK_REPLY;

             serverLog(LL_DEBUG, "Replication log: rr_check_write fd: %d, counter: %d", c->fd,
                       server.check_write_unresponse_num);

            makeSSDBsnapshotIfCheckOK();
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.checkwriteok);

        process_status = C_OK;
    } else if (IsReplyEqual(reply, shared.checkwritenok)) {
         if (server.ssdb_status == MASTER_SSDB_SNAPSHOT_CHECK_WRITE
            && !isSpecialConnection(c)
            && (c->ssdb_conn_flags & CONN_WAIT_WRITE_CHECK_REPLY)) {

             /* Reset customized replication status immediately. */
             resetCustomizedReplication();
             serverLog(LL_WARNING, "SSDB returns 'rr_check_write nok'.");
         } else
             serverLog(LL_DEBUG, "unexpected response:%s", shared.checkwritenok);

        process_status = C_OK;
    } else
        process_status = C_ERR;

    return process_status;
}

int handleResponseOfPsync(client *c, redisReply* reply) {
    int process_status;
    UNUSED(c);

    /* Reset is_allow_ssdb_write to ALLOW_SSDB_WRITE
       as soon as rr_make_snapshot is responsed. */
    if (IsReplyEqual(reply, shared.makesnapshotok)) {
        if (c == server.ssdb_replication_client && server.ssdb_status == MASTER_SSDB_SNAPSHOT_PRE) {
            server.make_snapshot_begin_time = -1;
            server.ssdb_snapshot_timestamp = mstime();
            server.ssdb_status = MASTER_SSDB_SNAPSHOT_OK;
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.makesnapshotok);

        process_status = C_OK;
        serverLog(LL_DEBUG, "Replication log: rr_make_snapshot ok.");
    } else if (IsReplyEqual(reply, shared.makesnapshotnok)) {
        if (c == server.ssdb_replication_client && server.ssdb_status == MASTER_SSDB_SNAPSHOT_PRE) {
            /* Reset customized replication status immediately. */
            resetCustomizedReplication();
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.makesnapshotnok);

        process_status = C_OK;
    } else
        process_status = C_ERR;

    return process_status;
}

void sendDelSSDBsnapshot() {
    sds cmdsds = sdsnew("*1\r\n$15\r\nrr_del_snapshot\r\n");

    if (sendCommandToSSDB(server.ssdb_replication_client, cmdsds) != C_OK) {
        server.retry_del_snapshot = 1;
        serverLog(LL_DEBUG, "Sending rr_del_snapshot to SSDB failed. will retry!");
    } else {
        serverLog(LL_DEBUG, "Replication log: send rr_del_snapshot to SSDB");
    }
}

int handleResponseOfDelSnapshot(client *c, redisReply* reply) {
    int process_status;
    UNUSED(c);

    if (IsReplyEqual(reply, shared.delsnapshotok)) {
        if (c == server.ssdb_replication_client) {
            if (server.ssdb_status == SSDB_NONE) {
                server.retry_del_snapshot = 0;
            }
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.delsnapshotok);

        process_status = C_OK;
    } else if (IsReplyEqual(reply, shared.delsnapshotnok)) {
        if (c == server.ssdb_replication_client) {
            if (server.ssdb_status == SSDB_NONE) {
                server.retry_del_snapshot = 1;
            }
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.delsnapshotok);

        process_status = C_OK;
    } else
        process_status = C_ERR;

    return process_status;
}

int handleResponseTimeoutOfTransferSnapshot(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    client* c = clientData;
    UNUSED(eventLoop);
    UNUSED(id);

    c->repl_timer_id = -1;
    if (c->ssdb_status == SLAVE_SSDB_SNAPSHOT_TRANSFER_PRE)
        freeClientAsync(c);

    /* we only use this timer once, remove it. */
    return AE_NOMORE;
}

int handleResponseOfTransferSnapshot(client *c, redisReply* reply) {
    int process_status;

    if (IsReplyEqual(reply, shared.transfersnapshotok)) {
        if ((c->flags & CLIENT_SLAVE) && c->ssdb_status == SLAVE_SSDB_SNAPSHOT_TRANSFER_PRE) {
            if (c->repl_timer_id != -1) {
                aeDeleteTimeEvent(server.el, c->repl_timer_id);
                c->repl_timer_id = -1;
            }

            c->transfer_snapshot_last_keepalive_time = server.unixtime;
            c->ssdb_status = SLAVE_SSDB_SNAPSHOT_TRANSFER_START;

            serverLog(LL_DEBUG, "Replication log: transfersnapshotok, fd: %d", c->fd);
            aeDeleteFileEvent(server.el, c->fd, AE_WRITABLE);
            if (aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
                                  sendBulkToSlave, c) == AE_ERR)
                freeClientAsync(c);
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.transfersnapshotok);

        process_status = C_OK;

    } else if (IsReplyEqual(reply, shared.transfersnapshotnok)) {
        if ((c->flags & CLIENT_SLAVE) && c->ssdb_status == SLAVE_SSDB_SNAPSHOT_TRANSFER_PRE) {
            if (c->repl_timer_id != -1) {
                aeDeleteTimeEvent(server.el, c->repl_timer_id);
                c->repl_timer_id = -1;
            }
            serverAssert(server.ssdb_status == MASTER_SSDB_SNAPSHOT_OK);

            serverLog(LL_DEBUG, "Replication log: transfersnapshotnok, fd: %d", c->fd);
            addReplyError(c, "snapshot transfer nok");
            freeClientAsync(c);
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.transfersnapshotnok);

        process_status = C_OK;
    } else if (IsReplyEqual(reply, shared.transfersnapshotcontinue)) {
        if ((c->flags & CLIENT_SLAVE) && c->ssdb_status == SLAVE_SSDB_SNAPSHOT_TRANSFER_START) {
            serverLog(LL_DEBUG, "Replication log: receive keepalive message, transfer ssdb snapshot continue...");
            c->transfer_snapshot_last_keepalive_time = server.unixtime;
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.transfersnapshotcontinue);

        process_status = C_OK;
    } else if (IsReplyEqual(reply, shared.transfersnapshotfinished)) {
        if ((c->flags & CLIENT_SLAVE) && c->ssdb_status == SLAVE_SSDB_SNAPSHOT_TRANSFER_START) {
            serverLog(LL_DEBUG, "Replication log: snapshot transfer finished, fd: %d", c->fd);
            c->ssdb_status = SLAVE_SSDB_SNAPSHOT_TRANSFER_END;
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.transfersnapshotfinished);

        process_status = C_OK;
    } else if (IsReplyEqual(reply, shared.transfersnapshotunfinished)) {
        if ((c->flags & CLIENT_SLAVE) && c->ssdb_status == SLAVE_SSDB_SNAPSHOT_TRANSFER_START) {

            serverAssert(server.ssdb_status == MASTER_SSDB_SNAPSHOT_OK);
            serverLog(LL_DEBUG, "Replication log: snapshot transfer unfinished, fd: %d", c->fd);
            addReplyError(c, "snapshot transfer unfinished");
            freeClientAsync(c);
        } else
            serverLog(LL_DEBUG, "unexpected response:%s", shared.transfersnapshotunfinished);

        process_status = C_OK;
    } else
        process_status = C_ERR;

    return process_status;
}

int handleResponseOfExpiredDelete(client *c) {
    redisReply *reply = c->ssdb_replies[0];
    if (reply->type == REDIS_REPLY_INTEGER) {
        int j;
        serverAssert(c->cmd->proc == delCommand);
        for (j=1; j < c->argc; j++) {
            serverLog(LL_DEBUG, "expired/evicted key: %s is deleted in ssdb", (char*)c->argv[j]->ptr);
            dictDelete(EVICTED_DATA_DB->ssdb_keys_to_clean, c->argv[j]->ptr);
        }
    }
    return C_OK;
}

int handleResponseOfDeleteCheckConfirm(client *c) {
    redisReply *reply = c->ssdb_replies[0];

    if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 0) {
        robj *argv[2] = {createStringObject("del", 3), c->argv[1]};

        /* the keys is not exist in ssdb, delete its key index in redis. */
        if (server.lazyfree_lazy_eviction)
            dbAsyncDelete(EVICTED_DATA_DB, c->argv[1]);
        else
            dbSyncDelete(EVICTED_DATA_DB, c->argv[1]);

        serverLog(LL_DEBUG, "key: %s is delete from EVICTED_DATA_DB->dict.", (char *)c->argv[1]->ptr);

        propagate(server.delCommand, 0, argv, 2, PROPAGATE_REPL);
        propagate(server.delCommand, EVICTED_DATA_DBID, argv, 2, PROPAGATE_AOF);
        serverLog(LL_DEBUG, "propagate key: %s to slave", (char *)c->argv[1]->ptr);
        decrRefCount(argv[0]);
    } else if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
        serverLog(LL_DEBUG, "key: %s exists in ssdb", (char *)c->argv[1]->ptr);
    } else {
        /* response content is wrong. */
        serverLog(LL_WARNING, "[!!!]delete-confirm response content is wrong.");
    }

    serverAssert(dictDelete(EVICTED_DATA_DB->delete_confirm_keys, c->argv[1]->ptr) == DICT_OK
                 || dictDelete(server.maybe_deleted_ssdb_keys, c->argv[1]->ptr) == DICT_OK);
    serverLog(LL_DEBUG, "delete_confirm_key: %s is deleted.", (char *)c->argv[1]->ptr);
    /* Queue the ready key to ssdb_ready_keys. */
    signalBlockingKeyAsReady(c->db, c->argv[1]);

    return C_OK;
}

void checkSSDBkeyIsDeleted(char* check_reply, struct redisCommand* cmd, int argc, robj** argv) {
    int *indexs = NULL;
    int numkeys = 0;
    sds key;

    if (check_reply && !strcmp(check_reply, "check 1")) {
        indexs = getKeysFromCommand(cmd, argv, argc, &numkeys);

        key = argv[indexs[0]]->ptr;

        if (NULL == dictFind(EVICTED_DATA_DB->delete_confirm_keys, key))
            dictAddOrFind(server.maybe_deleted_ssdb_keys, key);

        serverLog(LL_DEBUG, "cmd: %s, key: %s is added to delete_confirm_keys.", cmd->name, key);

        if (indexs) getKeysFreeResult(indexs);
    }
}

/* Handle the common extra reply from SSDB:
 response format:
 1) *1\r\n$7\r\ncheck 0\r\n
 2) *1\r\n$7\r\ncheck 1\r\n
 3) for the replication connection of slave redis:
    *2\r\n$7\r\ncheck 0\r\n$100 \r\nrepopid ${time} ${index}\r\n
 */
int handleExtraSSDBReply(client *c) {
    redisReply *element0, *element1, *reply;

    reply = c->ssdb_replies[1];

    serverAssert(reply->type == REDIS_REPLY_ARRAY);
    element0 = reply->element[0];
    serverAssert(element0->type == REDIS_REPLY_STRING);
    serverLog(LL_DEBUG, "check reply:%s", element0->str);

    if (server.master == c || server.cached_master == c) {
        /* process "repopid" response for slave redis. */
        serverAssert(reply->elements == 2);
        time_t repopid_time;
        int repopid_index;
        struct ssdb_write_op* op;
        listNode *ln;
        int ret;

        element1 = reply->element[1];
        ret = sscanf(element1->str,"repopid %ld %d", &repopid_time, &repopid_index);

        serverAssert(2 == ret);
        if (2 != ret) {
            serverLog(LL_WARNING, "wrong format of repopid response :%s", reply->str);
            server.slave_ssdb_critical_err_cnt++;
            closeAndReconnectSSDBconnection(c);
            return C_ERR;
        }

#define SSDB_INITIAL_REPOPID_INDEX 0
#define SSDB_INITIAL_REPOPID_TIME 1
        if (SSDB_INITIAL_REPOPID_INDEX == repopid_index && SSDB_INITIAL_REPOPID_TIME == repopid_time) {
            /* this is a response of the first "repopid set" request. */
            return C_OK;
        }

        if (0 == listLength(server.ssdb_write_oplist)) return C_OK;

        ln = listFirst(server.ssdb_write_oplist);
        op = ln->value;
        if ((repopid_time < op->time) || (repopid_time == op->time && repopid_index < op->index)) {
            /* this may caused by a previous 'flushall' which would empty server.ssdb_write_oplist
             * and visiting_ssdb_keys. see processCommandMaybeFlushdb. */
            return C_OK;
        }

        if (repopid_index == op->index && repopid_time == op->time) {
            serverLog(LL_DEBUG, "[REPOPID DONE]ssdb process (key: %s, cmd: %s, op time:%ld, op id:%d) success,"
                              " remove from write op list", op->argc > 1 ? (sds)op->argv[1]->ptr : "",
                      op->cmd->name, op->time, op->index);
            /* for server.master connection of slave, we check whether the key is deleted when
             * there are no other write commands on this key, that is to say, when this key is
             * removed from visiting_ssdb_keys)*/
            if (removeVisitingSSDBKey(op->cmd, op->argc, op->argv))
                checkSSDBkeyIsDeleted(element0->str, op->cmd, op->argc, op->argv);
            listDelNode(server.ssdb_write_oplist, ln);
        } else {
            serverLog(LL_DEBUG, "repopid time/index don't match the first in server.ssdb_write_oplist");
            closeAndReconnectSSDBconnection(c);
            return C_ERR;
        }
    } else {
        if (!isSpecialConnection(c))
            checkSSDBkeyIsDeleted(element0->str, c->cmd, c->argc, c->argv);
    }

    return C_OK;
}

int handleResponseOfReplicationConn(client* c, redisReply* reply) {
    if (c != server.master && c != server.cached_master) return C_ERR;

    if (c->flags & CLIENT_MASTER && c->ssdb_conn_flags & CONN_CHECK_REPOPID) {
        if (reply && reply->type == REDIS_REPLY_STRING && reply->str) {
            /* we received response of "repopid get" */
            time_t last_successful_write_time = -1;
            int last_successful_write_index = -1;

            int ret = sscanf(reply->str, "repopid %ld %d", &last_successful_write_time, &last_successful_write_index);
            serverAssert(2 == ret);
            if (2 != ret) {
                server.slave_ssdb_critical_err_cnt++;
                serverLog(LL_WARNING, "wrong format of repopid check response:%s", reply->str);
                closeAndReconnectSSDBconnection(c);
            } else {
                serverLog(LL_DEBUG, "[REPOPID CHECK] get ssdb last success write(op time:%ld, op id:%d)",
                          last_successful_write_time, last_successful_write_index);

                 /* reset this flag*/
                server.slave_failed_retry_interrupted = 0;
                server.blocked_write_op = NULL;

                /* may be blocked by transfer/loading/replication, fix swap-71 relication_4-2 issue */
                if (c->flags & CLIENT_BLOCKED) {
                    if (c->btype == BLOCKED_BY_FLUSHALL) {
                        struct ssdb_write_op* op;
                        listNode *ln;
                        ln = listFirst(server.ssdb_write_oplist);
                        op = ln->value;

                        serverAssert(op->cmd->proc == flushallCommand && 1 == listLength(server.ssdb_write_oplist));
                    } else {
                        removeSuccessWriteop(last_successful_write_time, last_successful_write_index);
                        /* we will re-send failed writes to ssdb after server.master is unblocked. */
                        server.send_failed_write_after_unblock = 1;
                    }
                } else
                    confirmAndRetrySlaveSSDBwriteOp(c, last_successful_write_time, last_successful_write_index);
            }
        } else {
            serverLog(LL_WARNING, "failed to get repopid of slave ssdb, reply type:%d", reply->type);
            server.slave_ssdb_critical_err_cnt++;
            closeAndReconnectSSDBconnection(c);
        }
        c->ssdb_conn_flags &= ~CONN_CHECK_REPOPID;
        return C_OK;
    }

    if (IsReplyEqual(reply, shared.repopidsetok)) {
        /* receive "repopid setok", do nothing */
        return C_OK;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        server.slave_ssdb_critical_err_cnt++;
        serverLog(LL_WARNING, "slave ssdb write error:%s", reply->str);
    }
    if (handleResponseOfSlaveSSDBflush(c, reply) == C_RETURN)
        return C_OK;

    handleExtraSSDBReply(c);
    return C_OK;
}

int isThisKeyVisitingWriteSSDB(sds key)
{
    dictEntry *entry = dictFind(EVICTED_DATA_DB->visiting_ssdb_keys, key);
    if (!entry) return 0;

    uint32_t visiting_write_num = dictGetVisitingSSDBwriteCount(entry);
    if (visiting_write_num > 0)
        return 1;
    else
        return 0;
}

int removeVisitingSSDBKey(struct redisCommand *cmd, int argc, robj** argv) {
    int *keys = NULL, numkeys = 0, j;
    int removed = 0;

    if ( (cmd->flags & (CMD_READONLY | CMD_WRITE)) &&
         (cmd->flags & CMD_SWAP_MODE) ) {
        keys = getKeysFromCommand(cmd, argv, argc, &numkeys);
        /* in swap_mode, we only support one key command. */
        if (numkeys > 0) serverAssert(1 == numkeys);

        for (j = 0; j < numkeys; j ++) {
            robj* key = argv[keys[j]];
            dictEntry *entry = dictFind(EVICTED_DATA_DB->visiting_ssdb_keys, key->ptr);
            uint32_t visiting_write_num = dictGetVisitingSSDBwriteCount(entry);
            uint32_t visiting_read_num = dictGetVisitingSSDBreadCount(entry);

            serverAssert(entry && ((visiting_read_num >= 1 && cmd->flags & CMD_READONLY) ||
                         (visiting_write_num >= 1 && cmd->flags & CMD_WRITE)));

            if (1 == (visiting_read_num+visiting_write_num)) {
                /* only this client is visiting the specified key, remove the key
                 * from visiting keys. */
                dictDelete(EVICTED_DATA_DB->visiting_ssdb_keys, key->ptr);
                serverLog(LL_DEBUG, "key: %s is deleted from visiting_ssdb_keys.", (char *) key->ptr);

                /* if this key is in server.hot_keys, load it to redis immediately. for master only. */

                /* NOTE: if there are some clients blocked by writing on the same key,
                 * we can't load this key before process them. */
                if (dictFind(server.hot_keys, key->ptr) &&
                    NULL == dictFind(server.db[0].blocking_keys_write_same_ssdbkey, key))
                    loadThisKeyImmediately(key->ptr);
                removed = 1;
            } else {
                /* there are other clients visiting the specified key, just reduce the visiting
                 * clients num by 1. */
                if (cmd->flags & CMD_WRITE)
                    dictSetVisitingSSDBwriteCount(entry, visiting_write_num-1);
                else if (cmd->flags & CMD_READONLY)
                    dictSetVisitingSSDBreadCount(entry, visiting_read_num-1);
                removed = 0;
            }
        }

        if (keys) getKeysFreeResult(keys);
        return removed;
    }
    return -1;
}

int isSpecialConnection(client *c) {
    if (c == server.ssdb_client
        || c == server.slave_ssdb_load_evict_client
        || c == server.ssdb_replication_client
        || c == server.expired_delete_client
        || c == server.delete_confirm_client)
        return 1;
    else
        return 0;
}

int isSpecialCommand(client *c) {
    if (c && c->cmd
        && c->cmd->proc == migrateCommand)
        return 1;
    else
        return 0;
}

int handleResponseOfMigrateDump(client *c) {
    robj *keyobj = c->argv[3];
    dictEntry *de = dictFind(EVICTED_DATA_DB->dict, keyobj->ptr);
    redisDb *olddb;
    redisReply *reply = c->ssdb_replies[0];
    serverAssert(keyobj && de);

    if (reply && c->btype == BLOCKED_MIGRATING_DUMP
        && (reply->type == REDIS_REPLY_STRING
            || reply->type == REDIS_REPLY_NIL)) {
        olddb = c->db;
        c->db = EVICTED_DATA_DB;

        if (reply->type == REDIS_REPLY_NIL)
            dbSyncDelete(c->db, keyobj);

        call(c, CMD_CALL_FULL);
        c->db = olddb;
        return C_OK;
    }

    serverLog(LL_DEBUG, "c->btype: %d, reply->type: %d", c->btype, reply->type);
    return C_ERR;
}

void handleSSDBReply(client *c, int revert_len) {
    redisReply *reply;

    reply = c->ssdb_replies[0];

    if (reply && reply->type == REDIS_REPLY_ERROR)
        serverLog(LL_WARNING, "Reply from SSDB is ERROR: %s, c->fd:%d, context fd:%d",
                  reply->str, c->fd, c->context ? c->context->fd : -1);
    if (reply && reply->type == REDIS_REPLY_STRING)
        serverLog(LL_DEBUG, "reply str: %s, reply len:%lu", reply->str, reply->len);
    if (reply && reply->type == REDIS_REPLY_INTEGER)
        serverLog(LL_DEBUG, "reply integer: %lld", reply->integer);

    /* Handle special connections. */
    if (c == server.ssdb_client) return;

    if ((c == server.master || c == server.cached_master) &&
        handleResponseOfReplicationConn(c, reply) == C_OK) return;

    if (c == server.expired_delete_client && handleResponseOfExpiredDelete(c) == C_OK) {
        if (c->btype == BLOCKED_BY_EXPIRED_DELETE) {
            unblockClient(c);
            resetClient(c);
        }
        return;
    }

    if (c == server.delete_confirm_client && handleResponseOfDeleteCheckConfirm(c) == C_OK) {
        if (c->btype == BLOCKED_BY_DELETE_CONFIRM) {
            unblockClient(c);
            resetClient(c);
        }
        return;
    }

    if (reply && reply->type == REDIS_REPLY_STRING) {
        if (handleResponseOfFlushCheck(c, reply, revert_len) == C_OK) {
            return;
        }

        if (handleResponseOfSSDBflushDone(c, reply, revert_len) == C_OK ) {
            /* we need reply the flushall result to user client, so don't call revertClientBufReply. */
            return;
        }

        /* Handle the response of rr_check_write. */
        if (handleResponseOfCheckWrite(c, reply) == C_OK) {
            revertClientBufReply(c, revert_len);
            return;
        }

        /* Handle the response of rr_make_snapshot. */
        if (handleResponseOfPsync(c, reply) == C_OK) {
            return;
        }

        /* Handle the response of rr_transfer_snapshot. */
        if (handleResponseOfTransferSnapshot(c, reply) == C_OK) {
            revertClientBufReply(c, revert_len);
            return;
        }

        /* Handle the respons of rr_del_snapshot. */
        if (handleResponseOfDelSnapshot(c, reply) == C_OK) {
            return;
        }
    }

    handleExtraSSDBReply(c);

    /* Unblock the client is reading/writing SSDB. */
    if (c->btype == BLOCKED_VISITING_SSDB
        || c->btype == BLOCKED_MIGRATING_DUMP) {

        /* Handle the rest of migrating. */
        if (c->cmd->proc == migrateCommand
            && handleResponseOfMigrateDump(c) != C_OK) {
            serverLog(LL_WARNING, "migrate log: failed to handle migrate dump.");
            return;
        }

        propagateCmdHandledBySSDB(c);
        server.stat_numcommands++;
        unblockClient(c);
        resetClient(c);
        if (c->flags & CLIENT_CLOSE_AFTER_SSDB_WRITE_PROPAGATE)
            freeClientAsync(c);
    }
}

int syncReadReply(redisContext *c, void **reply, long long timeout) {
    void *aux = NULL;
    long long start = mstime();
    long long elapsed;

    /* Read until there is a reply */
    do {
        if (redisBufferRead(c) == REDIS_ERR)
            return REDIS_ERR;
        if (redisGetReplyFromReader(c,&aux,NULL) == REDIS_ERR)
            return REDIS_ERR;

        elapsed = mstime() - start;

        if (elapsed >= timeout)
            return REDIS_ERR;
    } while (aux == NULL);

    *reply = aux;

    return REDIS_OK;
}

#define AE_BUFFER_HAVE_UNPROCESSED_DATA AE_WRITABLE
void ssdbClientUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    UNUSED(fd);

    client * c = (client *)privdata;
    void *aux = NULL;
    int flags = CMD_CALL_FULL;
    long long duration;

    if (!c || !c->context)
        return;

    int total_reply_len = 0;
    redisReader *r = c->context->reader;
    char* reply_start;

    do {
        int conn_read_bytes = 0;
        int reply_len = 0;
        int oldlen = r->len;

        if (redisBufferRead(c->context) == REDIS_OK) {

            /* the returned 'aux' may be NULL when redisGetReplyFromReader return REDIS_OK,
             * so we may need to read multiple times to get a completed response. */
            conn_read_bytes = r->len - oldlen;
        }

        /* encountered a read error. */
        if (c->context->err) {
            serverLog(LL_WARNING, "ssdb read error: %s ", c->context->errstr);

            if (isSpecialConnection(c)) {
                freeClient(c);
                return;
            } else {
                if (c->ssdb_replies[0])
                    revertClientBufReply(c, c->revert_len);
                else
                    revertClientBufReply(c, total_reply_len);

                /* we don't need to reply to server.master and server.delete_confirm_client. */
                if (c->btype == BLOCKED_VISITING_SSDB
                    || c->btype == BLOCKED_MIGRATING_DUMP
                    || c->btype == BLOCKED_BY_FLUSHALL) {
                    unblockClient(c);
                    resetClient(c);
                    if (c->flags & CLIENT_CLOSE_AFTER_SSDB_WRITE_PROPAGATE) {
                        freeClient(c);
                        return;
                    }
                    /* only reply to redis user client when there is a write/read SSDB request. */
                    addReplyError(c, "SSDB disconnect when read");
                }
                closeAndReconnectSSDBconnection(c);
                goto clean;
            }
        }

        /* the returned 'aux' may be NULL when redisGetReplyFromReader return REDIS_OK */
        if (redisGetSSDBreplyFromReader(c->context, &aux, &reply_len) == REDIS_ERR)
            break;
        total_reply_len += reply_len;

       /* maybe we don't get a reply, but r->pos maybe has changed.*/
        if (!c->ssdb_replies[0]) {
            if (reply_len) {
                reply_start = r->buf+r->pos-reply_len;
                /* Forbid to reply to client when cmd is spacial,
                   the intermedia will be handled. */
                if (!isSpecialConnection(c) && !isSpecialCommand(c))
                    addReplyString(c, reply_start, reply_len);
                /*
                 * Discard part of the buffer in these two cases:
                 * 1. when we've consumed at least 1024 bytes and
                 * unprocessed data length is less than 10K bytes.
                 * 2. when we've consumed at least 1M bytes.
                 * to avoid doing unnecessary calls to memmove() in sds.c.
                 * */

                /* NOTE: discardSSDBreaderBuffer may change r->pos */
                if (r->pos >= 1024 &&
                    (r->pos > (r->len - r->pos)/10 || r->pos > 1024000))
                    discardSSDBreaderBuffer(c->context->reader, 1024);
            }
        }

        /* if we have not read any data, and there is no enough data in the reader
        * buffer to construct a integral reply, just return and avoid to waste CPU.
        *
        * we will enter this callback again if new data arrive. */
        if (!aux && conn_read_bytes == 0) {
            if (!c->ssdb_replies[0]) {
                c->revert_len += total_reply_len;
            }
            return;
        }

        /* we get a reply, record 1th reply. */
        if (aux && !c->ssdb_replies[0]) {
            /* save the first reply len and we may need to revert it from the user buffer.*/
            c->revert_len += total_reply_len;
            /* reset total reply len for the second reply. */
            total_reply_len = 0;
            c->ssdb_replies[0] = aux;
            aux = NULL;

            serverAssert(!c->ssdb_replies[1]);

            /* the returned 'aux' may be NULL when redisGetReplyFromReader return REDIS_OK */
            /* the 'redisBufferRead' may read muliple responses, so we just try to get the sencond replies. */
            if (redisGetSSDBreplyFromReader(c->context, &aux, &reply_len) == REDIS_ERR)
                break;
            total_reply_len += reply_len;
        }

        /* Record 2th reply. */
        if (aux && c->ssdb_replies[0] && !c->ssdb_replies[1]) {
            c->ssdb_replies[1] = aux;
            /* NOTE: discardSSDBreaderBuffer may change r->pos */
            if (r->pos >= 1024 &&
                (r->pos > (r->len - r->pos)/10 || r->pos > 1024000))
                discardSSDBreaderBuffer(c->context->reader, 1024);

            break;
        }
    } while (aux == NULL);

    /* this is a protocol error, free the client. */
    if (c->context->err) {
        serverLog(LL_WARNING, "redis reader protocol error!");
        freeClient(c);
        return;
    }
    serverAssert(c->ssdb_replies[0] && c->ssdb_replies[1]);

    /* check if the second reply is 'check 0' or 'check 1' */
    if (c->ssdb_replies[1]) {
        redisReply* reply = c->ssdb_replies[1];
        redisReply* element = reply->element[0];

        if ( reply->type != REDIS_REPLY_ARRAY ||
             (element->type != REDIS_REPLY_STRING || (strcmp(element->str, "check 1") && strcmp(element->str, "check 0"))) ) {
            freeClient(c);
            return;
        }
    }

    /* the redisBufferRead function may read more than two ssdb replies, the rest replies
     * will be stored in the buffer of c->context->reader, use writeable fd event
     * to trigger this callback again, to avoid the rest replies not processed. */
    if (r->len - r->pos != 0) {
        aeCreateFileEvent(server.el, c->context->fd,
                              AE_BUFFER_HAVE_UNPROCESSED_DATA, ssdbClientUnixHandler, c);
    } else {
        aeDeleteFileEvent(server.el, c->context->fd, AE_BUFFER_HAVE_UNPROCESSED_DATA);
    }

    /* When EVAL is called loading the AOF we don't want commands called
     * from Lua to go into the slowlog or to populate statistics. */
    if (server.loading && c->flags & CLIENT_LUA)
        flags &= ~(CMD_CALL_SLOWLOG | CMD_CALL_STATS);


    if (!isSpecialConnection(c) && !(c->flags & CLIENT_MASTER)) {
        duration = ustime() - c->visit_ssdb_start;
        /* Log the command into the Slow log if needed, and populate the
         * per-command statistics that we show in INFO commandstats. */
        if (flags & CMD_CALL_SLOWLOG && c->cmd && c->cmd->proc != execCommand
            && !isSpecialConnection(c) && !(c->flags & CLIENT_MASTER)) {
            char *latency_event = (c->cmd->flags & CMD_FAST) ?
                "fast-command" : "command";
            latencyAddSampleIfNeeded(latency_event,duration/1000);
            slowlogPushEntryIfNeeded(c,c->argv,c->argc,duration);
        }
    }

    handleSSDBReply(c, c->revert_len);

clean:
    c->revert_len = 0;
    if (c->ssdb_replies[0]) {
        freeReplyObject(c->ssdb_replies[0]);
        c->ssdb_replies[0] = NULL;
    }
    if (c->ssdb_replies[1]) {
        freeReplyObject(c->ssdb_replies[1]);
        c->ssdb_replies[1] = NULL;
    }
}

client* createSpecialSSDBclient() {
    client* c;

    c = createClient(-1);
    if (!c) {
        serverLog(LL_WARNING, "Error creating specical SSDB client.");
        return NULL;
    }

    nonBlockConnectToSsdbServer(c);

    return c;
}

void connectSepecialSSDBclients() {
    server.ssdb_client = createSpecialSSDBclient();
    server.ssdb_replication_client = createSpecialSSDBclient();
    server.slave_ssdb_load_evict_client = createSpecialSSDBclient();
    server.delete_confirm_client = createSpecialSSDBclient();
    server.expired_delete_client = createSpecialSSDBclient();
}

static void freeClientArgv(client *c) {
    int j;

    if (c->argv)
        for (j = 0; j < c->argc; j++)
            decrRefCount(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
}

/* Close all the slaves connections. This is useful in chained replication
 * when we resync with our own master and want to force all our slaves to
 * resync with us as well. */
void disconnectSlaves(void) {
    while (listLength(server.slaves)) {
        listNode *ln = listFirst(server.slaves);
        freeClient((client*)ln->value);
    }
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCacheMaster(). */
void unlinkClient(client *c) {
    listNode *ln;

    /* If this is marked as current client unset it. */
    if (server.current_client == c) server.current_client = NULL;

    /* Certain operations must be done only if the client has an active socket.
     * If the client was already unlinked or if it's a "fake client" the
     * fd is already set to -1. */
    if (c->fd != -1) {

        /* Remove from the new added lists in swap-mode. */
        if (server.swap_mode) {
            dictIterator *di;
            dictEntry *de;
            robj *keyobj;

            ln = listSearchKey(server.ssdb_flushall_blocked_clients, c);
            if (ln) listDelNode(server.ssdb_flushall_blocked_clients, ln);

            ln = listSearchKey(server.no_writing_ssdb_blocked_clients, c);
            if (ln) listDelNode(server.no_writing_ssdb_blocked_clients, ln);

            ln = listSearchKey(server.delayed_migrate_clients, c);
            if (ln) {
                listDelNode(server.delayed_migrate_clients, ln);
                serverLog(LL_DEBUG, "client migrate list del: %ld", (long)c);
            }

            di = dictGetSafeIterator(server.db->ssdb_blocking_keys);
            while((de = dictNext(di))) {
                keyobj = dictGetKey(de);

                removeClientFromListForBlockedKey(c, server.db->ssdb_blocking_keys, keyobj);
            }
            dictReleaseIterator(di);

            di = dictGetSafeIterator(server.db->blocking_keys_write_same_ssdbkey);
            while((de = dictNext(di))) {
                keyobj = dictGetKey(de);

                removeClientFromListForBlockedKey(c, server.db->blocking_keys_write_same_ssdbkey, keyobj);
            }
            dictReleaseIterator(di);
        }

        /* Remove from the list of active clients. */
        ln = listSearchKey(server.clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients,ln);

        /* Unregister async I/O handlers and close the socket. */
        aeDeleteFileEvent(server.el,c->fd,AE_READABLE|AE_WRITABLE);
        close(c->fd);
        c->fd = -1;
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        ln = listSearchKey(server.clients_pending_write,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_write,ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        serverAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }
}

void resetSpecialCient(client *c) {
    if (c == server.ssdb_client) {
        /* for master */
        if (!server.masterhost) cleanAndSignalLoadingOrTransferringKeys();
        server.ssdb_client = NULL;
    }

    if (c == server.ssdb_replication_client)
        server.ssdb_replication_client = NULL;

    if (c == server.slave_ssdb_load_evict_client) {
        /* for slave */
        if (server.masterhost) cleanAndSignalLoadingOrTransferringKeys();
        server.slave_ssdb_load_evict_client = NULL;
    }

    if (c == server.delete_confirm_client) {
        cleanAndSignalDeleteConfirmKeys();
        server.delete_confirm_client = NULL;
    }

    if (c == server.expired_delete_client)
        server.expired_delete_client = NULL;

    /* this is a normal client doing flushall. */
    if (c == server.current_flushall_client)
        server.current_flushall_client = NULL;
}

void freeClient(client *c) {
    listNode *ln;

    /* If it is our master that's beging disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    if (server.master && c->flags & CLIENT_MASTER) {
        serverLog(LL_WARNING,"Connection with master lost.");
        if (server.swap_mode &&
            !(c->flags & (CLIENT_CLOSE_AFTER_REPLY| CLIENT_CLOSE_ASAP))
            && server.repl_state == REPL_STATE_CONNECTED)
        {
            if (c->flags & CLIENT_BLOCKED && c->btype == BLOCKED_SSDB_LOADING_OR_TRANSFER) {
                removeBlockedKeysFromTransferOrLoadingKeys(c);

                unblockClient(c);
                if (c->flags & CLIENT_MASTER && server.slave_failed_retry_interrupted) {
                    confirmAndRetrySlaveSSDBwriteOp(c, server.blocked_write_op->time, server.blocked_write_op->index);
                } else {
                    if (runCommand(c) == C_OK)
                        resetClient(c);
                    if (c->flags & CLIENT_MASTER && server.send_failed_write_after_unblock) {
                        serverAssert(c->flags & CLIENT_MASTER && !(c->ssdb_conn_flags & CONN_SUCCESS));
                        confirmAndRetrySlaveSSDBwriteOp(c, -1,-1);
                        server.send_failed_write_after_unblock = 0;
                    }
                }
            }
            replicationCacheMaster(c);
            if (c == server.cached_master
                && c->flags & ( CLIENT_BLOCKED| CLIENT_UNBLOCKED)
                && c->querybuf && sdslen(c->querybuf) > 0)
                c->flags |= CLIENT_BUFFER_HAS_UNPROCESSED_DATA;
            return;
        } else if (!server.swap_mode
                   && !(c->flags & (CLIENT_CLOSE_AFTER_REPLY|
                                    CLIENT_CLOSE_ASAP| CLIENT_BLOCKED|
                                    CLIENT_UNBLOCKED)) && server.repl_state == REPL_STATE_CONNECTED)
        {
            replicationCacheMaster(c);
            return;
        }
    }

    /* will close after we receive reply from SSDB and propagate. */
    if (server.swap_mode && c->flags & CLIENT_BLOCKED && c->btype == BLOCKED_VISITING_SSDB) {
        c->flags |= CLIENT_CLOSE_AFTER_SSDB_WRITE_PROPAGATE;
        return;
    }

    /* Log link disconnection with slave */
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR)) {
        serverLog(LL_WARNING,"Connection with slave %s lost.",
            replicationGetSlaveName(c));
    }

    /* Free the query buffer */
    sdsfree(c->querybuf);
    sdsfree(c->pending_querybuf);
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    if (c->flags & CLIENT_BLOCKED) unblockClient(c);
    dictRelease(c->bpop.keys);

    if (server.swap_mode) dictRelease(c->bpop.loading_or_transfer_keys);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    listRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);

    /* Free data structures. */
    listRelease(c->reply);
    freeClientArgv(c);

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    if (server.swap_mode) {
        /* remove replication timeout timer. */
        if (c->repl_timer_id != -1) {
            aeDeleteTimeEvent(server.el, c->repl_timer_id);
            c->repl_timer_id = -1;
        }
        /* handle ssdb connection */
        handleSSDBconnectionDisconnect(c);
    }


    /* Master/slave cleanup Case 1:
     * we lost the connection with a slave. */
    if (c->flags & CLIENT_SLAVE) {
        if (c->replstate == SLAVE_STATE_SEND_BULK) {
            if (c->repldbfd != -1) close(c->repldbfd);
            if (c->replpreamble) sdsfree(c->replpreamble);
        }
        list *l = (c->flags & CLIENT_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        serverAssert(ln != NULL);
        listDelNode(l,ln);
        /* We need to remember the time when we started to have zero
         * attached slaves, as after some time we'll free the replication
         * backlog. */
        if (c->flags & CLIENT_SLAVE && listLength(server.slaves) == 0)
            server.repl_no_slaves_since = server.unixtime;
        refreshGoodSlavesCount();
    }

    /* Master/slave cleanup Case 2:
     * we lost the connection with the master. */
    if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection();

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. */
    if (c->flags & CLIENT_CLOSE_ASAP) {
        ln = listSearchKey(server.clients_to_close,c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close,ln);
    }

    if (server.swap_mode) {
        if (c->ssdb_replies[0]) freeReplyObject(c->ssdb_replies[0]);
        if (c->ssdb_replies[1]) freeReplyObject(c->ssdb_replies[1]);

        resetSpecialCient(c);
    }

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    if (c->name) decrRefCount(c->name);
    zfree(c->argv);
    freeClientMultiState(c);
    sdsfree(c->peerid);
    zfree(c);
}

/* Schedule a client to free it at a safe time in the serverCron() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
void freeClientAsync(client *c) {
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;
    c->flags |= CLIENT_CLOSE_ASAP;
    listAddNodeTail(server.clients_to_close,c);
}

void freeClientsInAsyncFreeQueue(void) {
    while (listLength(server.clients_to_close)) {
        listNode *ln = listFirst(server.clients_to_close);
        client *c = listNodeValue(ln);

        c->flags &= ~CLIENT_CLOSE_ASAP;
        freeClient(c);
        listDelNode(server.clients_to_close,ln);
    }
}

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed. */
int writeToClient(int fd, client *c, int handler_installed) {
    ssize_t nwritten = 0, totwritten = 0;
    size_t objlen;
    sds o;

    while(clientHasPendingReplies(c)) {
        if (c->bufpos > 0) {
            nwritten = write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if ((int)c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            o = listNodeValue(listFirst(c->reply));
            objlen = sdslen(o);

            if (objlen == 0) {
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            nwritten = write(fd, o + c->sentlen, objlen - c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == objlen) {
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
                c->reply_bytes -= objlen;
                /* If there are no longer objects in the list, we expect
                 * the count of reply bytes to be exactly zero. */
                if (listLength(c->reply) == 0)
                    serverAssert(c->reply_bytes == 0);
            }
        }
        /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interface).
         *
         * However if we are over the maxmemory limit we ignore that and
         * just deliver as much data as it is possible to deliver. */
        if (totwritten > NET_MAX_WRITES_PER_EVENT &&
            (server.maxmemory == 0 ||
             zmalloc_used_memory() < server.maxmemory)) break;
    }
    server.stat_net_output_bytes += totwritten;
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            serverLog(LL_VERBOSE,
                "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return C_ERR;
        }
    }
    if (totwritten > 0) {
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = server.unixtime;
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        if (handler_installed) aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClient(c);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Write event handler. Just send data to the client. */
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    writeToClient(fd,privdata,1);
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
int handleClientsWithPendingWrites(void) {
    listIter li;
    listNode *ln;
    int processed = listLength(server.clients_pending_write);

    listRewind(server.clients_pending_write,&li);
    while((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
        listDelNode(server.clients_pending_write,ln);

        /* Try to write buffers to the client socket. */
        if (writeToClient(c->fd,c,0) == C_ERR) continue;

        /* If there is nothing left, do nothing. Otherwise install
         * the write handler. */
        if (clientHasPendingReplies(c) &&
            aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
                sendReplyToClient, c) == AE_ERR)
        {
            freeClientAsync(c);
        }
    }
    return processed;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    //serverLog(LL_DEBUG, "resetClient called: redis fd: %d, context fd:%d", c->fd, c->context ? c->context->fd : -1);
    redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;

    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;

    if (server.swap_mode) c->first_key_index = 0;

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != askingCommand)
        c->flags &= ~CLIENT_ASKING;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */
int processInlineBuffer(client *c) {
    char *newline;
    int argc, j;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(c->querybuf,'\n');

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
            addReplyError(c,"Protocol error: too big inline request");
            setProtocolError("too big inline request",c,0);
        }
        return C_ERR;
    }

    /* Handle the \r\n case. */
    if (newline && newline != c->querybuf && *(newline-1) == '\r')
        newline--;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf);
    aux = sdsnewlen(c->querybuf,querylen);
    argv = sdssplitargs(aux,&argc);
    sdsfree(aux);
    if (argv == NULL) {
        addReplyError(c,"Protocol error: unbalanced quotes in request");
        setProtocolError("unbalanced quotes in inline request",c,0);
        return C_ERR;
    }

    /* Newline from slaves can be used to refresh the last ACK time.
     * This is useful for a slave to ping back while loading a big
     * RDB file. */
    if (querylen == 0 && c->flags & CLIENT_SLAVE)
        c->repl_ack_time = server.unixtime;

    /* Leave data after the first line of the query in the buffer */
    sdsrange(c->querybuf,querylen+2,-1);

    /* Setup argv array on client structure */
    if (argc) {
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*argc);
    }

    /* Create redis objects for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            c->argv[c->argc] = createObject(OBJ_STRING,argv[j]);
            c->argc++;
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return C_OK;
}

/* Helper function. Trims query buffer to make the function that processes
 * multi bulk requests idempotent. */
#define PROTO_DUMP_LEN 128
static void setProtocolError(const char *errstr, client *c, int pos) {
    if (server.verbosity <= LL_VERBOSE) {
        sds client = catClientInfoString(sdsempty(),c);

        /* Sample some protocol to given an idea about what was inside. */
        char buf[256];
        if (sdslen(c->querybuf) < PROTO_DUMP_LEN) {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%s'", c->querybuf);
        } else {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%.*s' (... more %zu bytes ...) '%.*s'", PROTO_DUMP_LEN/2, c->querybuf, sdslen(c->querybuf)-PROTO_DUMP_LEN, PROTO_DUMP_LEN/2, c->querybuf+sdslen(c->querybuf)-PROTO_DUMP_LEN/2);
        }

        /* Remove non printable chars. */
        char *p = buf;
        while (*p != '\0') {
            if (!isprint(*p)) *p = '.';
            p++;
        }

        /* Log all the client and protocol info. */
        serverLog(LL_VERBOSE,
            "Protocol error (%s) from client: %s. %s", errstr, client, buf);
        sdsfree(client);
    }
    c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    sdsrange(c->querybuf,pos,-1);
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        serverAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                addReplyError(c,"Protocol error: too big mbulk count string");
                setProtocolError("too big mbulk count string",c,0);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        serverAssertWithInfo(c,NULL,c->querybuf[0] == '*');
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError("invalid mbulk count",c,pos);
            return C_ERR;
        }

        pos = (newline-c->querybuf)+2;
        if (ll <= 0) {
            sdsrange(c->querybuf,pos,-1);
            return C_OK;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
    }

    serverAssertWithInfo(c,NULL,c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                    addReplyError(c,
                        "Protocol error: too big bulk count string");
                    setProtocolError("too big bulk count string",c,0);
                    return C_ERR;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
                break;

            if (c->querybuf[pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[pos]);
                setProtocolError("expected $ but got something else",c,pos);
                return C_ERR;
            }

            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError("invalid bulk length",c,pos);
                return C_ERR;
            }

            pos += newline-(c->querybuf+pos)+2;
            if (ll >= PROTO_MBULK_BIG_ARG) {
                size_t qblen;

                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data. */
                sdsrange(c->querybuf,pos,-1);
                pos = 0;
                qblen = sdslen(c->querybuf);
                /* Hint the sds library about the amount of bytes this string is
                 * going to contain. */
                if (qblen < (size_t)ll+2)
                    c->querybuf = sdsMakeRoomFor(c->querybuf,ll+2-qblen);
            }
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                (signed) sdslen(c->querybuf) == c->bulklen+2)
            {
                c->argv[c->argc++] = createObject(OBJ_STRING,c->querybuf);
                sdsIncrLen(c->querybuf,-2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(NULL,c->bulklen+2);
                sdsclear(c->querybuf);
                pos = 0;
            } else {
                c->argv[c->argc++] =
                    createStringObject(c->querybuf+pos,c->bulklen);
                pos += c->bulklen+2;
            }
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    if (pos) sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) return C_OK;

    /* Still not ready to process the command */
    return C_ERR;
}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process. */
void processInputBuffer(client *c) {
    server.current_client = c;
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->querybuf)) {
        /* Return if clients are paused. */
        if (!(c->flags & CLIENT_SLAVE) && clientsArePaused()) break;

        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & CLIENT_BLOCKED) break;

        /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands).
         *
         * The same applies for clients we want to terminate ASAP. */
        if (c->flags & (CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP)) break;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[0] == '*') {
                c->reqtype = PROTO_REQ_MULTIBULK;
            } else {
                c->reqtype = PROTO_REQ_INLINE;
            }
        }

        if (c->reqtype == PROTO_REQ_INLINE) {
            if (processInlineBuffer(c) != C_OK) break;
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != C_OK) break;
        } else {
            serverPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* Only reset the client when the command was executed. */
            if (processCommand(c) == C_OK) {
                if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
                    /* Update the applied replication offset of our master. */
                    c->reploff = c->read_reploff - sdslen(c->querybuf);
                }

                /* Don't reset the client structure for clients blocked in a
                 * module blocking command, so that the reply callback will
                 * still be able to access the client argv and argc field.
                 * The client will be reset in unblockClientFromModule(). */
                if (!(c->flags & CLIENT_BLOCKED) || c->btype != BLOCKED_MODULE)
                    resetClient(c);
            }
            /* freeMemoryIfNeeded may flush slave output buffers. This may
             * result into a slave, that may be the active client, to be
             * freed. */
            if (server.current_client == NULL) break;
        }
    }
    server.current_client = NULL;
}

void processInputBufferOfMaster(client* c) {
    serverAssert(c->flags & CLIENT_MASTER);
    serverLog(LL_DEBUG, "slave_repl_offset:%lld,master_repl_offset:%lld",
              c->reploff, server.master_repl_offset);
    size_t prev_offset = c->reploff;
    processInputBuffer(c);
    size_t applied = c->reploff - prev_offset;
    if (applied) {
        replicationFeedSlavesFromMasterStream(server.slaves,
                                              c->pending_querybuf, applied);
        sdsrange(c->pending_querybuf,applied,-1);
    }
}

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *c = (client*) privdata;
    int nread, readlen;
    size_t qblen;
    UNUSED(el);
    UNUSED(mask);

    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG)
    {
        int remaining = (unsigned)(c->bulklen+2)-sdslen(c->querybuf);

        if (remaining < readlen) readlen = remaining;
    }

    qblen = sdslen(c->querybuf);
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
    c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);
    nread = read(fd, c->querybuf+qblen, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            serverLog(LL_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        serverLog(LL_VERBOSE, "Client closed connection");
        serverLog(LL_DEBUG, "Client fd: %d closed connection.", c->fd);
        freeClient(c);
        return;
    } else if (c->flags & CLIENT_MASTER) {
        /* Append the query buffer to the pending (not applied) buffer
         * of the master. We'll use this buffer later in order to have a
         * copy of the string applied by the last command executed. */
        c->pending_querybuf = sdscatlen(c->pending_querybuf,
                                        c->querybuf+qblen,nread);
    }

    sdsIncrLen(c->querybuf,nread);
    c->lastinteraction = server.unixtime;
    if (c->flags & CLIENT_MASTER) c->read_reploff += nread;
    server.stat_net_input_bytes += nread;
    if (sdslen(c->querybuf) > server.client_max_querybuf_len) {
        sds ci = catClientInfoString(sdsempty(),c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->querybuf,64);
        serverLog(LL_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClient(c);
        return;
    }

    /* Time to process the buffer. If the client is a master we need to
     * compute the difference between the applied offset before and after
     * processing the buffer, to understand how much of the replication stream
     * was actually applied to the master state: this quantity, and its
     * corresponding part of the replication stream, will be propagated to
     * the sub-slaves and to the replication backlog. */
    if (!(c->flags & CLIENT_MASTER)) {
        processInputBuffer(c);
    } else {
        processInputBufferOfMaster(c);
    }
}

void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    client *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

/* A Redis "Peer ID" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * A Peer ID always fits inside a buffer of NET_PEER_ID_LEN bytes, including
 * the null term.
 *
 * On failure the function still populates 'peerid' with the "?:0" string
 * in case you want to relax error checking or need to display something
 * anyway (see anetPeerToString implementation for more info). */
void genClientPeerId(client *client, char *peerid,
                            size_t peerid_len) {
    if (client->flags & CLIENT_UNIX_SOCKET) {
        /* Unix socket client. */
        snprintf(peerid,peerid_len,"%s:0",server.unixsocket);
    } else {
        /* TCP client. */
        anetFormatPeer(client->fd,peerid,peerid_len);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientPeerId(client *c) {
    char peerid[NET_PEER_ID_LEN];

    if (c->peerid == NULL) {
        genClientPeerId(c,peerid,sizeof(peerid));
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* Concatenate a string representing the state of a client in an human
 * readable format, into the sds string 's'. */
sds catClientInfoString(sds s, client *client) {
    char flags[16], events[3], *p;
    int emask;

    p = flags;
    if (client->flags & CLIENT_SLAVE) {
        if (client->flags & CLIENT_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & CLIENT_MASTER) *p++ = 'M';
    if (client->flags & CLIENT_MULTI) *p++ = 'x';
    if (client->flags & CLIENT_BLOCKED) *p++ = 'b';
    if (client->flags & CLIENT_DIRTY_CAS) *p++ = 'd';
    if (client->flags & CLIENT_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & CLIENT_UNBLOCKED) *p++ = 'u';
    if (client->flags & CLIENT_CLOSE_ASAP) *p++ = 'A';
    if (client->flags & CLIENT_UNIX_SOCKET) *p++ = 'U';
    if (client->flags & CLIENT_READONLY) *p++ = 'r';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    emask = client->fd == -1 ? 0 : aeGetFileEvents(server.el,client->fd);
    p = events;
    if (emask & AE_READABLE) *p++ = 'r';
    if (emask & AE_WRITABLE) *p++ = 'w';
    *p = '\0';
    return sdscatfmt(s,
        "id=%U addr=%s fd=%i name=%s age=%I idle=%I flags=%s db=%i sub=%i psub=%i multi=%i qbuf=%U qbuf-free=%U obl=%U oll=%U omem=%U events=%s cmd=%s",
        (unsigned long long) client->id,
        getClientPeerId(client),
        client->fd,
        client->name ? (char*)client->name->ptr : "",
        (long long)(server.unixtime - client->ctime),
        (long long)(server.unixtime - client->lastinteraction),
        flags,
        client->db->id,
        (int) dictSize(client->pubsub_channels),
        (int) listLength(client->pubsub_patterns),
        (client->flags & CLIENT_MULTI) ? client->mstate.count : -1,
        (unsigned long long) sdslen(client->querybuf),
        (unsigned long long) sdsavail(client->querybuf),
        (unsigned long long) client->bufpos,
        (unsigned long long) listLength(client->reply),
        (unsigned long long) getClientOutputBufferMemoryUsage(client),
        events,
        client->lastcmd ? client->lastcmd->name : "NULL");
}

sds getAllClientsInfoString(void) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsnewlen(NULL,200*listLength(server.clients));
    sdsclear(o);
    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        o = catClientInfoString(o,client);
        o = sdscatlen(o,"\n",1);
    }
    return o;
}

void clientCommand(client *c) {
    listNode *ln;
    listIter li;
    client *client;

    if (!strcasecmp(c->argv[1]->ptr,"list") && c->argc == 2) {
        /* CLIENT LIST */
        sds o = getAllClientsInfoString();
        addReplyBulkCBuffer(c,o,sdslen(o));
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            c->flags &= ~(CLIENT_REPLY_SKIP|CLIENT_REPLY_OFF);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            c->flags |= CLIENT_REPLY_OFF;
        } else if (!strcasecmp(c->argv[2]->ptr,"skip")) {
            if (!(c->flags & CLIENT_REPLY_OFF))
                c->flags |= CLIENT_REPLY_SKIP_NEXT;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"kill")) {
        /* CLIENT KILL <ip:port>
         * CLIENT KILL <option> [value] ... <option> [value] */
        char *addr = NULL;
        int type = -1;
        uint64_t id = 0;
        int skipme = 1;
        int killed = 0, close_this_client = 0;

        if (c->argc == 3) {
            /* Old style syntax: CLIENT KILL <addr> */
            addr = c->argv[2]->ptr;
            skipme = 0; /* With the old form, you can kill yourself. */
        } else if (c->argc > 3) {
            int i = 2; /* Next option index. */

            /* New style syntax: parse options. */
            while(i < c->argc) {
                int moreargs = c->argc > i+1;

                if (!strcasecmp(c->argv[i]->ptr,"id") && moreargs) {
                    long long tmp;

                    if (getLongLongFromObjectOrReply(c,c->argv[i+1],&tmp,NULL)
                        != C_OK) return;
                    id = tmp;
                } else if (!strcasecmp(c->argv[i]->ptr,"type") && moreargs) {
                    type = getClientTypeByName(c->argv[i+1]->ptr);
                    if (type == -1) {
                        addReplyErrorFormat(c,"Unknown client type '%s'",
                            (char*) c->argv[i+1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr,"addr") && moreargs) {
                    addr = c->argv[i+1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr,"skipme") && moreargs) {
                    if (!strcasecmp(c->argv[i+1]->ptr,"yes")) {
                        skipme = 1;
                    } else if (!strcasecmp(c->argv[i+1]->ptr,"no")) {
                        skipme = 0;
                    } else {
                        addReply(c,shared.syntaxerr);
                        return;
                    }
                } else {
                    addReply(c,shared.syntaxerr);
                    return;
                }
                i += 2;
            }
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }

        /* Iterate clients killing all the matching clients. */
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client = listNodeValue(ln);
            if (addr && strcmp(getClientPeerId(client),addr) != 0) continue;
            if (type != -1 && getClientType(client) != type) continue;
            if (id != 0 && client->id != id) continue;
            if (c == client && skipme) continue;

            /* Kill it. */
            if (c == client) {
                close_this_client = 1;
            } else {
                freeClient(client);
            }
            killed++;
        }

        /* Reply according to old/new format. */
        if (c->argc == 3) {
            if (killed == 0)
                addReplyError(c,"No such client");
            else
                addReply(c,shared.ok);
        } else {
            addReplyLongLong(c,killed);
        }

        /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
         * only after we queued the reply to its output buffers. */
        if (close_this_client) c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    } else if (!strcasecmp(c->argv[1]->ptr,"setname") && c->argc == 3) {
        int j, len = sdslen(c->argv[2]->ptr);
        char *p = c->argv[2]->ptr;

        /* Setting the client name to an empty string actually removes
         * the current name. */
        if (len == 0) {
            if (c->name) decrRefCount(c->name);
            c->name = NULL;
            addReply(c,shared.ok);
            return;
        }

        /* Otherwise check if the charset is ok. We need to do this otherwise
         * CLIENT LIST format will break. You should always be able to
         * split by space to get the different fields. */
        for (j = 0; j < len; j++) {
            if (p[j] < '!' || p[j] > '~') { /* ASCII is assumed. */
                addReplyError(c,
                    "Client names cannot contain spaces, "
                    "newlines or special characters.");
                return;
            }
        }
        if (c->name) decrRefCount(c->name);
        c->name = c->argv[2];
        incrRefCount(c->name);
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"getname") && c->argc == 2) {
        if (c->name)
            addReplyBulk(c,c->name);
        else
            addReply(c,shared.nullbulk);
    } else if (!strcasecmp(c->argv[1]->ptr,"pause") && c->argc == 3) {
        long long duration;

        if (getTimeoutFromObjectOrReply(c,c->argv[2],&duration,UNIT_MILLISECONDS)
                                        != C_OK) return;
        pauseClients(duration);
        addReply(c,shared.ok);
    } else {
        addReplyError(c, "Syntax error, try CLIENT (LIST | KILL | GETNAME | SETNAME | PAUSE | REPLY)");
    }
}

/* This callback is bound to POST and "Host:" command names. Those are not
 * really commands, but are used in security attacks in order to talk to
 * Redis instances via HTTP, with a technique called "cross protocol scripting"
 * which exploits the fact that services like Redis will discard invalid
 * HTTP headers and will process what follows.
 *
 * As a protection against this attack, Redis will terminate the connection
 * when a POST or "Host:" header is seen, and will log the event from
 * time to time (to avoid creating a DOS as a result of too many logs). */
void securityWarningCommand(client *c) {
    static time_t logged_time;
    time_t now = time(NULL);

    if (labs(now-logged_time) > 60) {
        serverLog(LL_WARNING,"Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to Redis. This is likely due to an attacker attempting to use Cross Protocol Scripting to compromise your Redis instance. Connection aborted.");
        logged_time = now;
    }
    freeClientAsync(c);
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    /* We free the objects in the original vector at the end, so we are
     * sure that if the same objects are reused in the new vector the
     * refcount gets incremented before it gets decremented. */
    for (j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
    zfree(c->argv);
    /* Replace argv and argc with our new versions. */
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    freeClientArgv(c);
    zfree(c->argv);
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val ref count is incremented, and the old decremented.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv. */
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;

    if (i >= c->argc) {
        c->argv = zrealloc(c->argv,sizeof(robj*)*(i+1));
        c->argc = i+1;
        c->argv[i] = NULL;
    }
    oldval = c->argv[i];
    c->argv[i] = newval;
    incrRefCount(newval);
    if (oldval) decrRefCount(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    if (i == 0) {
        c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
        serverAssertWithInfo(c,NULL,c->cmd != NULL);
    }
}

/* This function returns the number of bytes that Redis is virtually
 * using to store the reply still not read by the client.
 * It is "virtual" since the reply output list may contain objects that
 * are shared and are not really using additional memory.
 *
 * The function returns the total sum of the length of all the objects
 * stored in the output list, plus the memory used to allocate every
 * list node. The static reply buffer is not taken into account since it
 * is allocated anyway.
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. */
unsigned long getClientOutputBufferMemoryUsage(client *c) {
    unsigned long list_item_size = sizeof(listNode)+5;
    /* The +5 above means we assume an sds16 hdr, may not be true
     * but is not going to be a problem. */

    return c->reply_bytes + (list_item_size*listLength(c->reply));
}

/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * The function will return one of the following:
 * CLIENT_TYPE_NORMAL -> Normal client
 * CLIENT_TYPE_SLAVE  -> Slave or client executing MONITOR command
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 * CLIENT_TYPE_MASTER -> The client representing our replication master.
 */
int getClientType(client *c) {
    if (c->flags & CLIENT_MASTER) return CLIENT_TYPE_MASTER;
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR))
        return CLIENT_TYPE_SLAVE;
    if (c->flags & CLIENT_PUBSUB) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

int getClientTypeByName(char *name) {
    if (!strcasecmp(name,"normal")) return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name,"slave")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"pubsub")) return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name,"master")) return CLIENT_TYPE_MASTER;
    else return -1;
}

char *getClientTypeName(int class) {
    switch(class) {
    case CLIENT_TYPE_NORMAL: return "normal";
    case CLIENT_TYPE_SLAVE:  return "slave";
    case CLIENT_TYPE_PUBSUB: return "pubsub";
    case CLIENT_TYPE_MASTER: return "master";
    default:                       return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. */
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0, class;
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    class = getClientType(c);
    /* For the purpose of output buffer limiting, masters are handled
     * like normal clients. */
    if (class == CLIENT_TYPE_MASTER) class = CLIENT_TYPE_NORMAL;

    if (server.client_obuf_limits[class].hard_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].hard_limit_bytes)
        hard = 1;
    if (server.client_obuf_limits[class].soft_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    if (soft) {
        if (c->obuf_soft_limit_reached_time == 0) {
            c->obuf_soft_limit_reached_time = server.unixtime;
            soft = 0; /* First time we see the soft limit reached */
        } else {
            time_t elapsed = server.unixtime - c->obuf_soft_limit_reached_time;

            if (elapsed <=
                server.client_obuf_limits[class].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        c->obuf_soft_limit_reached_time = 0;
    }
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers. */
void asyncCloseClientOnOutputBufferLimitReached(client *c) {
    serverAssert(c->reply_bytes < SIZE_MAX-(1024*64));
    if (c->reply_bytes == 0 || c->flags & CLIENT_CLOSE_ASAP) return;
    if (checkClientOutputBufferLimits(c)) {
        sds client = catClientInfoString(sdsempty(),c);

        freeClientAsync(c);
        serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
    }
}

/* Helper function used by freeMemoryIfNeeded() in order to flush slaves
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * slaves the latest writes. */
void flushSlavesOutputBuffers(void) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = listNodeValue(ln);
        int events;

        /* Note that the following will not flush output buffers of slaves
         * in STATE_ONLINE but having put_online_on_ack set to true: in this
         * case the writable event is never installed, since the purpose
         * of put_online_on_ack is to postpone the moment it is installed.
         * This is what we want since slaves in this state should not receive
         * writes before the first ACK. */
        events = aeGetFileEvents(server.el,slave->fd);
        if (events & AE_WRITABLE &&
            slave->replstate == SLAVE_STATE_ONLINE &&
            clientHasPendingReplies(slave))
        {
            writeToClient(slave->fd,slave,0);
        }
    }
}

/* Pause clients up to the specified unixtime (in ms). While clients
 * are paused no command is processed from clients, so the data set can't
 * change during that time.
 *
 * However while this function pauses normal and Pub/Sub clients, slaves are
 * still served, so this function can be used on server upgrades where it is
 * required that slaves process the latest bytes from the replication stream
 * before being turned to masters.
 *
 * This function is also internally used by Redis Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * In such a case, the pause is extended if the duration is more than the
 * time left for the previous duration. However if the duration is smaller
 * than the time left for the previous pause, no change is made to the
 * left duration. */
void pauseClients(mstime_t end) {
    if (!server.clients_paused || end > server.clients_pause_end_time)
        server.clients_pause_end_time = end;
    server.clients_paused = 1;
}

/* Return non-zero if clients are currently paused. As a side effect the
 * function checks if the pause time was reached and clear it. */
int clientsArePaused(void) {
    if (server.clients_paused &&
        server.clients_pause_end_time < server.mstime)
    {
        listNode *ln;
        listIter li;
        client *c;

        server.clients_paused = 0;

        /* Put all the clients in the unblocked clients queue in order to
         * force the re-processing of the input buffer if any. */
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            c = listNodeValue(ln);

            /* Don't touch slaves and blocked clients. The latter pending
             * requests be processed when unblocked. */
            if (c->flags & (CLIENT_SLAVE|CLIENT_BLOCKED)) continue;
            c->flags |= CLIENT_UNBLOCKED;
            listAddNodeTail(server.unblocked_clients,c);
        }
    }
    return server.clients_paused;
}

/* This function is called by Redis in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the master
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */
int processEventsWhileBlocked(void) {
    int iterations = 4; /* See the function top-comment. */
    int count = 0;
    while (iterations--) {
        int events = 0;
        events += aeProcessEvents(server.el, AE_FILE_EVENTS|AE_DONT_WAIT);
        events += handleClientsWithPendingWrites();
        if (!events) break;
        count += events;
    }
    return count;
}
