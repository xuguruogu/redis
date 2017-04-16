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

#include "server.h"
#include "proxy.h"
#include "backend.h"
#include <ctype.h>

static bkReply *bkCreateReplyObject(int type);
static void *bkCreateStringObject(const bkReadTask *task, char *str, size_t len);
static void *bkCreateArrayObject(const bkReadTask *task, int elements);
static void *bkCreateIntegerObject(const bkReadTask *task, long long value);
static void *bkCreateNilObject(const bkReadTask *task);
static void *bkCreateErrorReplyObject(const char *str);

static void bkHandleRead(aeEventLoop *el, int fd, void *privdata, int mask);
static void bkHandleWrite(aeEventLoop *el, int fd, void *privdata, int mask);
static void __bkRunCallback(bkLink *link, bkReply *reply);
static int bkGetReply(bkLink *link, void **reply);
static void bkProcessCallbacks(bkLink *link);
static int bkLinkWrite(int fd, bkLink *link, int handler_installed);
int bkPrepareToWrite(bkLink *link);

/* ====================== async ======================= */

static void __bkLinkSetError(bkLink *link, const char *str) {
    /* mark as err, so that all writes be discard, callback be
     * fired whith an err reply, and waiting reconnection. */
    link->flags |= BACKEND_ERR;
    snprintf(link->errstr, sizeof(link->errstr), "-backend-link-error %s %s", str, link->name);
    /* I do not want err be handled twice. this is for development.
     * remove it in production environment. */
    serverAssert(link->flags & BACKEND_ERR);
    /* this is used to fire pending commands with err reply. */
    if (!link->replyOnFree) link->replyOnFree = bkCreateErrorReplyObject(str);
    /* add to pending white, so that all callback be fired a.s.a.p.
     * here we just use its side effect. */
    bkPrepareToWrite(link);
    /* call the onDisconnect. */
    if ((link->flags & BACKEND_CONNECTED) && link->onDisconnect)
        link->onDisconnect(link);
    serverLog(LL_WARNING, "%s", link->errstr);
}

/* do not try to connect. */
static bkLink *bkLinkCreate() {
    bkLink *link = zcalloc(sizeof(*link));
    
    link->errstr[0] = '\0';
    link->name[0] = '\0';
    
    link->rbuf = sdsempty();
    link->rmaxbuf = PROTO_IOBUF_MAX_LEN;
    link->ridx = -1;
    
    link->requests = listCreate();
    listSetFreeMethod(link->requests, decrRefCountVoid);
    link->callbacks = listCreate();
    listSetFreeMethod(link->callbacks, zfree);
    
    link->conn_time = mstime();
    
    return link;
}

static void __bkRunCallback(bkLink *link, bkReply *reply) {
    serverAssert(listLength(link->callbacks));
    listNode *ln = listFirst(link->callbacks);
    bkCallback *cb = listNodeValue(ln);
    if (cb->fn != NULL) {
        cb->fn(link, reply, cb->privdata);
    }
    listDelNode(link->callbacks, ln);
}

/*  */
static void _bkHandleCallbacksOnError(bkLink *link) {
    if (!link->replyOnFree) {
        serverAssert(!(link->flags & BACKEND_ERR));
        link->replyOnFree =
        bkCreateErrorReplyObject("Connection normal close. This should never happen "
                                 "as we lazy close after all the commands handled, "
                                 "please report this issue.");
    }
    
    while (listLength(link->callbacks)) {
        __bkRunCallback(link, link->replyOnFree);
    }
}

void bkLinkFree(bkLink *link) {
    if ((!(link->flags & BACKEND_ERR) && listLength(link->callbacks)) ||
        (link->flags & BACKEND_CLOSE_LAZY)) {
        /* make sure it can be reached again from event loop. */
        if (aeCreateFileEvent(server.el, link->fd, AE_READABLE, bkHandleRead, link) == AE_ERR) {
            __bkLinkSetError(link, "attach AE_READABLE error");
        } else {
            /* wait for all response done gently. see bkProcessCallbacks()
             * and bkHandleLinkssWithPendingWrites() which is fired when
             * reached an error. */
            link->flags |= BACKEND_CLOSE_LAZY;
            return;
        }
    }
    _bkHandleCallbacksOnError(link);
    
    if (link->fd > 0) {
        close(link->fd);
        aeDeleteFileEvent(server.el, link->fd, AE_READABLE|AE_WRITABLE);
        link->fd = 0;
    }
    
    if (link->rbuf) sdsfree(link->rbuf);
    if (link->reply) bkDecrReplyObject(link->reply);
    if (link->replyOnFree) bkDecrReplyObject(link->replyOnFree);
    if (link->requests) listRelease(link->requests);
    if (link->callbacks) listRelease(link->callbacks);
    
    if ((link->flags & BACKEND_CONNECTED) &&
        !(link->flags & BACKEND_ERR) &&
        link->onDisconnect)
        /* this is normal close. */
        link->onDisconnect(link);
    
    zfree(link);
}

/* this function always return a link. check link->flags | BACKEND_ERR. */
bkLink *bkConnectBind(char *ip, int port, char *source_addr) {
    char errstr[128];
    char self[NET_PEER_ID_LEN];
    char pear[NET_PEER_ID_LEN];
    
    bkLink *link = bkLinkCreate();
    link->fd = anetTcpNonBlockBindConnect(errstr, ip, port, source_addr);
    if (link->fd == -1) {
        __bkLinkSetError(link, errstr);
        return link;
    }
    
    anetFormatSock(link->fd, self, sizeof(self));
    anetFormatAddr(pear, sizeof(pear), ip, port);
    snprintf(link->name,sizeof(link->name), "%s->%s", self, pear);
    
    
    anetEnableTcpNoDelay(errstr, link->fd);
    if (server.tcpkeepalive) anetKeepAlive(errstr, link->fd, server.tcpkeepalive);
    
    return link;
}

void bkAttachEventLoop(bkLink *link, aeEventLoop *el) {
    if (aeCreateFileEvent(el, link->fd, AE_READABLE, bkHandleRead, link) == AE_ERR) {
        __bkLinkSetError(link, "attach AE_READABLE error");
        return;
    }
    if (aeCreateFileEvent(el, link->fd, AE_WRITABLE, bkHandleWrite, link) == AE_ERR) {
        __bkLinkSetError(link, "attach AE_WRITABLE error");
        return;
    }
}

void bkSetConnectCallback(bkLink *link, bkConnectCallback *fn) {
    link->onConnect = fn;
}

void bkSetDisconnectCallback(bkLink *link, bkDisconnectCallback *fn) {
    link->onDisconnect = fn;
}

static int __bkCheckSocketError(bkLink *link) {
    int err = 0;
    socklen_t errlen = sizeof(err);
    
    if (getsockopt(link->fd, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
        __bkLinkSetError(link, "getsockopt(SO_ERROR)");
        return C_ERR;
    }
    
    if (err) {
        errno = err;
        __bkLinkSetError(link, strerror(errno));
        return C_ERR;
    }
    
    return C_OK;
}

static int __bkHandleConnect(bkLink *link) {
    if (__bkCheckSocketError(link) == C_ERR) {
        /* Try again later when connect(2) is still in progress. */
        if (errno == EINPROGRESS)
            return C_OK;
        
        return C_ERR;
    }
    
    /* Mark context as connected. */
    link->flags |= BACKEND_CONNECTED;
    if (link->onConnect) link->onConnect(link);
    return C_OK;
}

/* may be for test. */
int bkReaderFeed(bkLink *link, const char *buf, size_t len) {
    sds newbuf;
    
    /* Return early when this reader is in an erroneous state. */
    if (link->flags & BACKEND_ERR) return C_ERR;
    
    /* Copy the provided buffer. */
    if (buf != NULL && len >= 1) {
        /* Destroy internal buffer when it is empty and is quite large. */
        if (link->rlen == 0 && link->rmaxbuf != 0 && sdsavail(link->rbuf) > link->rmaxbuf) {
            sdsfree(link->rbuf);
            link->rbuf = sdsempty();
            link->rpos = 0;
            
            /* link->buf should not be NULL since we just free'd a larger one. */
            serverAssert(link->rbuf != NULL);
        }
        
        newbuf = sdscatlen(link->rbuf, buf, len);
        link->rbuf = newbuf;
        link->rlen = sdslen(link->rbuf);
    }
    
    return C_OK;
}

static void bkProcessCallbacks(bkLink *link) {
    void *reply = NULL;
    
    while (listLength(link->callbacks)) {
        if (bkGetReply(link, &reply) == C_ERR) return;
        if (reply == NULL) break;
        
        __bkRunCallback(link, reply);
    }
    
    /* free it when we reach handle all the callbacks in lazy mode. */
    if ((link->flags & BACKEND_CLOSE_LAZY) &&
        (!listLength(link->callbacks))) {
            bkLinkFree(link);
        return;
        }
    
    /* Discard part of the buffer when we've consumed at least 1k, to avoid
     * doing unnecessary calls to memmove() in sds.c. */
    if (link->rpos >= 1024) {
        sdsrange(link->rbuf, (int) link->rpos, -1);
        link->rpos = 0;
        link->rlen = sdslen(link->rbuf);
    }
    
    /* Destroy internal buffer when it is empty and is quite large. */
    if (link->rlen == 0 && link->rmaxbuf != 0 && sdsavail(link->rbuf) > link->rmaxbuf) {
        sdsfree(link->rbuf);
        link->rbuf = sdsempty();
        link->rpos = 0;
    }
}

static void bkHandleRead(aeEventLoop *el, int fd, void *privdata, int mask) {
    bkLink *link = privdata;
    size_t nread, readlen, rblen;
    UNUSED(el);
    UNUSED(mask);
    
    /* Return early when the context has seen an error. */
    if (link->flags & BACKEND_ERR) return;
    
    if (!(link->flags & BACKEND_CONNECTED)) {
        if (__bkHandleConnect(link) != C_OK)
            return;
        /* Try again later when the context is still not connected. */
        if (!(link->flags & BACKEND_CONNECTED))
            return;
    }
    
    readlen = PROTO_IOBUF_LEN;
    rblen = sdslen(link->rbuf);
    if (link->rbuf_peak < rblen) link->rbuf_peak = rblen;
    link->rbuf = sdsMakeRoomFor(link->rbuf, readlen);
    nread = read(fd, link->rbuf + rblen, readlen);
    if (nread == -1) {
        if ((errno == EAGAIN) || (errno == EINTR)) {
            /* Try again later */
        } else {
            __bkLinkSetError(link, strerror(errno));
            return;
        }
    } else if (nread == 0) {
        __bkLinkSetError(link, "Server closed the connection");
        return;
    }
    server.stat_net_input_bytes += nread;
    /* adjust sds length */
    sdsIncrLen(link->rbuf,(int) nread);
    link->rlen = sdslen(link->rbuf);
    bkProcessCallbacks(link);
}

/* Write event handler. Just send data to the client. */
static void bkHandleWrite(aeEventLoop *el, int fd, void *privdata, int mask) {
    bkLink *link = privdata;
    UNUSED(el);
    UNUSED(mask);
    
    /* Return early when the context has seen an error. */
    if (link->flags & BACKEND_ERR) return;
    
    if (!(link->flags & BACKEND_CONNECTED)) {
        if (__bkHandleConnect(link) != C_OK)
            return;
        /* Try again later when the context is still not connected. */
        if (!(link->flags & BACKEND_CONNECTED))
            return;
    }
    
    
    bkLinkWrite(fd, privdata, 1);
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
int bkLinkHasPendingRequst(bkLink *link) {
    return link->wbufpos || listLength(link->requests);
}

/* Write data in output buffers to backend. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed. */
static int bkLinkWrite(int fd, bkLink *link, int handler_installed) {
    ssize_t nwritten = 0, totwritten = 0;
    size_t objlen;
    robj *o;
    
    /* Return early when the context has seen an error. */
    if (!(link->flags & BACKEND_CONNECTED) ||
        link->flags & BACKEND_ERR) return C_ERR;
    
    while(bkLinkHasPendingRequst(link)) {
        if (link->wbufpos > 0) {
            nwritten = write(fd, link->wbuf + link->wsentlen, link->wbufpos - link->wsentlen);
            if (nwritten <= 0) break;
            link->wsentlen += nwritten;
            totwritten += nwritten;
            
            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if ((int)link->wsentlen == link->wbufpos) {
                link->wbufpos = 0;
                link->wsentlen = 0;
            }
        } else {
            o = listNodeValue(listFirst(link->requests));
            objlen = sdslen(o->ptr);
            
            if (objlen == 0) {
                listDelNode(link->requests, listFirst(link->requests));
                continue;
            }
            
            nwritten = write(fd, ((char*)o->ptr) + link->wsentlen, objlen - link->wsentlen);
            if (nwritten <= 0) break;
            link->wsentlen += nwritten;
            totwritten += nwritten;
            
            /* If we fully sent the object on head go to the next one */
            if (link->wsentlen == objlen) {
                listDelNode(link->reply, listFirst(link->requests));
                link->wsentlen = 0;
            }
        }
        
        server.stat_net_output_bytes += totwritten;
    }
    
    if (nwritten == -1) {
        if ((errno == EAGAIN) || (errno == EINTR)) {
            nwritten = 0;
        } else {
            __bkLinkSetError(link, strerror(errno));
            return C_ERR;
        }
    }
    
    if (!bkLinkHasPendingRequst(link)) {
        link->wsentlen = 0;
        if (handler_installed) aeDeleteFileEvent(server.el, fd, AE_WRITABLE);
    }
    return C_OK;
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. see proxyBeforeSleep. */
void bkHandleLinkssWithPendingWrites(void) {
    listIter li;
    listNode *ln;
    
    listRewind(proxy.backend_pending_write, &li);
    while((ln = listNext(&li))) {
        bkLink *link = listNodeValue(ln);
        link->flags &= ~BACKEND_PENDING_WRITE;
        listDelNode(proxy.backend_pending_write, ln);
        
        /* We accect white when reached an error and handle callbacks 
         * befor sleep. */
        if (link->flags & BACKEND_ERR) {
            _bkHandleCallbacksOnError(link);
            if (link->flags & BACKEND_CLOSE_LAZY) {
                /* actually free the link when reached an error. */
                bkLinkFree(link);
            }
            continue;
        }
        
        /* Try to write buffers to the backend socket. 
         * with handler_installed due to it is fired before sleep. */
        if (bkLinkWrite(link->fd, link, 0) == C_ERR) continue;
        
        /* If there is nothing left, do nothing. Otherwise install
         * the write handler. */
        if (bkLinkHasPendingRequst(link) &&
            aeCreateFileEvent(server.el, link->fd, AE_WRITABLE,
                              bkHandleWrite, link) == AE_ERR)
        {
            __bkLinkSetError(link, strerror(errno));
            return;
        }
    }
}

/* This function is called every time we are going to transmit new data
 * to the bklink. The behavior is the following:
 *
 * Make sure to install the write handler in our event loop so that 
 * when the socket is writable new data gets written.
 *
 * The function may return C_OK without actually installing the write
 * event handler in the following cases:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contained something.
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns C_ERR no
 * data should be appended to the output buffers. */
int bkPrepareToWrite(bkLink *link) {
    /* Schedule the client to write the output buffers to the socket when
     * not already done (there were no pending writes already and the link
     * was yet not flagged), or, reached an error. */
    if (((link->flags & BACKEND_ERR)
         || !bkLinkHasPendingRequst(link)) &&
        !(link->flags & BACKEND_PENDING_WRITE))
    {
        /* Here instead of installing the write handler, we just flag the
         * bklink and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        link->flags |= BACKEND_PENDING_WRITE;
        listAddNodeHead(proxy.backend_pending_write, link);
    }
    
    /* Authorize the caller to queue in the output buffer of this client. */
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to request buffers.
 * -------------------------------------------------------------------------- */

int _bkAddRequestToBuffer(bkLink *link, const char *s, size_t len) {
    /* Ignore all request when bklink error */
    if (link->flags & BACKEND_ERR) return C_OK;
    
    size_t available = sizeof(link->wbuf) - link->wbufpos;
    
    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(link->requests) > 0) return C_ERR;
    
    /* Check that the buffer has enough space available for this string. */
    if (len > available) return C_ERR;
    
    memcpy(link->wbuf + link->wbufpos, s, len);
    link->wbufpos += len;
    return C_OK;
}

void _bkAddRequestObjectToList(bkLink *link, robj *o) {
    robj *tail;
    
    if (link->flags & BACKEND_ERR) return;
    
    if (listLength(link->requests) == 0) {
        incrRefCount(o);
        listAddNodeTail(link->requests, o);
    } else {
        tail = listNodeValue(listLast(link->requests));
        
        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            tail->encoding == OBJ_ENCODING_RAW &&
            sdslen(tail->ptr)+sdslen(o->ptr) <= PROTO_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(link->requests);
            tail->ptr = sdscatlen(tail->ptr, o->ptr, sdslen(o->ptr));
        } else {
            incrRefCount(o);
            listAddNodeTail(link->requests, o);
        }
    }
}

/* This method takes responsibility over the sds. When it is no longer
 * needed it will be free'd, otherwise it ends up in a robj. */
void _bkAddRequestSdsToList(bkLink *link, sds s) {
    robj *tail;
    
    if (link->flags & BACKEND_ERR) {
        sdsfree(s);
        return;
    }
    
    if (listLength(link->requests) == 0) {
        listAddNodeTail(link->requests, createObject(OBJ_STRING,s));
    } else {
        tail = listNodeValue(listLast(link->requests));
        
        /* Append to this object when possible. */
        if (tail->ptr != NULL && tail->encoding == OBJ_ENCODING_RAW &&
            sdslen(tail->ptr)+sdslen(s) <= PROTO_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(link->requests);
            tail->ptr = sdscatlen(tail->ptr,s,sdslen(s));
            sdsfree(s);
        } else {
            listAddNodeTail(link->requests, createObject(OBJ_STRING,s));
        }
    }
}

void _bkAddRequestStringToList(bkLink *link, const char *s, size_t len) {
    robj *tail;
    
    if (link->flags & BACKEND_ERR) return;
    
    if (listLength(link->requests) == 0) {
        robj *o = createStringObject(s,len);
        
        listAddNodeTail(link->requests,o);
    } else {
        tail = listNodeValue(listLast(link->requests));
        
        /* Append to this object when possible. */
        if (tail->ptr != NULL && tail->encoding == OBJ_ENCODING_RAW &&
            sdslen(tail->ptr)+len <= PROTO_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(link->requests);
            tail->ptr = sdscatlen(tail->ptr,s,len);
        } else {
            robj *o = createStringObject(s,len);
            
            listAddNodeTail(link->requests,o);
        }
    }
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the bklink output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

void bkAddRequest(bkLink *link, robj *obj) {
    if (bkPrepareToWrite(link) != C_OK) return;
    
    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (sdsEncodedObject(obj)) {
        if (_bkAddRequestToBuffer(link, obj->ptr, sdslen(obj->ptr)) != C_OK)
            _bkAddRequestObjectToList(link, obj);
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* Optimization: if there is room in the static buffer for 32 bytes
         * (more than the max chars a 64 bit integer can take as string) we
         * avoid decoding the object and go for the lower level approach. */
        if (listLength(link->requests) == 0 && (sizeof(link->wbuf) - link->wbufpos) >= 32) {
            char buf[32];
            int len;
            
            len = ll2string(buf,sizeof(buf),(long)obj->ptr);
            if (_bkAddRequestToBuffer(link,buf,len) == C_OK)
                return;
            /* else... continue with the normal code path, but should never
             * happen actually since we verified there is room. */
        }
        obj = getDecodedObject(obj);
        if (_bkAddRequestToBuffer(link, obj->ptr, sdslen(obj->ptr)) != C_OK)
            _bkAddRequestObjectToList(link, obj);
        decrRefCount(obj);
    } else {
        serverPanic("Wrong obj->encoding in bkAddRequest()");
    }
}

void bkAddRequestSds(bkLink *link, sds s) {
    if (bkPrepareToWrite(link) != C_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_bkAddRequestToBuffer(link, s, sdslen(s)) == C_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _bkAddRequestSdsToList(link, s);
    }
}


void bkAddRequestString(bkLink *link, const char *s, size_t len) {
    if (bkPrepareToWrite(link) != C_OK) return;
    if (_bkAddRequestToBuffer(link,s,len) != C_OK)
        _bkAddRequestStringToList(link,s,len);
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *bkAddDeferredMultiBulkLength(bkLink *link) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredMultiBulkLength() will be called. */
    if (bkPrepareToWrite(link) != C_OK) return NULL;
    listAddNodeTail(link->requests, createObject(OBJ_STRING,NULL));
    return listLast(link->requests);
}

/* Populate the length object and try gluing it to the next chunk. */
void bkSetDeferredMultiBulkLength(bkLink *link, void *node, long length) {
    listNode *ln = (listNode*)node;
    robj *len, *next;
    
    /* Abort when *node is NULL (see addDeferredMultiBulkLength). */
    if (node == NULL) return;
    
    len = listNodeValue(ln);
    len->ptr = sdscatprintf(sdsempty(),"*%ld\r\n",length);
    len->encoding = OBJ_ENCODING_RAW; /* in case it was an EMBSTR. */
    if (ln->next != NULL) {
        next = listNodeValue(ln->next);
        
        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next->ptr != NULL) {
            len->ptr = sdscatlen(len->ptr,next->ptr,sdslen(next->ptr));
            listDelNode(link->requests,ln->next);
        }
    }
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void bkAddRequstLongLongWithPrefix(bkLink *link, long long ll, char prefix) {
    char buf[128];
    int len;
    
    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        bkAddRequest(link, shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        bkAddRequest(link, shared.bulkhdr[ll]);
        return;
    }
    
    buf[0] = prefix;
    len = ll2string(buf+1, sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    bkAddRequestString(link, buf, len+3);
}

void bkAddReplyMultiBulkLen(bkLink *link, long length) {
    if (length < OBJ_SHARED_BULKHDR_LEN)
        bkAddRequest(link, shared.mbulkhdr[length]);
    else
        bkAddRequstLongLongWithPrefix(link, length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void bkAddRequestBulkLen(bkLink *link, robj *obj) {
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
        bkAddRequest(link, shared.bulkhdr[len]);
    else
        bkAddRequstLongLongWithPrefix(link, len, '$');
}

/* Add a Redis Object as a bulk reply */
void bkAddRequestBulk(bkLink *link, robj *obj) {
    bkAddRequestBulkLen(link,obj);
    bkAddRequest(link,obj);
    bkAddRequest(link,shared.crlf);
}

/* Add a C buffer as bulk reply */
void bkAddRequestBulkCBuffer(bkLink *link, const void *p, size_t len) {
    bkAddRequstLongLongWithPrefix(link,len,'$');
    bkAddRequestString(link,p,len);
    bkAddRequest(link,shared.crlf);
}

/* Add sds to reply (takes ownership of sds and frees it) */
void bkAddRequestBulkSds(bkLink *link, sds s)  {
    bkAddRequestSds(link,sdscatfmt(sdsempty(),"$%u\r\n",
                            (unsigned long)sdslen(s)));
    bkAddRequestSds(link,s);
    bkAddRequest(link,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void bkAddRequestBulkCString(bkLink *link, const char *s) {
    if (s == NULL) {
        bkAddRequest(link, shared.nullbulk);
    } else {
        bkAddRequestBulkCBuffer(link,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void bkAddRequestBulkLongLong(bkLink *link, long long ll) {
    char buf[64];
    int len;
    
    len = ll2string(buf,64,ll);
    bkAddRequestBulkCBuffer(link,buf,len);
}

/* ====================== add call back ======================= */

void bkAddCallback(bkLink *link, bkCallbackFn *fn, void *privdata) {
    bkCallback *cb = zmalloc(sizeof(*cb));
    cb->fn = fn;
    cb->privdata = privdata;
    listAddNodeTail(link->callbacks, cb);
}

/* ====================== object ======================= */

/* Create a reply object */
static bkReply *bkCreateReplyObject(int type) {
    bkReply *r = zcalloc(sizeof(*r));
    
    r->refcount = 1;
    r->type = type;
    return r;
}

void bkIncrReplyObject(void *ptr) {
    bkReply *r = ptr;
    r->refcount++;
}

/* used to free cmd object, this called when free cmd list and
 * called back from hiredis. this should be called every time
 * registered into hiredis. */
void bkDecrReplyObject(void *ptr) {
    bkReply *r = ptr;
    
    if (r->refcount <= 0) serverPanic("bkDecrReplyObject against refcount <= 0");
    if (r->refcount == 1) {
        size_t j;
        switch(r->type) {
            case PROTO_REPLY_INTEGER:
                break; /* Nothing to free */
            case PROTO_REPLY_ARRAY:
                if (r->element != NULL) {
                    for (j = 0; j < r->elements; j++)
                        if (r->element[j] != NULL)
                            bkDecrReplyObject(r->element[j]);
                    zfree(r->element);
                }
                break;
            case PROTO_REPLY_ERROR:
            case PROTO_REPLY_STATUS:
            case PROTO_REPLY_STRING:
                if (r->str != NULL)
                    zfree(r->str);
                break;
        }
        zfree(r);
    } else {
        r->refcount--;
    }
}

static void *bkCreateErrorReplyObject(const char *str) {
    bkReply *r;
    char *buf;
    size_t len = strlen(str);
    
    r = bkCreateReplyObject(PROTO_REPLY_ERROR);
    buf = zmalloc(len+1);
    
    memcpy(buf,str,len);
    buf[len] = '\0';
    r->str = buf;
    r->len = len;
    
    return r;
}

static void *bkCreateStringObject(const bkReadTask *task, char *str, size_t len) {
    bkReply *r, *parent;
    char *buf;
    
    serverAssert(task->type == PROTO_REPLY_ERROR  ||
                 task->type == PROTO_REPLY_STATUS ||
                 task->type == PROTO_REPLY_STRING);

    r = bkCreateReplyObject(task->type);
    buf = zmalloc(len+1);
    
    memcpy(buf,str,len);
    buf[len] = '\0';
    r->str = buf;
    r->len = len;
    
    if (task->parent) {
        parent = task->parent->obj;
        serverAssert(parent->type == PROTO_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *bkCreateArrayObject(const bkReadTask *task, int elements) {
    bkReply *r, *parent;

    r = bkCreateReplyObject(PROTO_REPLY_ARRAY);
    if (elements > 0) {
        r->element = zcalloc(elements * sizeof(bkReply*));
    }
    r->elements = elements;

    if (task->parent) {
        parent = task->parent->obj;
        serverAssert(parent->type == PROTO_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *bkCreateIntegerObject(const bkReadTask *task, long long value) {
    bkReply *r, *parent;

    r = bkCreateReplyObject(PROTO_REPLY_INTEGER);
    r->integer = value;

    if (task->parent) {
        parent = task->parent->obj;
        serverAssert(parent->type == PROTO_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *bkCreateNilObject(const bkReadTask *task) {
    bkReply *r, *parent;

    r = bkCreateReplyObject(PROTO_REPLY_NIL);
    if (task->parent) {
        parent = task->parent->obj;
        serverAssert(parent->type == PROTO_REPLY_ARRAY);
        parent->element[task->idx] = r;
    }
    return r;
}

/* ====================== parser ======================= */

static char *readBytes(bkLink *link, unsigned int bytes) {
    char *p;
    if (link->rlen - link->rpos >= bytes) {
        p = link->rbuf + link->rpos;
        link->rpos += bytes;
        return p;
    }
    return NULL;
}

/* Find pointer to \r\n. */
static char *seekNewline(char *s, size_t len) {
    int pos = 0;
    size_t _len = len-1;

    /* Position should be < len-1 because the character at "pos" should be
     * followed by a \n. Note that strchr cannot be used because it doesn't
     * allow to search a limited length and the buffer that is being searched
     * might not have a trailing NULL character. */
    while (pos < _len) {
        while(pos < _len && s[pos] != '\r') pos++;
        if (s[pos] != '\r') {
            /* Not found. */
            return NULL;
        } else {
            if (s[pos+1] == '\n') {
                /* Found. */
                return s+pos;
            } else {
                /* Continue searching. */
                pos++;
            }
        }
    }
    return NULL;
}

/* Read a long long value starting at *s, under the assumption that it will be
 * terminated by \r\n. Ambiguously returns -1 for unexpected input. */
static long long readLongLong(char *s) {
    long long v = 0;
    int dec, mult = 1;
    char c;

    if (*s == '-') {
        mult = -1;
        s++;
    } else if (*s == '+') {
        mult = 1;
        s++;
    }

    while ((c = *(s++)) != '\r') {
        dec = c - '0';
        if (dec >= 0 && dec < 10) {
            v *= 10;
            v += dec;
        } else {
            /* Should not happen... */
            return -1;
        }
    }

    return mult*v;
}

static char *readLine(bkLink *link, size_t *_len) {
    char *p, *s;
    size_t len;

    p = link->rbuf + link->rpos;
    s = seekNewline(p,(link->rlen-link->rpos));
    if (s != NULL) {
        len = s-(link->rbuf + link->rpos);
        link->rpos += len+2; /* skip \r\n */
        if (_len) *_len = len;
        return p;
    }
    return NULL;
}

static void moveToNextTask(bkLink *link) {
    bkReadTask *cur, *prv;
    while (link->ridx >= 0) {
        /* Return a.s.a.p. when the stack is now empty. */
        if (link->ridx == 0) {
            link->ridx--;
            return;
        }

        cur = &(link->rstack[link->ridx]);
        prv = &(link->rstack[link->ridx-1]);
        serverAssert(prv->type == PROTO_REPLY_ARRAY);
        if (cur->idx == prv->elements-1) {
            link->ridx--;
        } else {
            /* Reset the type because the next item can be anything */
            serverAssert(cur->idx < prv->elements);
            cur->type = -1;
            cur->elements = -1;
            cur->idx++;
            return;
        }
    }
}

static int processLineItem(bkLink *link) {
    bkReadTask *cur = &(link->rstack[link->ridx]);
    void *obj;
    char *p;
    size_t len;

    if ((p = readLine(link,&len)) != NULL) {
        if (cur->type == PROTO_REPLY_INTEGER) {
            obj = bkCreateIntegerObject(cur, readLongLong(p));
        } else {
            /* Type will be error or status. */
            obj = bkCreateStringObject(cur, p, len);
        }

        /* Set reply if this is the root object. */
        if (link->ridx == 0) link->reply = obj;
        moveToNextTask(link);
        return C_OK;
    }

    return C_ERR;
}

static int processBulkItem(bkLink *link) {
    bkReadTask *cur = &(link->rstack[link->ridx]);
    void *obj = NULL;
    char *p, *s;
    long len;
    unsigned long bytelen;
    int success = 0;

    p = link->rbuf + link->rpos;
    s = seekNewline(p,link->rlen - link->rpos);
    if (s != NULL) {
        p = link->rbuf + link->rpos;
        bytelen = s-(link->rbuf + link->rpos)+2; /* include \r\n */
        len = readLongLong(p);

        if (len < 0) {
            /* The nil object can always be created. */
            obj = bkCreateNilObject(cur);
            success = 1;
        } else {
            /* Only continue when the buffer contains the entire bulk item. */
            bytelen += len+2; /* include \r\n */
            if (link->rpos + bytelen <= link->rlen) {
                obj = bkCreateStringObject(cur, s+2, len);
                success = 1;
            }
        }

        /* Proceed when obj was created. */
        if (success) {
            link->rpos += bytelen;

            /* Set reply if this is the root object. */
            if (link->ridx == 0) link->reply = obj;
            moveToNextTask(link);
            return C_OK;
        }
    }

    return C_ERR;
}

static int processMultiBulkItem(bkLink *link) {
    bkReadTask *cur = &(link->rstack[link->ridx]);
    void *obj;
    char *p;
    int elements;
    int root = 0;

    /* Set error for nested multi bulks with depth > 7 */
    if (link->ridx == 8) {
        __bkLinkSetError(link, "Protocol: No support for nested multi bulk replies with depth > 7");
        return C_ERR;
    }

    if ((p = readLine(link,NULL)) != NULL) {
        elements = (int) readLongLong(p);
        root = (link->ridx == 0);

        if (elements == -1) {
            obj = bkCreateNilObject(cur);
            moveToNextTask(link);
        } else {
            obj = bkCreateArrayObject(cur, elements);
            /* Modify task stack when there are more than 0 elements. */
            if (elements > 0) {
                cur->elements = elements;
                cur->obj = obj;
                link->ridx++;
                link->rstack[link->ridx].type = -1;
                link->rstack[link->ridx].elements = -1;
                link->rstack[link->ridx].idx = 0;
                link->rstack[link->ridx].obj = NULL;
                link->rstack[link->ridx].parent = cur;
            } else {
                moveToNextTask(link);
            }
        }

        /* Set reply if this is the root object. */
        if (root) link->reply = obj;
        return C_OK;
    }

    return C_ERR;
}

static int processItem(bkLink *link) {
    bkReadTask *cur = &(link->rstack[link->ridx]);
    char *p;
    sds s;

    /* check if we need to read type */
    if (cur->type < 0) {
        if ((p = readBytes(link,1)) != NULL) {
            switch (p[0]) {
            case '-':
                cur->type = PROTO_REPLY_ERROR;
                break;
            case '+':
                cur->type = PROTO_REPLY_STATUS;
                break;
            case ':':
                cur->type = PROTO_REPLY_INTEGER;
                break;
            case '$':
                cur->type = PROTO_REPLY_STRING;
                break;
            case '*':
                cur->type = PROTO_REPLY_ARRAY;
                break;
            default:
                s = sdsnew("Protocol: reply type byte unpected: ");
                s = sdscatrepr(s, p, 1);
                __bkLinkSetError(link, s);
                sdsfree(s);
                return C_ERR;
            }
        } else {
            /* could not consume 1 byte */
            return C_ERR;
        }
    }

    /* process typed item */
    switch(cur->type) {
        case PROTO_REPLY_ERROR:
        case PROTO_REPLY_STATUS:
        case PROTO_REPLY_INTEGER:
            return processLineItem(link);
        case PROTO_REPLY_STRING:
            return processBulkItem(link);
        case PROTO_REPLY_ARRAY:
            return processMultiBulkItem(link);
        default:
            serverPanic("unrecognized type");
    }
}

static int bkGetReply(bkLink *link, void **reply) {
    /* Default target pointer to NULL. */
    if (reply != NULL)
        *reply = NULL;

    /* Return early when this reader is in an erroneous state. */
    if (link->flags & BACKEND_ERR) return  C_ERR;

    /* When the buffer is empty, there will never be a reply. */
    if (link->rlen == 0) return C_OK;

    /* Set first item to process when the stack is empty. */
    if (link->ridx == -1) {
        link->rstack[0].type = -1;
        link->rstack[0].elements = -1;
        link->rstack[0].idx = -1;
        link->rstack[0].obj = NULL;
        link->rstack[0].parent = NULL;
        link->ridx = 0;
    }

    /* Process items in reply. */
    while (link->ridx >= 0)
        if (processItem(link) != C_OK)
            break;

    /* Return ASAP when an error occurred. */
    if (link->flags & BACKEND_ERR) return  C_ERR;

    /* Emit a reply when there is one. */
    if (link->ridx == -1) {
        if (reply != NULL)
            *reply = link->reply;
        link->reply = NULL;
    }
    return C_OK;
}
