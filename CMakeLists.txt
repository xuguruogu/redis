cmake_minimum_required(VERSION 2.8)
project(redis C)

add_definitions(-Wall -g -std=c99 -pedantic)

add_library(lua
        deps/lua/src/fpconv.c
        deps/lua/src/lapi.c
        deps/lua/src/lauxlib.c
        deps/lua/src/lbaselib.c
        deps/lua/src/lcode.c
        deps/lua/src/ldblib.c
        deps/lua/src/ldebug.c
        deps/lua/src/ldo.c
        deps/lua/src/ldump.c
        deps/lua/src/lfunc.c
        deps/lua/src/lgc.c
        deps/lua/src/linit.c
        deps/lua/src/liolib.c
        deps/lua/src/llex.c
        deps/lua/src/lmathlib.c
        deps/lua/src/lmem.c
        deps/lua/src/loadlib.c
        deps/lua/src/lobject.c
        deps/lua/src/lopcodes.c
        deps/lua/src/loslib.c
        deps/lua/src/lparser.c
        deps/lua/src/lstate.c
        deps/lua/src/lstring.c
        deps/lua/src/lstrlib.c
        deps/lua/src/ltable.c
        deps/lua/src/ltablib.c
        deps/lua/src/ltm.c
        deps/lua/src/lua_bit.c
        deps/lua/src/lua_cjson.c
        deps/lua/src/lua_cmsgpack.c
        deps/lua/src/lua_struct.c
        deps/lua/src/lundump.c
        deps/lua/src/lvm.c
        deps/lua/src/lzio.c
        deps/lua/src/strbuf.c)

add_library(hiredis
        deps/hiredis/async.c
        deps/hiredis/dict.c
        deps/hiredis/hiredis.c
        deps/hiredis/net.c
        deps/hiredis/read.c
        deps/hiredis/sds.c)

execute_process(
        COMMAND sh ./mkreleasehdr.sh
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_executable(redis-server
        src/adlist.c
        src/ae.c
        src/anet.c
        src/aof.c
        src/bio.c
        src/bitops.c
        src/blocked.c
        src/childinfo.c
        src/cluster.c
        src/config.c
        src/crc16.c
        src/crc64.c
        src/db.c
        src/dict.c
        src/debug.c
        src/endianconv.c
        src/evict.c
        src/expire.c
        src/geo.c
        src/geohash.c
        src/geohash_helper.c
        src/hyperloglog.c
        src/intset.c
        src/latency.c
        src/lazyfree.c
        src/lzf_c.c
        src/lzf_d.c
        src/memtest.c
        src/module.c
        src/multi.c
        src/networking.c
        src/notify.c
        src/object.c
        src/pqsort.c
        src/pubsub.c
        src/quicklist.c
        src/rand.c
        src/rdb.c
        src/redis-check-rdb.c
        src/redis-check-aof.c
        src/release.c
        src/replication.c
        src/rio.c
        src/scripting.c
        src/sds.c
        src/server.c
        src/rax.c
        src/siphash.c
        src/defrag.c
        src/sentinel.c
        src/setproctitle.c
        src/sha1.c
        src/slowlog.c
        src/sort.c
        src/sparkline.c
        src/syncio.c
        src/t_hash.c
        src/t_list.c
        src/t_set.c
        src/t_string.c
        src/t_zset.c
        src/util.c
        src/ziplist.c
        src/zipmap.c
        src/zmalloc.c)

target_include_directories(redis-server
        PUBLIC
        deps/hiredis
        deps/lua/src)

target_compile_definitions(redis-server PUBLIC
        -DMULTIPLE_DC
        -DUSE_CLUSTER_PROTOCOL_V3)

target_link_libraries(redis-server
        hiredis
        lua
        m
        pthread
        dl)
