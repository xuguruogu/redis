set(HIREDIS_LIB_NAME libhiredis)

set(HIREDIS_HFILES
    async.h
    dict.h
    fmacros.h
    hiredis.h
    net.h
    read.h
    sds.h
    sdsalloc.h)

set(HIREDIS_CFILES
    async.c
    dict.c
    hiredis.c
    net.c
    read.c
    sds.c)

add_library(
    ${HIREDIS_LIB_NAME}
    STATIC
    ${HIREDIS_HFILES} ${HIREDIS_CFILES}
)