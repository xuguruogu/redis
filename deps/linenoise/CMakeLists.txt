set(LINENOISE_LIB_NAME liblinenoise)

set(LINENOISE_HFILES
    linenoise.h)

set(LINENOISE_CFILES
    linenoise.c)

add_library(
    ${LINENOISE_LIB_NAME}
    STATIC
    ${LINENOISE_HFILES} ${LINENOISE_CFILES}
)