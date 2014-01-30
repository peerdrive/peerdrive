#ifndef __NETWATCH_COMMON_H
#define __NETWATCH_COMMON_H

#include "erl_driver.h"

#define DEBUG 0
#if DEBUG
#define DBG(fmt, ...) printf("netwatch: " fmt, ##__VA_ARGS__)
#else
#define DBG(fmt, ...) do { if (0) printf("netwatch: " fmt, ##__VA_ARGS__); } while (0)
#endif

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

/* for driver v2.1, driver_output_term() got replaced with erl_drv_output_term() */
#if ERL_DRV_EXTENDED_MAJOR_VERSION >= 2 && ERL_DRV_EXTENDED_MINOR_VERSION >= 1
#define output_term(P, T) \
	erl_drv_output_term(driver_mk_port(P), T, ARRAY_SIZE(T))
#else
#define output_term(P, T) \
	driver_output_term(P, T, ARRAY_SIZE(T))
#endif

#endif
