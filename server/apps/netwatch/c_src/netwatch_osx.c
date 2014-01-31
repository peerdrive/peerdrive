/*
 * This file is part of PeerDrive.
 * Copyright (C) 2014  Dirk Hörner <dirker AT gmail DOT com>
 *
 * PeerDrive is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * PeerDrive is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with PeerDrive. If not, see <http://www.gnu.org/licenses/>.
 */

#include <erl_driver.h>

#include <stdio.h>
#include <CoreFoundation/CoreFoundation.h>
#include <SystemConfiguration/SystemConfiguration.h>

#include "netwatch_common.h"

struct self {
	ErlDrvPort port;
	ErlDrvTermData ifup_ind_terms[6];
	ErlDrvTermData ifdn_ind_terms[6];

	SCDynamicStoreRef store;

	ErlDrvTid ifcheck_tid;
	ErlDrvMutex *ifcheck_mutex;
	ErlDrvCond *ifcheck_initialized;
	CFRunLoopSourceRef ifcheck_eventsource;
	CFRunLoopRef ifcheck_runloop;
	int ifcheck_stop;
};

static const char *to_cstr(CFStringRef cfstr)
{
	return CFStringGetCStringPtr(cfstr, kCFStringEncodingASCII);
}

static void ifcheck_change_detected(SCDynamicStoreRef store,
		CFArrayRef changed_keys, void *info)
{
	struct self *self = (struct self *)info;
	int i;

	for (i = 0; i < CFArrayGetCount(changed_keys); i++) {
		CFStringRef key;
		CFPropertyListRef value;

		key = (CFStringRef)CFArrayGetValueAtIndex(changed_keys, i);
		value = SCDynamicStoreCopyValue(store, key);

		if (value) {
			output_term(self->port, self->ifup_ind_terms);
			DBG("up:   key: %s\n", to_cstr(key));
		} else {
			output_term(self->port, self->ifdn_ind_terms);
			DBG("down: key: %s\n", to_cstr(key));
		}

		if (value)
			CFRelease(value);
	}
}

static void *ifcheck_thread(void *arg) {
	struct self *self = (struct self *)arg;
	CFRunLoopRef runloop = CFRunLoopGetCurrent();

	CFRunLoopAddSource(runloop, self->ifcheck_eventsource, kCFRunLoopCommonModes);
	self->ifcheck_runloop = runloop;

	erl_drv_cond_signal(self->ifcheck_initialized);

	do {
		CFRunLoopRun();
	} while (!self->ifcheck_stop);

	return NULL;
}

static SCDynamicStoreRef dynamicstore_create_connection(
		struct self *self,
		SCDynamicStoreCallBack callback)
{
	SCDynamicStoreRef store;
	SCDynamicStoreContext store_context;

	memset(&store_context, 0, sizeof(store_context));
	store_context.info = self;

	store = SCDynamicStoreCreate(NULL, CFSTR("netwatch_drv"), callback, &store_context);
	return store;
}

static CFRunLoopSourceRef dynamicstore_setup_notifications(SCDynamicStoreRef store)
{
	CFMutableArrayRef patterns;
	CFStringRef pattern;
	Boolean val;

	patterns = CFArrayCreateMutable(kCFAllocatorDefault, 0, &kCFTypeArrayCallBacks);

	/* notify on "State:/Network/Service/[^/]+/IPv4" change */
	pattern = SCDynamicStoreKeyCreateNetworkServiceEntity(NULL,
		kSCDynamicStoreDomainState, kSCCompAnyRegex, kSCEntNetIPv4);
	CFArrayAppendValue(patterns, pattern);
	CFRelease(pattern);

	/* notify on "State:/Network/Service/[^/]+/IPv6" change */
	pattern = SCDynamicStoreKeyCreateNetworkServiceEntity(NULL,
		kSCDynamicStoreDomainState, kSCCompAnyRegex, kSCEntNetIPv6);
	CFArrayAppendValue(patterns, pattern);
	CFRelease(pattern);

	val = SCDynamicStoreSetNotificationKeys(store, NULL, patterns);
	CFRelease(patterns);

	if (val != TRUE)
		return NULL;

	return SCDynamicStoreCreateRunLoopSource(NULL, store, 0);
}

static int init(void)
{
	return 0;
}

static void finish(void)
{
}

static ErlDrvData start(ErlDrvPort port, char *cmd)
{
	struct self *self = driver_alloc(sizeof(*self));
	int ret;

	memset(self, 0, sizeof(*self));
	self->port = port;

	self->ifup_ind_terms[0] = ERL_DRV_PORT;
	self->ifup_ind_terms[1] = driver_mk_port(port);
	self->ifup_ind_terms[2] = ERL_DRV_ATOM;
	self->ifup_ind_terms[3] = driver_mk_atom("ifup");
	self->ifup_ind_terms[4] = ERL_DRV_TUPLE;
	self->ifup_ind_terms[5] = 2;

	self->ifdn_ind_terms[0] = ERL_DRV_PORT;
	self->ifdn_ind_terms[1] = driver_mk_port(port);
	self->ifdn_ind_terms[2] = ERL_DRV_ATOM;
	self->ifdn_ind_terms[3] = driver_mk_atom("ifdown");
	self->ifdn_ind_terms[4] = ERL_DRV_TUPLE;
	self->ifdn_ind_terms[5] = 2;

	self->store = dynamicstore_create_connection(self, ifcheck_change_detected);
	if (!self->store) {
		DBG("failed connecting to dynamic store\n");
		goto err_store;
	}

	self->ifcheck_eventsource = dynamicstore_setup_notifications(self->store);
	if (!self->ifcheck_eventsource) {
		DBG("setting up notifications failed\n");
		goto err_notifications;
	}

	self->ifcheck_mutex = erl_drv_mutex_create(NULL);
	if (!self->ifcheck_mutex) {
		DBG("creating mutex failed\n");
		goto err_mutex;
	}

	self->ifcheck_initialized = erl_drv_cond_create(NULL);
	if (!self->ifcheck_initialized) {
		DBG("creating condition failed\n");
		goto err_cond;
	}

	erl_drv_mutex_lock(self->ifcheck_mutex);

	ret = erl_drv_thread_create("check", &self->ifcheck_tid,
		ifcheck_thread, self, NULL);
	if (ret != 0) {
		DBG("failed creating thread (%d)\n", ret);
		goto err_thread;
	}

	erl_drv_cond_wait(self->ifcheck_initialized, self->ifcheck_mutex);
	erl_drv_mutex_unlock(self->ifcheck_mutex);

	return (ErlDrvData)self;

err_thread:
	erl_drv_cond_destroy(self->ifcheck_initialized);
err_cond:
	erl_drv_mutex_destroy(self->ifcheck_mutex);
err_mutex:
	CFRelease(self->ifcheck_eventsource);
err_notifications:
	CFRelease(self->store);
err_store:
	driver_free(self);
	return ERL_DRV_ERROR_GENERAL;
}

static void stop(ErlDrvData handle) {
	struct self *self = (struct self *)handle;

	self->ifcheck_stop = 1;
	CFRunLoopStop(self->ifcheck_runloop);
	erl_drv_thread_join(self->ifcheck_tid, NULL);

	erl_drv_cond_destroy(self->ifcheck_initialized);
	erl_drv_mutex_destroy(self->ifcheck_mutex);
	CFRelease(self->ifcheck_eventsource);
	CFRelease(self->store);
	driver_free(self);
}

static ErlDrvEntry netwatch_driver_entry = {
	init,                           /* .init            */
	start,                          /* .start           */
	stop,                           /* .stop            */
	NULL,                           /* .output          */
	NULL,                           /* .ready_input     */
	NULL,                           /* .ready_output    */
	"netwatch_drv",                 /* .driver_name     */
	finish,                         /* .finish          */
	NULL,                           /* .handle          */
	NULL,                           /* .control         */
	NULL,                           /* .timeout         */
	NULL,                           /* .outputv         */
	NULL,                           /* .ready_async     */
	NULL,                           /* .flush           */
	NULL,                           /* .call            */
	NULL,                           /* .event           */
	ERL_DRV_EXTENDED_MARKER,        /* .extended_marker */
	ERL_DRV_EXTENDED_MAJOR_VERSION, /* .major_version   */
	ERL_DRV_EXTENDED_MINOR_VERSION, /* .minor_version   */
	ERL_DRV_FLAG_USE_PORT_LOCKING,  /* .driver_flags    */
	NULL,                           /* .handle2         */
	NULL,                           /* .process_exit    */
	NULL,                           /* .stop_select     */
};

DRIVER_INIT(netwatch_drv)
{
	return &netwatch_driver_entry;
}
