/*
 * This file is part of PeerDrive.
 * Copyright (C) 2012  Jan Klötzke <jan DOT kloetzke AT freenet DOT de>
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

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <linux/types.h>
#include <linux/netlink.h>
#include <linux/rtnetlink.h>
#include <linux/if.h>
#include <linux/unistd.h>
#include <unistd.h>
#include <netlink/object-api.h>
#include <netlink/route/addr.h>
#include <netlink/route/rtnl.h>
#include <netlink/route/link.h>
#include <netlink/socket.h>

#include <erl_driver.h>

#include "netwatch_common.h"

struct ifc {
	unsigned int idx;
	struct ifc *next;
};

struct self {
	ErlDrvPort port;
	struct nl_handle *nlh;
	ErlDrvTermData ifup_ind_terms[6];
	ErlDrvTermData ifdn_ind_terms[6];
	struct ifc *alive_ifcs;
};

static struct {
	int fd;
	struct nl_handle *handle;
} stop_select_pending[3];
static ErlDrvMutex *stop_select_mutex;


static struct ifc *check_ifc_alive(struct ifc *me, unsigned int idx, int *result)
{
	if (!me) {
		me = driver_alloc(sizeof(*me));
		if (!me)
			return NULL;

		*result = 1;
		me->idx = idx;
		me->next = NULL;
	} else if (me->idx != idx)
		me->next = check_ifc_alive(me->next, idx, result);

	return me;
}

static struct ifc *check_ifc_dead(struct ifc *me, unsigned int idx, int *result)
{
	if (!me)
		return NULL;

	if (me->idx == idx) {
		struct ifc *tmp = me;
		me = me->next;
		driver_free(tmp);
		*result = 1;
	} else
		me->next = check_ifc_dead(me->next, idx, result);

	return me;
}

static void netlink_msg_handler(struct nl_object *obj, void *arg)
{
	struct self *self = arg;
	struct rtnl_link *filter;
	struct rtnl_link *link_obj;
	unsigned int ifidx;
	int is_new = 0;

	filter = rtnl_link_alloc();
	if (!filter)
		return;

	/* Ensure it's a link object */
	if (nl_object_match_filter(obj, OBJ_CAST(filter)) == 0)
		goto done;

	link_obj = (struct rtnl_link *)obj;
	ifidx = rtnl_link_get_ifindex(link_obj);

	if (rtnl_link_get_flags(link_obj) & IFF_LOWER_UP) {
		self->alive_ifcs = check_ifc_alive(self->alive_ifcs, ifidx, &is_new);
		if (is_new)
			output_term(self->port, self->ifup_ind_terms);
	} else {
		self->alive_ifcs = check_ifc_dead(self->alive_ifcs, ifidx, &is_new);
		if (is_new)
			output_term(self->port, self->ifdn_ind_terms);
	}

done:
	rtnl_link_put(filter);
}


static int netlink_msg_ready(struct nl_msg *msg, void *arg)
{
	nl_msg_parse(msg, &netlink_msg_handler, arg);

	return NL_OK;
}

static int netlink_setup(struct self *self)
{
	int err;
	struct nl_cache *cache;
	struct nl_object *obj;

	self->nlh = nl_handle_alloc();
	if (!self->nlh)
		return -ENOMEM;

	nl_disable_sequence_check(self->nlh);
	nl_socket_modify_cb(self->nlh, NL_CB_MSG_IN, NL_CB_CUSTOM, netlink_msg_ready,
		self);

	err = nl_connect(self->nlh, NETLINK_ROUTE);
	if (err < 0)
		goto err_sock_free;

	err = nl_set_passcred(self->nlh, 1);
	if (err < 0)
		goto err_sock_free;

	err = nl_socket_set_nonblocking(self->nlh);
	if (err < 0)
		goto err_sock_free;

	err = nl_socket_add_membership(self->nlh, RTNLGRP_LINK);
	if (err < 0)
		goto err_sock_free;

	/* build up list of all available interfaces */
	cache = rtnl_link_alloc_cache(self->nlh);
	if (!cache) {
		err = -ENOMEM;
		goto err_sock_free;
	}

	obj = nl_cache_get_first(cache);
	while (obj) {
		struct rtnl_link *link_obj = (struct rtnl_link *)obj;

		if (rtnl_link_get_flags(link_obj) & IFF_LOWER_UP) {
			int dummy;
			unsigned int ifidx = rtnl_link_get_ifindex(link_obj);
			self->alive_ifcs = check_ifc_alive(self->alive_ifcs, ifidx, &dummy);
		}

		obj = nl_cache_get_next(obj);
	}

	nl_cache_free(cache);

	return 0;

err_sock_free:
	nl_handle_destroy(self->nlh);
	return err;
}


static int init(void)
{
	stop_select_mutex = erl_drv_mutex_create("stop_select_mutex");
	if (!stop_select_mutex)
		return -1;

	return 0;
}

static void finish(void)
{
	erl_drv_mutex_destroy(stop_select_mutex);
}

static ErlDrvData start(ErlDrvPort port, char* cmd)
{
	struct self *self = driver_alloc(sizeof(struct self));
	struct ifc *i;

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

	if (netlink_setup(self))
		goto err_setup;

	if (driver_select(port, (ErlDrvEvent)nl_socket_get_fd(self->nlh),
	                  ERL_DRV_READ | ERL_DRV_USE, 1))
		goto err_select;

	return (ErlDrvData)self;

err_select:
	i = self->alive_ifcs;
	while (i) {
		struct ifc *tmp = i;
		i = i->next;
		driver_free(tmp);
	}
	nl_handle_destroy(self->nlh);
err_setup:
	driver_free(self);
	return ERL_DRV_ERROR_GENERAL;
}

static void stop(ErlDrvData handle)
{
	struct self *self = (struct self *)handle;
	int i, fd = nl_socket_get_fd(self->nlh);
	struct ifc *j;

	/*
	 * De-selecting and closing handles is a bit odd. The call below will
	 * deselect the netlink fd. Once it is safe to close the handles the
	 * emulator will call 'stop_select' where we can finally close the handle.
	 * Since we cannot directly call close() but instead nl_handle_destroy() we
	 * have to use a global variable to keep track of the original struct
	 * nl_handle and destroy it then...
	 */
	erl_drv_mutex_lock(stop_select_mutex);
	for (i = 0; i < 3; i++) {
		if (stop_select_pending[i].fd)
			continue;

		stop_select_pending[i].fd = fd;
		stop_select_pending[i].handle = self->nlh;
		break;
	}
	erl_drv_mutex_unlock(stop_select_mutex);
	driver_select(self->port, (ErlDrvEvent)fd, ERL_DRV_READ | ERL_DRV_USE, 0);

	j = self->alive_ifcs;
	while (j) {
		struct ifc *tmp = j;
		j = j->next;
		driver_free(tmp);
	}
	driver_free(self);
}

static void stop_select(ErlDrvEvent event, void *reserved)
{
	int i, fd = (int)event;

	erl_drv_mutex_lock(stop_select_mutex);
	for (i = 0; i < 3; i++) {
		if (stop_select_pending[i].fd == fd) {
			stop_select_pending[i].fd = 0;
			nl_handle_destroy(stop_select_pending[i].handle);
			break;
		}
	}
	erl_drv_mutex_unlock(stop_select_mutex);
}

static void ready_input(ErlDrvData handle, ErlDrvEvent event)
{
	struct self *self = (struct self *)handle;

	nl_recvmsgs_default(self->nlh);
}

static ErlDrvEntry netwatch_driver_entry = {
	init,                           /* .init            */
	start,                          /* .start           */
	stop,                           /* .stop            */
	NULL,                           /* .output          */
	ready_input,                    /* .ready_input     */
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
	stop_select                     /* .stop_select     */
};

DRIVER_INIT(netwatch_drv)
{
	return &netwatch_driver_entry;
}

