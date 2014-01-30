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

#define __WIN32__ /* needed by erl_driver.h */

#include <erl_driver.h>

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iphlpapi.h>
#include <stdio.h>
#include <windows.h>

#include "netwatch_common.h"

#pragma comment(lib, "iphlpapi.lib")
#pragma comment(lib, "ws2_32.lib")

struct self {
	ErlDrvPort port;
	ErlDrvTermData ifupIndTems[6];
	ErlDrvTermData ifdnIndTems[6];

	DWORD *knownAddrs; /* 0 is an unused slot */
	unsigned int knownAddrsLen;

	HANDLE handle;
	WSAEVENT event;
	OVERLAPPED overlapped;
};


static DWORD AddKnownAddr(DWORD addr, struct self *self)
{
	unsigned int i;

	for (i = 0; i < self->knownAddrsLen; i++)
		if (self->knownAddrs[i] == 0)
			break;

	if (i >= self->knownAddrsLen) {
		DWORD *newArr;

		/* enlarge array */
		self->knownAddrsLen *= 2;
		newArr = driver_realloc(self->knownAddrs, self->knownAddrsLen *
			sizeof(DWORD));
		if (!newArr)
			return ERROR_NOT_ENOUGH_MEMORY;

		self->knownAddrs = newArr;
	}

	self->knownAddrs[i] = addr;
	return 0;
}

static DWORD ScanAddrTable(struct self *self, int notify)
{
	PMIB_IPADDRTABLE pIPAddrTable = NULL;
	DWORD dwSize = sizeof(MIB_IPADDRTABLE) + 4 * sizeof(MIB_IPADDRROW);
	DWORD ret = 0;
	unsigned int i, j, isNew = 0, isDown = 0;

	do {
		if (pIPAddrTable)
			driver_free(pIPAddrTable);
		pIPAddrTable = driver_alloc(dwSize);
		if (!pIPAddrTable)
			return ERROR_NOT_ENOUGH_MEMORY;
		ret = GetIpAddrTable(pIPAddrTable, &dwSize, 0);
	} while (ret == ERROR_INSUFFICIENT_BUFFER);

	if (ret != NO_ERROR) {
		driver_free(pIPAddrTable);
		return ret;
	}

	/* Scan for removed entries */
	for (i = 0; i < self->knownAddrsLen; i++) {
		DWORD addr = self->knownAddrs[i];
		if (!addr)
			continue;

		for (j = 0; j < pIPAddrTable->dwNumEntries; j++) {
			if (pIPAddrTable->table[j].wType & MIB_IPADDR_DISCONNECTED)
				continue;
			if (pIPAddrTable->table[j].dwAddr == addr)
				break;
		}

		if (j >= pIPAddrTable->dwNumEntries) {
			DBG("down: addr:%ld\n", addr);
			self->knownAddrs[i] = 0;
			isDown = 1;
		}
	}

	/* Scan for added entries */
	for (i = 0; i < pIPAddrTable->dwNumEntries; i++) {
		DWORD addr = pIPAddrTable->table[i].dwAddr;

		if (pIPAddrTable->table[i].wType & MIB_IPADDR_DISCONNECTED)
			continue;

		for (j = 0; j < self->knownAddrsLen; j++)
			if (addr == self->knownAddrs[j])
				break;

		if (j >= self->knownAddrsLen) {
			ret = AddKnownAddr(addr, self);
			if (ret)
				goto done;
			DBG("up: addr: %ld index:%ld type: %d\n", addr,
				pIPAddrTable->table[i].dwIndex,
				(int)pIPAddrTable->table[i].wType);
			isNew = 1;
		}
	}

	/* send event if some new interface went down */
	if (isDown && notify)
		output_term(self->port, self->ifdnIndTems);

	/* send event if some new interface got up */
	if (isNew && notify)
		output_term(self->port, self->ifupIndTems);

done:
	driver_free(pIPAddrTable);
	return ret;
}

static DWORD RegisterNotification(struct self *self)
{
	DWORD ret;

	memset(&self->overlapped, 0, sizeof(self->overlapped));
	self->overlapped.hEvent = self->event;

	ret = NotifyAddrChange(&self->handle, &self->overlapped);
	if (ret != ERROR_IO_PENDING)
		return ret;

	return 0;
}


static int init(void)
{
	return 0;
}

static ErlDrvData start(ErlDrvPort port, char* cmd)
{
	struct self *self = driver_alloc(sizeof(struct self));

	memset(self, 0, sizeof(*self));
	self->port = port;

	self->ifupIndTems[0] = ERL_DRV_PORT;
	self->ifupIndTems[1] = driver_mk_port(port);
	self->ifupIndTems[2] = ERL_DRV_ATOM;
	self->ifupIndTems[3] = driver_mk_atom("ifup");
	self->ifupIndTems[4] = ERL_DRV_TUPLE;
	self->ifupIndTems[5] = 2;

	self->ifdnIndTems[0] = ERL_DRV_PORT;
	self->ifdnIndTems[1] = driver_mk_port(port);
	self->ifdnIndTems[2] = ERL_DRV_ATOM;
	self->ifdnIndTems[3] = driver_mk_atom("ifdown");
	self->ifdnIndTems[4] = ERL_DRV_TUPLE;
	self->ifdnIndTems[5] = 2;

	self->knownAddrsLen = 4;
	self->knownAddrs = driver_alloc(self->knownAddrsLen * sizeof(DWORD));
	if (!self->knownAddrs)
		goto err_alloc;
	memset(self->knownAddrs, 0, self->knownAddrsLen * sizeof(DWORD));

	self->event = WSACreateEvent();
	if (self->event == WSA_INVALID_EVENT)
		goto err_event;

	if (driver_select(port, (ErlDrvEvent)self->event,
	                  ERL_DRV_READ | ERL_DRV_USE, 1))
		goto err_select;

	if (RegisterNotification(self))
		goto err_arm;

	if (ScanAddrTable(self, 0))
		goto err_scan;

	return (ErlDrvData)self;

err_scan:
	CancelIPChangeNotify(&self->overlapped);
err_arm:
	driver_select(port, (ErlDrvEvent)self->event,
	              ERL_DRV_READ | ERL_DRV_USE, 0);
err_select:
	WSACloseEvent(self->event);
err_event:
	driver_free(self->knownAddrs);
err_alloc:
	driver_free(self);
	return ERL_DRV_ERROR_GENERAL;
}

static void stop(ErlDrvData handle)
{
	struct self *self = (struct self *)handle;

	driver_select(self->port, (ErlDrvEvent)self->event,
		ERL_DRV_READ | ERL_DRV_USE, 0);
	CancelIPChangeNotify(&self->overlapped);
	driver_free(self->knownAddrs);
	driver_free(self);
}

static void stop_select(ErlDrvEvent event, void *reserved)
{
	WSACloseEvent((WSAEVENT)event);
}

static void ready_input(ErlDrvData handle, ErlDrvEvent event)
{
	struct self *self = (struct self *)handle;
	DWORD ret, tmp;

	if (GetOverlappedResult(self->handle, &self->overlapped, &tmp, FALSE)) {
		WSAResetEvent(self->event);
		ret = RegisterNotification(self);
		if (ret) {
			driver_failure(self->port, ret);
			return;
		}

		ret = ScanAddrTable(self, 1);
		if (ret)
			driver_failure(self->port, ret);
	} else {
		ret = GetLastError();
		if (ret != ERROR_IO_PENDING)
			driver_failure(self->port, ret);
	}
}

static ErlDrvEntry netwatch_driver_entry = {
	init,                           /* .init            */
	start,                          /* .start           */
	stop,                           /* .stop            */
	NULL,                           /* .output          */
	ready_input,                    /* .ready_input     */
	NULL,                           /* .ready_output    */
	"netwatch_drv",                 /* .driver_name     */
	NULL,                           /* .finish          */
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
