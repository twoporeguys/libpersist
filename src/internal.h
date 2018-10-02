/*
 * Copyright 2018 Jakub Klama <jakub.klama@gmail.com>
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef LIBPERSIST_INTERNAL_H
#define	LIBPERSIST_INTERNAL_H

#include <rpc/object.h>
#include <glib.h>
#include <persist.h>
#include "linker_set.h"

#define DECLARE_DRIVER(_driver)		DATA_SET(drv_set, _driver)
#define	COLLECTIONS			"__collections"

struct persist_db;

struct persist_driver
{
	const char *		pd_name;
	int (*pd_open)(struct persist_db *);
	void (*pd_close)(struct persist_db *);
	int (*pd_get_collections)(void *, GPtrArray *);
	int (*pd_create_collection)(void *, const char *);
	int (*pd_destroy_collection)(void *, const char *);
	int (*pd_add_index)(void *, const char *, const char *, const char *);
	int (*pd_drop_index)(void *, const char *, const char *);
	int (*pd_get_object)(void *, const char *, const char *, rpc_object_t *);
	int (*pd_save_object)(void *, const char *, const char *, rpc_object_t);
	int (*pd_save_objects)(void *, const char *, rpc_object_t);
	int (*pd_delete_object)(void *, const char *, const char *);
	int (*pd_start_tx)(void *);
	int (*pd_commit_tx)(void *);
	int (*pd_rollback_tx)(void *);
	ssize_t (*pd_count)(void *, const char *, rpc_object_t);
	void *(*pd_query)(void *, const char *, rpc_object_t, persist_query_params_t);
	int (*pd_query_next)(void *, char **, rpc_object_t *);
	void (*pd_query_close)(void *);
};

struct persist_db
{
	const struct persist_driver *	pdb_driver;
	void *				pdb_arg;
	const char *			pdb_path;
};

struct persist_collection
{
	struct persist_db *		pc_db;
	char *				pc_name;
	rpc_object_t			pc_metadata;
};

struct persist_iter
{
	struct persist_collection *	pi_col;
	void *				pi_arg;
};

const struct persist_driver *persist_find_driver(const char *name);
void persist_set_last_error(int code, const char *fmt, ...);

#endif /* LIBPERSIST_INTERNAL_H */
