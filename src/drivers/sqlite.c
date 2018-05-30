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

#include <glib.h>
#include <sqlite3.h>
#include "../linker_set.h"
#include "../internal.h"

struct sqlite_context
{
	sqlite3 *	sc_db;
};

static int
sqlite_open(struct persist_db *db)
{
	struct sqlite_context *ctx;
	int err;

	ctx = g_malloc0(sizeof(*ctx));

	err = sqlite3_open(db->pdb_path, &ctx->sc_db);
	if (err != SQLITE_OK) {

	}

	db->pdb_arg = ctx;
	return (0);
}

static void
sqlite_close(struct persist_db *db)
{

}

static int
sqlite_get_collections(void *arg, GPtrArray *result)
{

}

static int
sqlite_get_object(void *arg, const char *collection, const char *id,
    rpc_object_t *obj)
{

}

static int
sqlite_save_object(void *arg, const char *collection, const char *id,
    rpc_object_t obj)
{

}

static const struct persist_driver sqlite_driver = {
	.pd_name = "sqlite",
	.pd_open = sqlite_open,
	.pd_close = sqlite_close,
	.pd_get_collections = sqlite_get_collections,
	.pd_get_object = sqlite_get_object,
	.pd_save_object = sqlite_save_object,
};

DECLARE_DRIVER(sqlite_driver);
