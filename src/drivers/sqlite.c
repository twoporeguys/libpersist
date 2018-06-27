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

#include <errno.h>
#include <glib.h>
#include <sqlite3.h>
#include <rpc/object.h>
#include <rpc/serializer.h>
#include "../linker_set.h"
#include "../internal.h"

#define SQL_LIST_TABLES		"SELECT * FROM sqlite_master WHERE TYPE='table';"
#define	SQL_GET			"SELECT * FROM %s WHERE id = ?;"
#define	SQL_INSERT		"INSERT INTO %s (id, value) VALUES (?, ?);"
#define SQL_DELETE		"DELETE FROM %s WHERE id = ?;"

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
		persist_set_last_error(errno, "%s", sqlite3_errstr(err));
		return (-1);
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
	struct sqlite_context *sqlite = arg;
	sqlite3_stmt *stmt;
	char *name;

	if (sqlite3_prepare_v2(sqlite->sc_db, SQL_LIST_TABLES, -1,
	    &stmt, NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	for (;;) {
		switch (sqlite3_step(stmt)) {
		case SQLITE_ROW:
			name = (char *)sqlite3_column_text(stmt, 2);
			g_ptr_array_add(result, name);
			continue;

		case SQLITE_DONE:
			goto endloop;
		}

endloop:
		break;
	}

	sqlite3_finalize(stmt);
	return (0);
}

static int
sqlite_get_object(void *arg, const char *collection, const char *id,
    rpc_object_t *obj)
{
	struct sqlite_context *sqlite = arg;
	sqlite3_stmt *stmt;
	char *sql;
	const void *blob = NULL;
	size_t len = 0;
	rpc_object_t result = NULL;
	int ret = 0;

	sql = g_strdup_printf(SQL_GET, collection);

	if (sqlite3_prepare_v2(sqlite->sc_db, sql, -1, &stmt,
	    NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	if (sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	switch (sqlite3_step(stmt)) {
	case SQLITE_ROW:
		blob = sqlite3_column_blob(stmt, 2);
		len = (size_t)sqlite3_column_bytes(stmt, 2);
		break;

	case SQLITE_DONE:
		persist_set_last_error(ENOENT, "Not found");
		ret = -1;
		break;

	case SQLITE_ERROR:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		ret = -1;
		break;

	default:
		g_assert_not_reached();
	}

	if (blob != NULL) {
		result = rpc_serializer_load("json", blob, len);
		if (result == NULL) {
			result = rpc_get_last_error();
			persist_set_last_error(rpc_error_get_code(result),
			    rpc_error_get_message(result));
			ret = -1;
		}
	}

	sqlite3_finalize(stmt);
	g_free(sql);
	*obj = result;
	return (ret);
}

static int
sqlite_save_object(void *arg, const char *collection, const char *id,
    rpc_object_t obj)
{
	struct sqlite_context *sqlite = arg;
	char *sql;
	void *buf;
	size_t len;
	sqlite3_stmt *stmt;
	rpc_object_t error;
	int ret = 0;

	if (rpc_serializer_dump("json", obj, &buf, &len) != 0) {
		error = rpc_get_last_error();
		persist_set_last_error(rpc_error_get_code(error), "%s",
		    rpc_error_get_message(error));
		return (-1);
	}

	sql = g_strdup_printf(SQL_INSERT, collection);

	if (sqlite3_prepare_v2(sqlite->sc_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	if (sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	if (sqlite3_bind_blob64(stmt, 2, buf, (uint64_t)len, SQLITE_STATIC) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	switch (sqlite3_step(stmt)) {
	case SQLITE_DONE:
		break;

	case SQLITE_ERROR:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		ret = -1;
		break;

	default:
		g_assert_not_reached();
	}

	sqlite3_finalize(stmt);
	g_free(sql);
	g_free(buf);
	return (ret);
}

static int
sqlite_delete_object(void *arg, const char *collection, const char *id)
{
	struct sqlite_context *sqlite = arg;
	char *sql;
	sqlite3_stmt *stmt;
	int ret = 0;

	sql = g_strdup_printf(SQL_DELETE, collection);

	if (sqlite3_prepare_v2(sqlite->sc_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	if (sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	switch (sqlite3_step(stmt)) {
	case SQLITE_DONE:
		break;

	case SQLITE_ERROR:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		ret = -1;
		break;

	default:
		g_assert_not_reached();
	}

	sqlite3_finalize(stmt);
	g_free(sql);
	return (ret);
}

static int
sqlite_query(void *arg, const char *collection, rpc_object_t query)
{

}

static const struct persist_driver sqlite_driver = {
	.pd_name = "sqlite",
	.pd_open = sqlite_open,
	.pd_close = sqlite_close,
	.pd_get_collections = sqlite_get_collections,
	.pd_get_object = sqlite_get_object,
	.pd_save_object = sqlite_save_object,
	.pd_delete_object = sqlite_delete_object,
	.pd_query = sqlite_query
};

DECLARE_DRIVER(sqlite_driver);
