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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <glib.h>
#include <sqlite3.h>
#include <rpc/object.h>
#include <rpc/serializer.h>
#include "../linker_set.h"
#include "../internal.h"

#define SQL_CREATE_TABLE	"CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY, value TEXT);"
#define SQL_LIST_TABLES		"SELECT * FROM sqlite_master WHERE TYPE='table';"
#define SQL_GET			"SELECT * FROM %s WHERE id = ?;"
#define	SQL_QUERY		"SELECT * FROM %s;"
#define SQL_INSERT		"INSERT INTO %s (id, value) VALUES (?, ?);"
#define SQL_DELETE		"DELETE FROM %s WHERE id = ?;"

struct sqlite_context
{
	sqlite3 *		sc_db;
};

struct sqlite_iter
{
	struct sqlite_context *	si_sc;
	sqlite3_stmt *		si_stmt;
};

static int sqlite_trace_callback(unsigned int, void *, void *, void *);
static int sqlite_unpack(sqlite3_stmt *, rpc_object_t *);
static int sqlite_open(struct persist_db *);
static void sqlite_close(struct persist_db *);
static int sqlite_create_collection(void *, const char *);
static int sqlite_get_collections(void *, GPtrArray *);
static int sqlite_get_object(void *, const char *, const char *, rpc_object_t *);
static int sqlite_save_object(void *, const char *, const char *, rpc_object_t);
static int sqlite_delete_object(void *, const char *, const char *);
static void *sqlite_query(void *, const char *, rpc_object_t);
static int sqlite_query_next(void *, rpc_object_t *);
static void sqlite_query_close(void *);

static int
sqlite_trace_callback(unsigned int code, void *ctx, void *p, void *x)
{
	sqlite3_stmt *stmt = p;
	char *sql;
	const uint8_t *id;

	if (code == SQLITE_TRACE_STMT) {
		sql = sqlite3_expanded_sql(stmt);
		fprintf(stderr, "(%p): executing %s\n", ctx, sql);
		sqlite3_free(sql);
		return (0);
	}

	if (code == SQLITE_TRACE_ROW) {
		id = sqlite3_column_text(stmt, 1);
		fprintf(stderr, "(%p): table %s: returning row %s\n", ctx,
		    sqlite3_column_table_name(stmt, 1), (const char *)id);
		return (0);
	}

	g_assert_not_reached();
}

static int
sqlite_unpack(sqlite3_stmt *stmt, rpc_object_t *result)
{
	const uint8_t *id;
	const void *blob;
	size_t len;
	rpc_object_t obj;

	id = sqlite3_column_text(stmt, 0);
	blob = sqlite3_column_text(stmt, 1);
	len = (size_t)sqlite3_column_bytes(stmt, 1);

	if (blob == NULL) {
		return (-1);
	}

	obj = rpc_serializer_load("json", blob, len);
	if (obj == NULL) {
		obj = rpc_get_last_error();
		persist_set_last_error(rpc_error_get_code(obj),
		    rpc_error_get_message(obj));

	}

	rpc_dictionary_set_string(obj, "id", (const char *)id);

	if (result != NULL)
		*result = obj;

	return (0);
}

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

	if (g_strcmp0(g_getenv("LIBPERSIST_LOGGING"), "stderr") == 0) {
		sqlite3_trace_v2(ctx->sc_db,
		    SQLITE_TRACE_STMT | SQLITE_TRACE_ROW,
		    sqlite_trace_callback, ctx);
	}

	db->pdb_arg = ctx;
	return (0);
}

static void
sqlite_close(struct persist_db *db)
{
	struct sqlite_context *ctx;

	ctx = db->pdb_arg;
	sqlite3_close(ctx->sc_db);
	g_free(ctx);
}

static int
sqlite_create_collection(void *arg, const char *name)
{
	struct sqlite_context *sqlite = arg;
	char *errmsg;
	g_autofree char *sql = g_strdup_printf(SQL_CREATE_TABLE, name);

	if (sqlite3_exec(sqlite->sc_db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
		persist_set_last_error(ENXIO, "%s", errmsg);
		return (-1);
	}

	return (0);
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

		default:
			persist_set_last_error(EFAULT, "%s",
			    sqlite3_errmsg(sqlite->sc_db));
			return (-1);
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

retry:
	switch (sqlite3_step(stmt)) {
	case SQLITE_ROW:
		ret = sqlite_unpack(stmt, obj);
		break;

	case SQLITE_DONE:
		persist_set_last_error(ENOENT, "Not found");
		ret = -1;
		break;

	case SQLITE_LOCKED:
	case SQLITE_BUSY:
		g_usleep(10 * 1000); /* 10ms sleep */
		goto retry;

	default:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}


	sqlite3_finalize(stmt);
	g_free(sql);
	if (obj != NULL)
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
	int err;

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

	if (sqlite3_bind_text64(stmt, 2, buf, (uint64_t)len, SQLITE_STATIC,
	    SQLITE_UTF8) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

retry:
	err = sqlite3_step(stmt);
	switch (err) {
	case SQLITE_DONE:
		break;

	case SQLITE_LOCKED:
	case SQLITE_BUSY:
		g_usleep(10 * 1000); /* 10ms sleep */
		goto retry;

	default:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		return (-1);
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

	default:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	sqlite3_finalize(stmt);
	g_free(sql);
	return (ret);
}

static void *
sqlite_query(void *arg, const char *collection, rpc_object_t query)
{
	struct sqlite_context *sqlite = arg;
	struct sqlite_iter *iter;
	g_autofree char *sql;
	sqlite3_stmt *stmt;

	sql = g_strdup_printf(SQL_QUERY, collection);
	if (sqlite3_prepare_v2(sqlite->sc_db, sql, -1, &stmt, NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (NULL);
	}

	iter = g_malloc0(sizeof(*iter));
	iter->si_sc = sqlite;
	iter->si_stmt = stmt;
	return (iter);
}

static int
sqlite_query_next(void *q_arg, rpc_object_t *result)
{
	struct sqlite_iter *iter = q_arg;
	int ret;

retry:
	ret = sqlite3_step(iter->si_stmt);
	switch (ret) {
	case SQLITE_DONE:
		*result = NULL;
		return (0);

	case SQLITE_ROW:
		return (sqlite_unpack(iter->si_stmt, result));


	case SQLITE_LOCKED:
	case SQLITE_BUSY:
		g_usleep(10 * 1000); /* 10ms sleep */
		goto retry;

	default:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(iter->si_sc->sc_db));
		return (-1);
	}
}

static void
sqlite_query_close(void *q_arg)
{
	struct sqlite_iter *iter = q_arg;

	sqlite3_finalize(iter->si_stmt);
	g_free(iter);

}

static const struct persist_driver sqlite_driver = {
	.pd_name = "sqlite",
	.pd_open = sqlite_open,
	.pd_close = sqlite_close,
	.pd_create_collection = sqlite_create_collection,
	.pd_get_collections = sqlite_get_collections,
	.pd_get_object = sqlite_get_object,
	.pd_save_object = sqlite_save_object,
	.pd_delete_object = sqlite_delete_object,
	.pd_query = sqlite_query,
	.pd_query_next = sqlite_query_next,
	.pd_query_close = sqlite_query_close,
};

DECLARE_DRIVER(sqlite_driver);
