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
#include <inttypes.h>
#include <glib.h>
#include <sqlite3.h>
#include <rpc/object.h>
#include <rpc/serializer.h>
#include "../linker_set.h"
#include "../internal.h"

#define SQLITE_YIELD_DELAY	(1 * 1000)
#define SQL_INIT		"PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF;"
#define SQL_CREATE_TABLE	"CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY, value TEXT);"
#define SQL_DROP_TABLE		"DROP TABLE %s;"
#define SQL_LIST_TABLES		"SELECT * FROM sqlite_master WHERE TYPE='table';"
#define SQL_GET			"SELECT * FROM %s WHERE id = ?;"
#define SQL_INSERT		"INSERT OR REPLACE INTO %s (id, value) VALUES (?, ?);"
#define SQL_DELETE		"DELETE FROM %s WHERE id = ?;"
#define SQL_ADD_INDEX(_x)	"CREATE INDEX IF NOT EXISTS %s_%s ON %s(" SQL_EXTRACT(_x) ");"
#define SQL_DROP_INDEX		"DROP INDEX %s_%s"
#define SQL_EXTRACT(_x)		"json_quote(json_extract(value, '$." _x "'))"
#define SQL_JSON(_x)		"json('" _x "')"

struct sqlite_context
{
	sqlite3 *		sc_db;
	bool			sc_trace;
	GHashTable *		sc_stmt_cache;
	GMutex			sc_mtx;
};

struct sqlite_iter
{
	struct sqlite_context *	si_sc;
	sqlite3_stmt *		si_stmt;
};

struct sqlite_operator
{
	const char *		so_librpc;
	const char *		so_sqlite;
};

struct sqlite_prepared_stmts
{
	sqlite3_stmt *		sc_prepared_get;
	sqlite3_stmt *		sc_prepared_insert;
	sqlite3_stmt *		sc_prepared_delete;
};

static bool sqlite_eval_logic_and(GString *, rpc_object_t);
static bool sqlite_eval_logic_or(GString *, rpc_object_t);
static bool sqlite_eval_logic_nor(GString *, rpc_object_t);
static bool sqlite_eval_logic_operator(GString *, rpc_object_t);
static bool sqlite_eval_field_operator(GString *, rpc_object_t);
static bool sqlite_eval_rule(GString *, rpc_object_t);
static int sqlite_trace_callback(unsigned int, void *, void *, void *);
static int sqlite_exec(struct sqlite_context *, const char *);
static int sqlite_unpack(sqlite3_stmt *, char **, rpc_object_t *);
static struct sqlite_prepared_stmts *sqlite_get_prepared_stmts(
    struct sqlite_context *, const char *);
static void sqlite_free_prepared_stmts(struct sqlite_prepared_stmts *);
static int sqlite_open(struct persist_db *);
static void sqlite_close(struct persist_db *);
static int sqlite_create_collection(void *, const char *);
static int sqlite_destroy_collection(void *, const char *);
static int sqlite_get_collections(void *, GPtrArray *);
static int sqlite_add_index(void *, const char *, const char *, const char *);
static int sqlite_drop_index(void *, const char *, const char *);
static int sqlite_get_object(void *, const char *, const char *, rpc_object_t *);
static int sqlite_save_object(void *, const char *, const char *, rpc_object_t);
static int sqlite_save_objects(void *, const char *, rpc_object_t);
static int sqlite_delete_object(void *, const char *, const char *);
static int sqlite_start_tx(void *);
static int sqlite_commit_tx(void *);
static int sqlite_rollback_tx(void *);
static bool sqlite_in_tx(void *);
static ssize_t sqlite_count(void *, const char *, rpc_object_t);
static void *sqlite_query(void *, const char *, rpc_object_t, persist_query_params_t);
static int sqlite_query_next(void *, char **id, rpc_object_t *);
static void sqlite_query_close(void *);

static GMutex sqlite_mtx;
static const struct sqlite_operator sqlite_operator_table[] = {
	{ "=", "=" },
	{ "!=", "!=" },
	{ ">", ">" },
	{ ">=", ">=" },
	{ "<" , "<" },
	{ "<=", "<=" },
	{ "~", "REGEXP" },
	{ "match", "GLOB" },
	{ }
};

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
		id = sqlite3_column_text(stmt, 0);
		fprintf(stderr, "(%p): table %s: returning row %s\n", ctx,
		    sqlite3_column_table_name(stmt, 0), (const char *)id);
		return (0);
	}

	g_assert_not_reached();
}

static int
sqlite_exec(struct sqlite_context *ctx, const char *sql)
{
	char *errmsg;
	int ret;

	retry:
	ret = sqlite3_exec(ctx->sc_db, sql, NULL, NULL, &errmsg);

	switch (ret) {
		case SQLITE_OK:
		case SQLITE_DONE:
			break;

		case SQLITE_BUSY:
		case SQLITE_LOCKED:
			g_usleep(SQLITE_YIELD_DELAY);
			goto retry;

		default:
			persist_set_last_error(ENXIO, "%s", errmsg);
			return (-1);
	}

	return (0);
}

static int
sqlite_unpack(sqlite3_stmt *stmt, char **idp, rpc_object_t *result)
{
	const uint8_t *id;
	const void *blob;
	size_t len;
	rpc_object_t obj;

	id = sqlite3_column_text(stmt, 0);
	blob = sqlite3_column_text(stmt, 1);
	len = (size_t)sqlite3_column_bytes(stmt, 1);

	if (blob == NULL) {
		persist_set_last_error(EINVAL, "Inconsistent database state");
		return (-1);
	}

	obj = rpc_serializer_load("json", blob, len);
	if (obj == NULL) {
		obj = rpc_get_last_error();
		persist_set_last_error(rpc_error_get_code(obj),
		    rpc_error_get_message(obj));

	}

	if (idp != NULL)
		*idp = g_strdup((const char *)id);

	if (result != NULL)
		*result = obj;

	return (0);
}

static struct sqlite_prepared_stmts *
sqlite_get_prepared_stmts(struct sqlite_context *sqlite, const char *col)
{
	struct sqlite_prepared_stmts *stmts;
	g_autofree char *get_sql = NULL;
	g_autofree char *insert_sql = NULL;
	g_autofree char *delete_sql = NULL;

	g_mutex_lock(&sqlite->sc_mtx);

retry:
	stmts = g_hash_table_lookup(sqlite->sc_stmt_cache, col);
	if (stmts != NULL) {
		g_mutex_unlock(&sqlite->sc_mtx);
		return (stmts);
	}

	stmts = g_malloc0(sizeof(*stmts));
	get_sql = g_strdup_printf(SQL_GET, col);
	insert_sql = g_strdup_printf(SQL_INSERT, col);
	delete_sql = g_strdup_printf(SQL_DELETE, col);

	if (sqlite3_prepare_v2(sqlite->sc_db, get_sql, -1,
	    &stmts->sc_prepared_get, NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (NULL);
	}

	if (sqlite3_prepare_v2(sqlite->sc_db, insert_sql, -1,
	    &stmts->sc_prepared_insert, NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (NULL);
	}

	if (sqlite3_prepare_v2(sqlite->sc_db, delete_sql, -1,
	    &stmts->sc_prepared_delete, NULL) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (NULL);
	}

	g_hash_table_insert(sqlite->sc_stmt_cache, g_strdup(col), stmts);
	goto retry;
}

static void
sqlite_free_prepared_stmts(struct sqlite_prepared_stmts *stmts)
{

	sqlite3_finalize(stmts->sc_prepared_get);
	sqlite3_finalize(stmts->sc_prepared_insert);
	sqlite3_finalize(stmts->sc_prepared_delete);
	g_free(stmts);
}

static int
sqlite_open(struct persist_db *db)
{
	struct sqlite_context *ctx;
	int err;

	err = sqlite3_enable_shared_cache(1);
	if (err != SQLITE_OK)
		abort();

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

		ctx->sc_trace = true;
	}

	if (sqlite_exec(ctx, SQL_INIT) != 0) {
		sqlite3_close(ctx->sc_db);
		g_free(ctx);
		return (-1);
	}

	g_mutex_init(&ctx->sc_mtx);
	ctx->sc_stmt_cache = g_hash_table_new_full(g_str_hash, g_str_equal,
	    (GDestroyNotify)g_free, (GDestroyNotify)sqlite_free_prepared_stmts);

	db->pdb_arg = ctx;
	return (0);
}

static void
sqlite_close(struct persist_db *db)
{
	struct sqlite_context *ctx;

	ctx = db->pdb_arg;
	sqlite3_close(ctx->sc_db);
	g_hash_table_destroy(ctx->sc_stmt_cache);
	g_mutex_clear(&ctx->sc_mtx);
	g_free(ctx);
}

static int
sqlite_create_collection(void *arg, const char *name)
{
	struct sqlite_context *sqlite = arg;
	g_autofree char *sql = g_strdup_printf(SQL_CREATE_TABLE, name);

	return (sqlite_exec(sqlite, sql));
}

static int
sqlite_destroy_collection(void *arg, const char *name)
{
	struct sqlite_context *sqlite = arg;
	g_autofree char *sql = g_strdup_printf(SQL_DROP_TABLE, name);

	return (sqlite_exec(sqlite, sql));
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
		retry:
		switch (sqlite3_step(stmt)) {
		case SQLITE_ROW:
			name = (char *)sqlite3_column_text(stmt, 2);
			g_ptr_array_add(result, name);
			continue;

		case SQLITE_LOCKED:
		case SQLITE_BUSY:
			g_usleep(SQLITE_YIELD_DELAY);
			goto retry;

		case SQLITE_DONE:
			goto endloop;

		default:
			persist_set_last_error(EFAULT, "%s",
			    sqlite3_errmsg(sqlite->sc_db));
			sqlite3_finalize(stmt);
			return (-1);
		}

endloop:
		break;
	}

	sqlite3_finalize(stmt);
	return (0);
}

static int
sqlite_add_index(void *arg, const char *collection, const char *name,
    const char *path)
{
	struct sqlite_context *sqlite = arg;
	g_autofree char *sql = g_strdup_printf(SQL_ADD_INDEX("%s"),
	    collection, name, collection, path);

	return (sqlite_exec(sqlite, sql));
}

static int
sqlite_drop_index(void *arg, const char *collection, const char *name)
{
	struct sqlite_context *sqlite = arg;
	g_autofree char *sql = g_strdup_printf(SQL_DROP_INDEX, collection,
	    name);

	return (sqlite_exec(sqlite, sql));
}

static int
sqlite_get_object(void *arg, const char *collection, const char *id,
    rpc_object_t *obj)
{
	struct sqlite_context *sqlite = arg;
	struct sqlite_prepared_stmts *stmts;
	sqlite3_stmt *stmt;
	int ret = 0;

	stmts = sqlite_get_prepared_stmts(sqlite, collection);
	stmt = stmts->sc_prepared_get;

	if (sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

retry:
	switch (sqlite3_step(stmt)) {
	case SQLITE_ROW:
		ret = sqlite_unpack(stmt, NULL, obj);
		break;

	case SQLITE_DONE:
		persist_set_last_error(ENOENT, "Not found");
		ret = -1;
		break;

	case SQLITE_LOCKED:
	case SQLITE_BUSY:
		g_usleep(SQLITE_YIELD_DELAY); /* 10ms sleep */
		goto retry;

	default:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		sqlite3_clear_bindings(stmt);
		sqlite3_reset(stmt);
		return (-1);
	}

	sqlite3_clear_bindings(stmt);
	sqlite3_reset(stmt);
	return (ret);
}

static int
sqlite_save_object(void *arg, const char *collection, const char *id,
    rpc_object_t obj)
{
	struct sqlite_context *sqlite = arg;
	struct sqlite_prepared_stmts *stmts;
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

	stmts = sqlite_get_prepared_stmts(sqlite, collection);
	stmt = stmts->sc_prepared_insert;

	if (sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		ret = -1;
		goto out;
	}

	if (sqlite3_bind_text64(stmt, 2, buf, (uint64_t)len, SQLITE_STATIC,
	    SQLITE_UTF8) != SQLITE_OK) {
		persist_set_last_error(errno, "%s", sqlite3_errmsg(sqlite->sc_db));
		ret = -1;
		goto out;
	}

retry:
	g_mutex_lock(&sqlite_mtx);
	err = sqlite3_step(stmt);
	g_mutex_unlock(&sqlite_mtx);

	switch (err) {
	case SQLITE_DONE:
		break;

	case SQLITE_LOCKED:
	case SQLITE_BUSY:
		g_usleep(SQLITE_YIELD_DELAY);
		goto retry;

	default:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		ret = -1;
		goto out;
	}

out:
	sqlite3_clear_bindings(stmt);
	sqlite3_reset(stmt);
	g_free(buf);
	return (ret);
}

static int
sqlite_save_objects(void *arg, const char *collection, rpc_object_t objects)
{
	bool stop;

	stop = rpc_array_apply(objects, ^bool(size_t idx, rpc_object_t item) {
		rpc_auto_object_t id = NULL;

		id = rpc_dictionary_detach_key(item, "id");
		if (id == NULL) {
			persist_set_last_error(EINVAL, "Object has no 'id' key");
			return (false);
		}

		if (sqlite_save_object(arg, collection,
		    rpc_string_get_string_ptr(id), item) != 0)
			return (false);

		return (true);
	});

	return (stop ? -1 : 0);
}

static int
sqlite_delete_object(void *arg, const char *collection, const char *id)
{
	struct sqlite_context *sqlite = arg;
	struct sqlite_prepared_stmts *stmts;
	sqlite3_stmt *stmt;
	int ret = 0;

	stmts = sqlite_get_prepared_stmts(sqlite, collection);
	stmt = stmts->sc_prepared_delete;

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
		sqlite3_clear_bindings(stmt);
		sqlite3_reset(stmt);
		return (-1);
	}

	sqlite3_clear_bindings(stmt);
	sqlite3_reset(stmt);
	return (ret);
}

static int
sqlite_start_tx(void *arg)
{
	struct sqlite_context *sqlite = arg;
	int ret;

	g_mutex_lock(&sqlite_mtx);
	ret = sqlite_exec(sqlite, "BEGIN TRANSACTION;");
	g_mutex_unlock(&sqlite_mtx);

	return (ret);
}

static int
sqlite_commit_tx(void *arg)
{
	struct sqlite_context *sqlite = arg;
	int ret;

	g_mutex_lock(&sqlite_mtx);
	ret = sqlite_exec(sqlite, "COMMIT TRANSACTION;");
	g_mutex_unlock(&sqlite_mtx);

	return (ret);
}

static int
sqlite_rollback_tx(void *arg)
{
	struct sqlite_context *sqlite = arg;

	return (sqlite_exec(sqlite, "ROLLBACK TRANSACTION;"));
}

static bool
sqlite_in_tx(void *arg)
{
	struct sqlite_context *sqlite = arg;

	return (sqlite3_get_autocommit(sqlite->sc_db) ? false : true);
}

static bool
sqlite_eval_logic_and(GString *sql, rpc_object_t lst)
{
	size_t len;
	bool stop;

	if (rpc_get_type(lst) != RPC_TYPE_ARRAY) {
		persist_set_last_error(EINVAL, "'and' predicate is not an array");
		return (false);
	}

	if (rpc_array_get_count(lst) == 0) {
		g_string_append_printf(sql, "(1==1)");
		return (true);
	}

	len = rpc_array_get_count(lst);
	g_string_append(sql, "(");

	stop = rpc_array_apply(lst, ^(size_t idx, rpc_object_t v) {
		if (!sqlite_eval_rule(sql, v))
			return ((bool)false);

		if (idx != len - 1)
			g_string_append(sql, " AND ");

		return ((bool)true);
	});

	g_string_append(sql, ")");
	return (!stop);
}

static bool
sqlite_eval_logic_or(GString *sql, rpc_object_t lst)
{
	size_t len;
	bool stop;

	if (rpc_get_type(lst) != RPC_TYPE_ARRAY) {
		persist_set_last_error(EINVAL, "'or' predicate is not an array");
		return (false);
	}

	len = rpc_array_get_count(lst);
	g_string_append(sql, "(");

	stop = rpc_array_apply(lst, ^(size_t idx, rpc_object_t v) {
		if (!sqlite_eval_rule(sql, v))
			return ((bool)false);

		if (idx == len - 1)
			g_string_append(sql, " OR ");

		return ((bool)true);
	});

	g_string_append(sql, ")");
	return (!stop);
}

static bool
sqlite_eval_logic_nor(GString *sql, rpc_object_t lst)
{
	size_t len;
	bool stop;

	if (rpc_get_type(lst) != RPC_TYPE_ARRAY) {
		persist_set_last_error(EINVAL, "'nor' predicate is not an array");
		return (false);
	}

	len = rpc_array_get_count(lst);
	g_string_append(sql, "(");

	stop = rpc_array_apply(lst, ^(size_t idx, rpc_object_t v) {
		if (!sqlite_eval_rule(sql, v))
			return ((bool)false);

		if (idx == len - 1)
			g_string_append(sql, " AND "); /* XXX should be NOR */

		return ((bool)true);
	});

	g_string_append(sql, ")");
	return (!stop);
}

static bool
sqlite_eval_logic_operator(GString *sql, rpc_object_t rule)
{
	const char *op;
	rpc_object_t value;

	if (rpc_object_unpack(rule, "[s,v]", &op, &value) < 2) {
		persist_set_last_error(EINVAL, "Cannot unpack logic tuple");
		return (false);
	}

	if (g_strcmp0(op, "and") == 0)
		return (sqlite_eval_logic_and(sql, value));

	if (g_strcmp0(op, "or") == 0)
		return (sqlite_eval_logic_or(sql, value));

	if (g_strcmp0(op, "nor") == 0)
		return (sqlite_eval_logic_nor(sql, value));

	return (false);
}

static bool
sqlite_eval_field_operator(GString *sql, rpc_object_t rule)
{
	const struct sqlite_operator *op;
	const char *sql_op = NULL;
	const char *rule_op;
	const char *field;
	char *value_str;
	size_t value_len;
	rpc_object_t value;

	if (rpc_object_unpack(rule, "[s,s,v]", &field, &rule_op, &value) < 3)
		return (false);

	if (rpc_serializer_dump("json", value, (void **)&value_str,
	    &value_len) != 0) {
		persist_set_last_error(EFAULT, "Cannot serialize value");
		return (false);
	}

	for (op = &sqlite_operator_table[0]; op->so_librpc != NULL; op++) {
		if (g_strcmp0(rule_op, op->so_librpc) == 0) {
			sql_op = op->so_sqlite;
			break;
		}
	}

	if (sql_op == NULL) {
		persist_set_last_error(EINVAL, "Invalid operator: %s", rule_op);
		return (false);
	}

	g_string_append_printf(sql, SQL_EXTRACT("%s") " %s " SQL_JSON("%*s"),
	    field, sql_op, (int)value_len, value_str);
	return (true);
}

static bool
sqlite_eval_rule(GString *sql, rpc_object_t rule)
{
	if (rpc_get_type(rule) != RPC_TYPE_ARRAY) {
		persist_set_last_error(EINVAL, "Rule is not an array");
		return (false);
	}

	switch (rpc_array_get_count(rule)) {
	case 2:
		return (sqlite_eval_logic_operator(sql, rule));

	case 3:
		return (sqlite_eval_field_operator(sql, rule));

	default:
		persist_set_last_error(EINVAL,
		    "Invalid number of items in a rule tuple");

		return (false);
	}
}

static ssize_t
sqlite_count(void *arg, const char *collection, rpc_object_t rules)
{
	struct sqlite_context *sqlite = arg;
	GString *sql;
	sqlite3_stmt *stmt;
	ssize_t result;
	int ret;

	sql = g_string_new("SELECT ");
	g_string_append_printf(sql, "count(id) FROM %s ", collection);

	if (rules != NULL) {
		g_string_append_printf(sql, "WHERE ");
		if (!sqlite_eval_logic_and(sql, rules)) {
			g_string_free(sql, true);
			return (-1);
		}
	}

	g_string_append(sql, ";");

	if (sqlite->sc_trace)
		fprintf(stderr, "(%p): query string: %s\n", sqlite, sql->str);

	if (sqlite3_prepare_v2(sqlite->sc_db, sql->str, -1, &stmt, NULL) != SQLITE_OK) {
		persist_set_last_error(EFAULT, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (-1);
	}

	g_string_free(sql, true);

retry:
	ret = sqlite3_step(stmt);
	switch (ret) {
	case SQLITE_DONE:
		persist_set_last_error(ENOENT, "sqlite returned no rows");
		result = -1;
		break;

	case SQLITE_ROW:
		result = sqlite3_column_int(stmt, 0);
		break;

	case SQLITE_LOCKED:
	case SQLITE_BUSY:
		g_usleep(SQLITE_YIELD_DELAY);
		goto retry;

	default:
		persist_set_last_error(EFAULT, "%s",
		    sqlite3_errmsg(sqlite->sc_db));
		result = -1;
		break;
	}

	sqlite3_finalize(stmt);
	return (result);
}

static void *
sqlite_query(void *arg, const char *collection, rpc_object_t rules,
    persist_query_params_t params)
{
	struct sqlite_context *sqlite = arg;
	struct sqlite_iter *iter;
	GString *sql;
	sqlite3_stmt *stmt;

	sql = g_string_new("SELECT ");
	g_string_append_printf(sql, "id, value FROM %s ", collection);

	if (rules != NULL) {
		g_string_append_printf(sql, "WHERE ");
		if (!sqlite_eval_logic_and(sql, rules)) {
			g_string_free(sql, true);
			return (NULL);
		}
	}

	if (params != NULL) {
		if (params->sort_field != NULL) {
			g_string_append_printf(sql,
			    "ORDER BY " SQL_EXTRACT("%s") " %s ",
			    params->sort_field,
			    params->descending ? "DESC" : "ASC");
		}

		if (params->limit) {
			g_string_append_printf(sql, "LIMIT %" PRIu64 " ",
			    params->limit);
		}

		if (params->offset) {
			g_string_append_printf(sql, "OFFSET %" PRIu64 " ",
			    params->offset);
		}

		if (params->single)
			g_string_append(sql, "LIMIT 1 ");
	}

	g_string_append(sql, ";");

	if (sqlite->sc_trace)
		fprintf(stderr, "(%p): query string: %s\n", sqlite, sql->str);

	if (sqlite3_prepare_v2(sqlite->sc_db, sql->str, -1, &stmt, NULL) != SQLITE_OK) {
		persist_set_last_error(EFAULT, "%s", sqlite3_errmsg(sqlite->sc_db));
		return (NULL);
	}

	g_string_free(sql, true);

	iter = g_malloc0(sizeof(*iter));
	iter->si_sc = sqlite;
	iter->si_stmt = stmt;
	return (iter);
}

static int
sqlite_query_next(void *q_arg, char **id, rpc_object_t *result)
{
	struct sqlite_iter *iter = q_arg;
	int ret;

retry:
	ret = sqlite3_step(iter->si_stmt);
	switch (ret) {
	case SQLITE_DONE:
		if (id != NULL)
			*id = NULL;

		if (result != NULL)
			*result = NULL;

		return (0);

	case SQLITE_ROW:
		return (sqlite_unpack(iter->si_stmt, id, result));

	case SQLITE_LOCKED:
	case SQLITE_BUSY:
		g_usleep(SQLITE_YIELD_DELAY);
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
	.pd_destroy_collection = sqlite_destroy_collection,
	.pd_add_index = sqlite_add_index,
	.pd_drop_index = sqlite_drop_index,
	.pd_get_object = sqlite_get_object,
	.pd_save_object = sqlite_save_object,
	.pd_save_objects = sqlite_save_objects,
	.pd_delete_object = sqlite_delete_object,
	.pd_start_tx = sqlite_start_tx,
	.pd_commit_tx = sqlite_commit_tx,
	.pd_rollback_tx = sqlite_rollback_tx,
	.pd_in_tx = sqlite_in_tx,
	.pd_count = sqlite_count,
	.pd_query = sqlite_query,
	.pd_query_next = sqlite_query_next,
	.pd_query_close = sqlite_query_close,
};

DECLARE_DRIVER(sqlite_driver);
