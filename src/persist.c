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
#include <rpc/object.h>
#include <persist.h>
#include "internal.h"

static int
persist_create_collection(persist_db_t db, const char *name)
{
	rpc_auto_object_t col;

	col = rpc_object_pack("{v,[],{}}",
	    "created_at", rpc_date_create_from_current(),
	    "migrations",
	    "metadata");

	if (db->pdb_driver->pd_create_collection(db->pdb_arg, name) != 0)
		return (-1);

	if (db->pdb_driver->pd_save_object(db->pdb_arg, COLLECTIONS,
	    name, col) != 0)
		return (-1);

	return (0);
}

persist_db_t
persist_open(const char *path, const char *driver, rpc_object_t params)
{
	struct persist_db *db;

	db = g_malloc0(sizeof(*db));
	db->pdb_path = path;
	db->pdb_driver = persist_find_driver(driver);

	if (db->pdb_driver->pd_open(db) != 0) {
		g_free(db);
		return (NULL);
	}

	if (db->pdb_driver->pd_create_collection(db->pdb_arg,
	    COLLECTIONS) != 0) {

	}

	return (db);
}

void
persist_close(persist_db_t db)
{
}

persist_collection_t
persist_collection_get(persist_db_t db, const char *name, bool create)
{
	persist_collection_t result;
	rpc_object_t col;

	if (db->pdb_driver->pd_get_object(db->pdb_arg, COLLECTIONS,
	    name, &col) != 0) {
		if (errno == ENOENT && create) {
			if (persist_create_collection(db, name) == 0)
				goto ok;
		}

		return (NULL);
	}

ok:
	result = g_malloc0(sizeof(*result));
	result->pc_db = db;
	result->pc_name = name;
	return (result);
}

bool
persist_collection_exists(persist_db_t db, const char *name)
{

	return (db->pdb_driver->pd_get_object(db->pdb_arg, COLLECTIONS,
	    name, NULL) == 0);
}

int
persist_collection_remove(persist_db_t db, const char *name)
{

}

rpc_object_t
persist_collection_get_metadata(persist_db_t db, const char *name)
{
	rpc_object_t result;

	if (db->pdb_driver->pd_get_object(db->pdb_arg, COLLECTIONS,
	    name, &result) != 0) {
		persist_set_last_error(ENOENT, "Collection not found");
		return (NULL);
	}

	return (rpc_dictionary_get_value(result, "metadata"));
}

int
persist_collection_set_metadata(persist_db_t db, const char *name,
    rpc_object_t metadata)
{
	rpc_object_t result;

	if (db->pdb_driver->pd_get_object(db->pdb_arg, COLLECTIONS,
	    name, &result) != 0) {
		persist_set_last_error(ENOENT, "Collection not found");
		return (-1);
	}

	rpc_dictionary_set_value(result, "metadata", metadata);
	return (db->pdb_driver->pd_save_object(db->pdb_arg, COLLECTIONS,
	    name, result));
}

void
persist_collection_close(persist_collection_t collection)
{

	g_free(collection);
}

void
persist_collections_apply(persist_db_t db, persist_collection_iter_t fn)
{
	void * iter;
	rpc_object_t collection;

	iter = db->pdb_driver->pd_query(db->pdb_arg, COLLECTIONS, NULL);

	for (;;) {
		if (db->pdb_driver->pd_query_next(iter, &collection) != 0)
			return;

		if (collection == NULL)
			return;

		fn(rpc_dictionary_get_string(collection, "id"));
	}
}

rpc_object_t
persist_get(persist_collection_t col, const char *id)
{
	rpc_object_t result;

	if (col->pc_db->pdb_driver->pd_get_object(col->pc_db->pdb_arg,
	    col->pc_name, id, &result) != 0)
		return (NULL);

	return (result);
}

persist_iter_t
persist_query(persist_collection_t col, rpc_object_t query)
{
	struct persist_iter *iter;

	iter = g_malloc0(sizeof(*iter));
	iter->pi_col = col;
	iter->pi_arg = col->pc_db->pdb_driver->pd_query(
	    col->pc_db->pdb_arg, col->pc_name, query);

	if (iter->pi_arg == NULL) {
		g_free(iter);
		return (NULL);
	}

	return (iter);
}

int
persist_save(persist_collection_t col, const char *id, rpc_object_t obj)
{

	if (col->pc_db->pdb_driver->pd_save_object(col->pc_db->pdb_arg,
	    col->pc_name, id, obj) != 0)
		return (-1);

	return (0);
}

rpc_object_t
persist_iter_next(persist_iter_t iter)
{
	rpc_object_t result;

	if (iter->pi_col->pc_db->pdb_driver->pd_query_next(iter->pi_arg,
	    &result) != 0)
		return (NULL);

	return (result);
}

void
persist_iter_close(persist_iter_t iter)
{

	iter->pi_col->pc_db->pdb_driver->pd_query_close(iter->pi_arg);
	g_free(iter);
}

int
persist_delete(persist_collection_t col, const char *id)
{

}
