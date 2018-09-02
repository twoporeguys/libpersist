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

#ifndef LIBPERSIST_PERSIST_H
#define LIBPERSIST_PERSIST_H

#include <rpc/object.h>
#include <rpc/rpc.h>

struct persist_db;
struct persist_collection;
struct persist_iter;
struct persist_query_params;

/**
 * An open database handle.
 */
typedef struct persist_db *persist_db_t;

/**
 * A collection handle.
 */
typedef struct persist_collection *persist_collection_t;

/**
 *
 */
typedef struct persist_iter *persist_iter_t;

/**
 *
 */
typedef struct persist_query_params *persist_query_params_t;

/**
 *
 */
typedef bool (^persist_collection_iter_t)(const char *_Nonnull name);

/**
 * Converts function pointer to a persist_collection_iter_t block type.
 */
#define	PERSIST_COLLECTION_ITER(_fn, _arg)		\
	^(const char *_name) {				\
                return ((bool)_fn(_arg, _name));	\
        }

struct persist_query_params
{
	bool				single;
	bool				count;
	bool				descending;
	const char *_Nullable		sort_field;
	uint64_t			offset;
	uint64_t			limit;
	_Nullable rpc_query_cb_t	callback;
};

/**
 * Opens a database in a file @p path.
 *
 * If the database file doesn't exist, it will get created.
 *
 * @param path Database file path
 * @param params
 * @return Open database handle
 */
_Nullable persist_db_t persist_open(const char *_Nonnull path,
    const char *_Nonnull driver, _Nullable rpc_object_t params);

/**
 * Closes the database handle.
 *
 * @param db Database handle
 */
void persist_close(_Nonnull persist_db_t db);

/**
 * Returns a collection handle. If no such collection exists, it will
 * be created.
 *
 * @param db Database handle
 * @param name Collection name
 * @return
 */
_Nullable persist_collection_t persist_collection_get(
    _Nonnull persist_db_t db, const char *_Nonnull name, bool create);

/**
 *
 * @param db Database handle
 * @param name Collection name
 * @return
 */
bool persist_collection_exists(_Nonnull persist_db_t db,
    const char *_Nonnull name);

/**
 *
 * @param db Database handle
 * @param name Collection name
 * @return
 */
int persist_collection_remove(_Nonnull persist_db_t db,
    const char *_Nonnull name);

/**
 * Retrieves collection metadata object for collection @p name.
 *
 * @param db Database handle
 * @param name Collection name
 * @return Metadata object.
 */
_Nullable rpc_object_t persist_collection_get_metadata(
    _Nonnull persist_db_t db, const char *_Nonnull name);

/**
 *
 * @param db Database handle
 * @param name Collection name
 * @param metadata
 * @return
 */
int persist_collection_set_metadata(_Nonnull persist_db_t db,
    const char *_Nonnull name, _Nullable rpc_object_t metadata);

/**
 *
 * @param collection
 */
void persist_collection_close(_Nonnull persist_collection_t collection);

/**
 * @param db Database handle
 */
void persist_collections_apply(_Nonnull persist_db_t db,
    _Nonnull persist_collection_iter_t fn);

/**
 *
 * @param col Collection handle
 * @param id Primary key
 * @return
 */
_Nullable rpc_object_t persist_get(_Nonnull persist_collection_t col,
    const char *_Nonnull id);

/**
 *
 * @param col Collection handle
 * @param query
 * @return
 */
_Nullable persist_iter_t persist_query(_Nonnull persist_collection_t col,
    _Nullable rpc_object_t filter, _Nullable persist_query_params_t params);

/**
 *
 * @param col Collection handle
 * @param obj Object to save
 * @return
 */
int persist_save(_Nonnull persist_collection_t col, _Nonnull rpc_object_t obj);

/**
 *
 * @param col
 * @param objetcs
 * @return
 */
int persist_save_many(_Nonnull persist_collection_t col,
    _Nonnull rpc_object_t objects);


/**
 *
 * @param col Collection handle
 * @param id Primary key
 * @return
 */
int persist_delete(_Nonnull persist_collection_t col, const char *_Nonnull id);

/**
 *
 * @param iter
 * @return
 */
int persist_iter_next(_Nonnull persist_iter_t iter,
    _Nullable rpc_object_t *_Nonnull result);

/**
 *
 * @param iter
 */
void persist_iter_close(_Nonnull persist_iter_t iter);

/**
 *
 * @param msgp
 * @return
 */
int persist_get_last_error(const char *_Nullable *_Nonnull msgp);

#endif /* LIBPERSIST_PERSIST_H */
