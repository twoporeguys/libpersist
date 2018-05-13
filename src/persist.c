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

#include <rpc/object.h>
#include <persist.h>

persist_db_t
persist_open(const char *path, const char *driver, rpc_object_t params)
{

}

void
persist_close(persist_db_t db)
{

}

persist_collection_t
persist_collection_get(persist_db_t db, const char *name)
{

}

int
persist_collection_remove(persist_db_t db, const char *name)
{

}

rpc_object_t
persist_collection_get_metadata(persist_db_t db, const char *name)
{

}

int
persist_collection_set_metadata(persist_db_t db, const char *name,
    rpc_object_t metadata)
{

}

void
persist_collections_apply(persist_db_t db)
{

}

rpc_object_t
persist_get(persist_collection_t col, const char *id)
{

}

/**
 *
 * @param col
 * @param query
 * @return
 */
bool persist_query(persist_collection_t col, rpc_object_t query);

/**
 *
 * @param col
 * @param id
 * @param obj
 * @return
 */
int persist_save(persist_collection_t col, const char *id, rpc_object_t obj);

/**
 *
 * @param col
 * @param id
 * @return
 */
int persist_delete(persist_collection_t col, const char *id);
