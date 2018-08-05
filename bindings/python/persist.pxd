#
# Copyright 2018 Wojciech Kloska <shangteus@gmail.com>
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted providing that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#

from libc.stdint cimport *


ctypedef bint (*persist_collection_iter_f)(void *arg, const char *name)


cdef extern from "rpc/object.h":
    ctypedef struct rpc_object:
        pass

    ctypedef rpc_object *rpc_object_t


cdef extern from "persist.h" nogil:
    cdef struct persist_db:
        pass

    cdef struct persist_collection:
        pass

    cdef struct persist_iter:
        pass

    cdef struct persist_query_params:
        bint single
        bint count
        bint descending
        const char *sort_field
        uint64_t offset
        uint64_t limit

    ctypedef persist_db *persist_db_t
    ctypedef persist_collection *persist_collection_t
    ctypedef persist_iter *persist_iter_t
    ctypedef persist_query_params *persist_query_params_t

    void *PERSIST_COLLECTION_ITER(persist_collection_iter_f fn, void *arg)

    persist_db_t persist_open(const char *path, const char *driver,
        rpc_object_t params)
    void persist_close(persist_db_t db)
    persist_collection_t persist_collection_get(persist_db_t db, const char *name, bint create)
    bint persist_collection_exists(persist_db_t db, const char *name)
    int persist_collection_remove(persist_db_t db, const char *name)
    rpc_object_t persist_collection_get_metadata(persist_db_t db, const char *name)
    int persist_collection_set_metadata(persist_db_t db, const char *name,
        rpc_object_t metadata)
    void persist_collections_apply(persist_db_t db, void *applier)
    rpc_object_t persist_get(persist_collection_t col, const char *id)
    persist_iter_t persist_query(persist_collection_t col, rpc_object_t rules, persist_query_params_t params)
    int persist_save(persist_collection_t col, rpc_object_t obj)
    int persist_delete(persist_collection_t col, const char *id)
    int persist_get_last_error(char **msgp)
    void persist_collection_close(persist_collection_t collection)
    void persist_iter_close(persist_iter_t iter)
    int persist_iter_next(persist_iter_t iter, rpc_object_t *result)


cdef class Database(object):
    cdef persist_db_t db

    @staticmethod
    cdef bint c_apply_callback(void *arg, const char *name)


cdef class Collection(object):
    cdef persist_collection_t collection

    @staticmethod
    cdef Collection wrap(persist_collection_t ptr)
    cdef persist_collection_t unwrap(self) nogil


cdef class CollectionIterator(object):
    cdef persist_iter_t iter
    cdef object cnt

    @staticmethod
    cdef CollectionIterator wrap(persist_iter_t iter)
