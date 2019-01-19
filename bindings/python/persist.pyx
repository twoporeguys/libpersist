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

import logging
from librpc import ObjectType
from libc.string cimport memset
cimport persist

logger = logging.getLogger(__name__)

class PersistException(RuntimeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


cdef class Database(object):
    def __init__(self, path, driver, params=None):
        cdef Object rpc_params = Object(params)

        if not isinstance(path, str):
            raise TypeError('Path needs to be a string')

        if not isinstance(driver, str):
            raise TypeError('Driver needs to be a string')

        self.path = path
        self.driver = driver
        self.params = rpc_params
        self.collections = []

    def __dealloc__(self):
        if self.db != <persist_db_t>NULL:
            logger.warning('Leaking memory')

    def open(self):
        self.db = persist_open(
            self.path.encode('utf-8'),
            self.driver.encode('utf-8'),
            self.params.unwrap()
        )
        if self.db == <persist_db_t>NULL:
            check_last_error()

    def close(self):
        if self.db != <persist_db_t>NULL:
            # Close our collections
            for col in self.collections:
                col.close()
            persist_close(self.db)

        self.db = <persist_db_t>NULL

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    property is_open:
        def __get__(self):
            return self.db != <persist_db_t>NULL

    def collection_exists(self, name):
        if self.db == <persist_db_t>NULL:
            raise ValueError('Database is closed')

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        return persist_collection_exists(self.db, name.encode('utf-8'))

    def get_collection(self, name, create=False):
        cdef persist_collection_t collection

        if self.db == <persist_db_t>NULL:
            raise ValueError('Database is closed')

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not create and not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        collection = persist_collection_get(self.db, name.encode('utf-8'), create)

        self.collections.append(collection)

        return Collection.wrap(self, collection)

    def create_collection(self, name):
        if self.db == <persist_db_t>NULL:
            raise ValueError('Database is closed')

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if persist_collection_exists(self.db, name.encode('utf-8')):
            raise ValueError('Collection {} already exists'.format(name))

        return self.get_collection(name, True)

    def remove_collection(self, name):
        if self.db == <persist_db_t>NULL:
            raise ValueError('Database is closed')

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        persist_collection_remove(self.db, name.encode('utf-8'))

    def get_collection_metadata(self, name):
        cdef rpc_object_t metadata

        if self.db == <persist_db_t>NULL:
            raise ValueError('Database is closed')

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        metadata = persist_collection_get_metadata(self.db, name.encode('utf-8'))

        return Object.wrap(metadata).unpack()

    def set_collection_metadata(self, name, metadata):
        cdef Object rpc_metadata = Object(metadata)

        if self.db == <persist_db_t>NULL:
            raise ValueError('Database is closed')

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        persist_collection_set_metadata(self.db, name.encode('utf-8'), rpc_metadata.unwrap())

    def list_collections(self):
        if self.db == <persist_db_t>NULL:
            raise ValueError('Database is closed')

        names = []
        def collect(name):
            names.append(name.decode('utf-8'))

        persist_collections_apply(
            self.db,
            PERSIST_COLLECTION_ITER(
                <persist_collection_iter_f>Database.c_apply_callback,
                <void *>collect
            )
        )

        return names

    @staticmethod
    cdef bint c_apply_callback(void *arg, const char *name):
        cdef object cb = <object>arg
        cb(name)


cdef class Collection(object):
    def __dealloc__(self):
        if self.collection != <persist_collection_t>NULL:
            persist_collection_close(self.collection)

    @staticmethod
    cdef Collection wrap(object parent, persist_collection_t ptr):
        cdef Collection ret

        if ptr == <persist_collection_t>NULL:
            return None

        ret = Collection.__new__(Collection)
        ret.collection = ptr
        ret.parent = parent
        ret.queries = []

        return ret

    cdef persist_collection_t unwrap(self) nogil:
        return self.collection

    def close(self):
        if self.parent.db != <persist_db_t>NULL:
            # Close our queries
            for quer in self.queries:
                quer.close()
            persist_collection_close(self.collection)

        self.collection = <persist_collection_t>NULL

    def get(self, id, default=None):
        cdef rpc_object_t ret

        if not self.parent.is_open:
            raise ValueError('Database is closed')

        if not isinstance(id, str):
            raise TypeError('Id needs to be a string')

        ret = persist_get(self.collection, id.encode('utf-8'))

        if ret == <rpc_object_t>NULL:
            return default

        return Object.wrap(ret).unpack()

    def set(self, value):
        cdef Object rpc_value
        cdef int ret

        if not self.parent.is_open:
            raise ValueError('Database is closed')

        rpc_value = Object(value)
        if rpc_value.type != ObjectType.DICTIONARY:
            raise TypeError('Value has to be a dictionary')

        with nogil:
            ret = persist_save(self.collection, rpc_value.unwrap())

        if ret != 0:
            check_last_error()

    def insert_many(self, values):
        cdef Object rpc_value
        cdef int ret

        if not self.parent.is_open:
            raise ValueError('Database is closed')

        rpc_value = Object(values)
        if rpc_value.type != ObjectType.ARRAY:
            raise TypeError('Value has to be a dictionary')

        with nogil:
            ret = persist_save_many(self.collection, rpc_value.unwrap())

        if ret != 0:
            check_last_error()

    def delete(self, id):
        if not self.parent.is_open:
            raise ValueError('Database is closed')

        if not isinstance(id, str):
            raise TypeError('Id needs to be a string')

        persist_delete(self.collection, id.encode('utf-8'))

    def count(self, rules):
        cdef ssize_t result
        cdef rpc_object_t rpc_rules = Object(rules).unwrap()

        with nogil:
            result = persist_count(self.collection, rpc_rules)

        if result == -1:
            check_last_error()

        return result

    def query(self, rules, sort=None, descending=False, offset=None, limit=None):
        cdef persist_iter_t iter
        cdef persist_query_params params
        cdef Object rpc_rules = Object(rules);
        cdef rpc_object_t raw_rules = rpc_rules.unwrap()

        if not self.parent.is_open:
            raise ValueError('Database is closed')

        memset(&params, 0, sizeof(params))

        if sort is not None:
            b_sort = sort.encode('utf-8')
            params.sort_field = b_sort

        if descending:
            params.descending = True

        if offset is not None:
            params.offset = offset

        if limit is not None:
            params.limit = limit

        with nogil:
            iter = persist_query(self.collection, raw_rules, &params)

        if iter == <persist_iter_t>NULL:
            check_last_error()

        citer = CollectionIterator.wrap(self, iter)
        self.queries.append(citer)

        return citer


cdef class CollectionIterator(object):
    def __dealloc__(self):
        if self.iter != <persist_iter_t>NULL:
            persist_iter_close(self.iter)

    def __iter__(self):
        return self

    def __next__(self):
        cdef rpc_object_t result

        if not self.cnt:
            raise StopIteration

        if persist_iter_next(self.iter, &result) != 0:
            check_last_error()

        if result == <rpc_object_t>NULL:
            self.cnt = False
            raise StopIteration

        return Object.wrap(result).unpack()

    def next(self):
        return self.__next__()

    def close(self):
        cdef persist_collection_t parent_col
        parent_col = self.parent.unwrap()
        if parent_col != <persist_collection_t>NULL and self.iter != <persist_iter_t>NULL:
            # Close our iterator
            persist_iter_close(self.iter)
            self.iter = <persist_iter_t>NULL

    @staticmethod
    cdef CollectionIterator wrap(object parent, persist_iter_t iter):
        cdef CollectionIterator ret

        ret = CollectionIterator.__new__(CollectionIterator)
        ret.iter = iter
        ret.cnt = True
        ret.parent = parent

        return ret


cdef check_last_error():
    cdef const char *errmsg
    cdef int errcode

    errcode = persist_get_last_error(&errmsg)
    raise PersistException(errcode, errmsg.decode('utf-8'))
