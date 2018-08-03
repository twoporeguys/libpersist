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

from librpc import ObjectType
from librpc cimport Object
cimport persist


cdef class Database(object):
    def __init__(self, path, driver, params=None):
        cdef Object rpc_params = Object(params)

        if not isinstance(path, str):
            raise TypeError('Path needs to be a string')

        if not isinstance(driver, str):
            raise TypeError('Driver needs to be a string')

        if params and not isinstance(params, Object):
            raise TypeError('Params needs to be a librpc Object or None')

        self.db = persist_open(path.encode('utf-8'), driver.encode('utf-8'), rpc_params.unwrap())

        if self.db == <persist_db_t>NULL:
            raise ValueError('Cannot open database {}'.format(path))

    def __dealloc__(self):
        if self.db != <persist_db_t>NULL:
            persist_close(self.db)

    def collection_exists(self, name):
        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        return persist_collection_exists(self.db, name.encode('utf-8'))

    def get_collection(self, name, create=False):
        cdef persist_collection_t collection

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not create and not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        collection = persist_collection_get(self.db, name.encode('utf-8'), create)

        return Collection.wrap(collection)

    def create_collection(self, name):
        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if persist_collection_exists(self.db, name.encode('utf-8')):
            raise ValueError('Collection {} already exists'.format(name))

        return self.get_collection(name, True)

    def remove_collection(self, name):
        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        persist_collection_remove(self.db, name.encode('utf-8'))

    def get_collection_metadata(self, name):
        cdef rpc_object_t metadata

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        metadata = persist_collection_get_metadata(self.db, name.encode('utf-8'))

        return Object.wrap(metadata).unpack()

    def set_collection_metadata(self, name, metadata):
        cdef Object rpc_metadata = Object(metadata)

        if not isinstance(name, str):
            raise TypeError('Collection name needs to be a string')

        if not persist_collection_exists(self.db, name.encode('utf-8')):
            raise NameError('Collection {} does not exist'.format(name))

        persist_collection_set_metadata(self.db, name.encode('utf-8'), rpc_metadata.unwrap())

    def list_collections(self):
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
    cdef Collection wrap(persist_collection_t ptr):
        cdef Collection ret

        if ptr == <persist_collection_t>NULL:
            return None

        ret = Collection.__new__(Collection)
        ret.collection = ptr

        return ret

    cdef persist_collection_t unwrap(self) nogil:
        return self.collection

    def get(self, id, default=None):
        cdef rpc_object_t ret

        if not isinstance(id, str):
            raise TypeError('Id needs to be a string')

        ret = persist_get(self.collection, id.encode('utf-8'))

        if ret == <rpc_object_t>NULL:
            return default

        return Object.wrap(ret).unpack()

    def set(self, id, value):
        cdef Object rpc_value

        if not isinstance(id, str):
            raise TypeError('Id needs to be a string')

        rpc_value = Object(value)
        if rpc_value.type != ObjectType.DICTIONARY:
            raise TypeError('Value has to be a dictionary')

        persist_save(self.collection, id.encode('utf-8'), rpc_value.unwrap())

    def delete(self, id):
        if not isinstance(id, str):
            raise TypeError('Id needs to be a string')

        persist_delete(self.collection, id.encode('utf-8'))

    def query(self, params):
        cdef persist_iter_t iter
        cdef Object rpc_params = Object(params)

        iter = persist_query(self.collection, rpc_params.unwrap())
        return CollectionIterator.wrap(iter)


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

        result = persist_iter_next(self.iter)
        if result == <rpc_object_t>NULL:
            self.cnt = False
            raise StopIteration

        return Object.wrap(result).unpack()

    def next(self):
        return self.__next__()

    @staticmethod
    cdef CollectionIterator wrap(persist_iter_t iter):
        cdef CollectionIterator ret

        ret = CollectionIterator.__new__(CollectionIterator)
        ret.iter = iter
        ret.cnt = True

        return ret
