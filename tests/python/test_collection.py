#
# Copyright 2018 Jakub Klama <jakub.klama@gmail.com>
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

import pytest
import librpc
import persist


TRIVIAL_OBJ = librpc.Dictionary({
    'id': 'trivial_insert',
    'foo': 5,
    'binary': b'blah',
    'nothing': None
})


DUPLICATE_OBJ = librpc.Dictionary({
    'id': 'duplicate_insert',
    'foo': 5,
    'binary': b'blah',
    'nothing': None
})


class TestCollection(object):
    def test_trivial_insert(self, db):
        col = db.get_collection('test', True)
        assert col is not None

        col.set(TRIVIAL_OBJ)
        obj = col.get('trivial_insert')
        assert obj == TRIVIAL_OBJ

        col.delete('trivial_insert')
        obj = col.get('trivial_insert')
        assert obj is None

    def test_duplicate_insert(self, db):
        col = db.get_collection('test', True)
        assert col is not None

        col.set(DUPLICATE_OBJ)
        obj = col.get('duplicate_insert')
        assert obj == DUPLICATE_OBJ

        another_obj = DUPLICATE_OBJ.copy()
        another_obj['different_field'] = librpc.uint(5)
        col.set(another_obj)
        obj = col.get('duplicate_insert')
        assert obj == another_obj

        col.delete('duplicate_insert')
        obj = col.get('duplicate_insert')
        assert obj is None

    def test_invalid_insert(self, db):
        col = db.get_collection('test', True)
        assert col is not None

        with pytest.raises(TypeError):
            col.set(5)

        with pytest.raises(TypeError):
            col.set(None)

        with pytest.raises(persist.PersistException):
            col.set({'not an id': 5})

        with pytest.raises(persist.PersistException):
            col.set({'id': -1})

    def test_invalid_delete(self, db):
        col = db.get_collection('test', True)
        assert col is not None

        with pytest.raises(TypeError):
            col.delete(5)

        with pytest.raises(TypeError):
            col.delete(None)

        with pytest.raises(TypeError):
            col.delete(TRIVIAL_OBJ)

    def test_nonexistent_get(self, db):
        col = db.get_collection('test', True)
        assert col is not None
        assert col.get('nonexistent') is None
