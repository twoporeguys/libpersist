cdef extern from "persist.h":
    persist_db_t persist_open(const char *path, const char *driver,
        rpc_object_t params)
    void persist_close(persist_db_t db)
    persist_collection_t persist_collection_get(persist_db_t db, const char *name)
    bool persist_collection_exists(persist_db_t db, const char *name)
    int persist_collection_remove(persist_db_t db, const char *name)
    rpc_object_t persist_collection_get_metadata(persist_db_t db, const char *name)
    int persist_collection_set_metadata(persist_db_t db, const char *name,
        rpc_object_t metadata)
    void persist_collections_apply(persist_db_t db)
    rpc_object_t persist_get(persist_collection_t col, const char *id)
    bool persist_query(persist_collection_t col, rpc_object_t query)
    int persist_save(persist_collection_t col, const char *id, rpc_object_t obj)
    int persist_delete(persist_collection_t col, const char *id)
    int persist_get_last_error(char **msgp)
