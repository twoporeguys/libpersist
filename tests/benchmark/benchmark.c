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
#include <glib.h>
#include <persist.h>

static const char *filename = "/tmp/benchmark.db";
static const char *driver = "sqlite";
static int n_inserts = 10000;
static int inserts_per_tx = 100;
static int payload_size = 1024;

static GOptionEntry arguments[] = {
	{
		.long_name = "file",
		.short_name = 'f',
		.description = "Database path",
		.arg = G_OPTION_ARG_STRING,
		.arg_data = &filename
	},
	{
		.long_name = "driver",
		.short_name = 'd',
		.description = "Driver name",
		.arg = G_OPTION_ARG_STRING,
		.arg_data = &driver,
	},
	{
		.long_name = "size",
		.short_name = 's',
		.description = "Payload size",
		.arg = G_OPTION_ARG_INT,
		.arg_data = &payload_size
	},
	{
		.long_name = "num-inserts",
		.short_name = 'n',
		.description = "Number of inserts",
		.arg = G_OPTION_ARG_INT,
		.arg_data = &n_inserts
	},
	{
		.long_name = "num-per-transaction",
		.short_name = 't',
		.description = "Number of inserts in a transaction",
		.arg = G_OPTION_ARG_INT,
		.arg_data = &inserts_per_tx
	},
	{ }
};

int main(int argc, char *argv[])
{
	GError *err = NULL;
	GOptionContext *context;
	persist_db_t db;
	persist_collection_t col;
	persist_iter_t iter;
	rpc_object_t obj;
	const char *errmsg;
	char *uuid;
	uint8_t *blob;
	size_t len = (size_t)payload_size;
	int64_t start;
	int64_t end;
	double diff;
	int i, j;

	context = g_option_context_new("");
	g_option_context_add_main_entries(context, arguments, NULL);
	g_option_context_parse(context, &argc, &argv, &err);

	db = persist_open(filename, driver, NULL);
	if (db == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "Cannot open database: %s\n", errmsg);
		return (1);
	}

	col = persist_collection_get(db, "benchmark", true);
	if (col == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "Cannot open collection: %s\n", errmsg);
		return (1);
	}

	blob = g_malloc(len);
	for (i = 0; i < payload_size; i++)
		blob[i] = (uint8_t)i;

	start = g_get_monotonic_time();


	for (i = 0; i < n_inserts / inserts_per_tx; i++) {
		if (persist_start_transaction(db) != 0) {
			persist_get_last_error(&errmsg);
			fprintf(stderr, "Cannot start transaction: %s\n", errmsg);
			return (1);
		}

		for (j = 0; j < inserts_per_tx; j++) {
			uuid = g_uuid_string_random();
			obj = rpc_object_pack("{s,s,i,B}",
			    "id", uuid,
			    "string", "test",
			    "num", (int64_t)i,
			    "data", blob, len, NULL);

			persist_save(col, obj);
			g_free(uuid);
		}

		if (persist_commit_transaction(db) != 0) {
			persist_get_last_error(&errmsg);
			fprintf(stderr, "Cannot commit transaction: %s\n", errmsg);
			return (1);
		}
	}

	end = g_get_monotonic_time();
	diff = ((double)end - (double)start) / 1000 / 1000;
	printf("Total insert time: %f seconds\n", diff);
	printf("Avg number of inserts per second: %f\n", n_inserts / diff);

	iter = persist_query(col, NULL, NULL);
	if (iter == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "Cannot start query: %s\n", errmsg);
		return (1);
	}

	start = g_get_monotonic_time();

	for (;;) {
		if (persist_iter_next(iter, &obj) != 0)
			break;

		if (obj == NULL)
			break;

		rpc_release(obj);
	}

	end = g_get_monotonic_time();
	diff = ((double)end - (double)start) / 1000 / 1000;
	printf("Total query time: %f seconds\n", diff);
	printf("Avg number of rows returned per second: %f\n", n_inserts / diff);

	return (0);
}
