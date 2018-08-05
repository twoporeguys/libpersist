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
#include <unistd.h>
#include <glib.h>
#include <rpc/rpc.h>
#include <persist.h>

#define BUFLEN	1024

static int open_db(const char *, const char *);
static int print_object(rpc_object_t);
static rpc_object_t ingest_object(void);
static int cmd_list(int, char *[]);
static int cmd_query(int, char *[]);
static int cmd_get_metadata(int, char *[]);
static int cmd_set_metadata(int, char *[]);
static int cmd_get(int, char *[]);
static int cmd_insert(int, char *[]);
static int cmd_delete(int, char *[]);
static void usage(void);

static const char *file;
static const char *format = "native";
static const char *driver = "sqlite";
static char **args;
static persist_db_t db;
static GOptionContext *context;

static struct {
	const char *name;
	int (*fn)(int argc, char *argv[]);
} commands[] = {
	{ "list", cmd_list },
	{ "query", cmd_query },
	{ "get-metadata", cmd_get_metadata },
	{ "set-metadata", cmd_set_metadata },
	{ "get", cmd_get },
	{ "insert", cmd_insert },
	{ "delete", cmd_delete },
	{ }
};

static const GOptionEntry options[] = {
	{ "file", 'f', 0, G_OPTION_ARG_STRING, &file, "Database path", NULL },
	{ "format", 't', 0, G_OPTION_ARG_STRING, &format, "Input/output format", NULL },
	{ "driver", 'd', 0, G_OPTION_ARG_STRING, &driver, "Driver", NULL },
	{ G_OPTION_REMAINING, 0, 0, G_OPTION_ARG_STRING_ARRAY, &args, "", NULL },
	{ }
};

static int
open_db(const char *filename, const char *driver)
{
	const char *errmsg;

	db = persist_open(filename, driver, NULL);
	if (db == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "Cannot open database: %s\n", errmsg);
		return (-1);
	}

	return (0);
}

static rpc_object_t
ingest_object(void)
{
	rpc_object_t result;
	rpc_object_t error;
	void *blob = NULL;
	size_t len = 0;
	size_t pos = 0;
	size_t ret;

	for (;;) {
		if (len - pos < BUFLEN) {
			len += BUFLEN;
			blob = realloc(blob, len);
		}

		ret = fread(blob + pos, 1, BUFLEN, stdin);
		pos += ret;

		if (ret < BUFLEN)
			break;
	}

	result = rpc_serializer_load(format, blob, pos);
	if (result == NULL) {
		error = rpc_get_last_error();
		fprintf(stderr, "cannot read object: %s\n",
		    rpc_error_get_message(error));
		return (NULL);
	}

	return (result);
}

static int
print_object(rpc_object_t obj)
{
	g_autofree char *str = NULL;
	g_autofree void *output = NULL;
	rpc_object_t error;
	size_t len;

	if (g_strcmp0(format, "native") == 0) {
		str = rpc_copy_description(obj);
		printf("%s\n", str);
		return (0);
	}

	if (rpc_serializer_dump(format, obj, &output, &len) != 0) {
		error = rpc_get_last_error();
		fprintf(stderr, "cannot serialize: %s\n",
		    rpc_error_get_message(error));
		return (-1);
	}

	fwrite(output, len, 1, stdout);
	printf("\n");
	return (0);
}

static int
cmd_list(int argc, char *argv[])
{
	persist_collections_apply(db, ^(const char *name) {
		printf("%s\n", name);
		return ((bool)true);
	});

	return (0);
}

static int
cmd_query(int argc, char *argv[])
{
	GOptionContext *context;
	persist_collection_t col;
	persist_iter_t iter;
	rpc_object_t obj;
	const char *errmsg;

	if (argc < 1) {
		usage();
		return (1);
	}

	col = persist_collection_get(db, argv[0], false);
	if (col == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "cannot open collection: %s\n", errmsg);
		return (-1);
	}

	iter = persist_query(col, NULL, NULL);
	for (;;) {
		obj = persist_iter_next(iter);
		if (obj == NULL)
			break;

		print_object(obj);
	}

	return (0);
}

static int
cmd_get_metadata(int argc, char *argv[])
{
	rpc_auto_object_t metadata = NULL;
	const char *errmsg;

	if (argc < 1) {
		usage();
		return (1);
	}

	metadata = persist_collection_get_metadata(db, argv[0]);
	if (metadata == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "cannot read metadata: %s\n", errmsg);
		return (1);
	}

	print_object(metadata);
	return (0);
}

static int
cmd_set_metadata(int argc, char *argv[])
{

}

static int
cmd_get(int argc, char *argv[])
{
	persist_collection_t col;
	rpc_auto_object_t obj = NULL;
	const char *errmsg;

	if (argc < 2) {
		usage();
		return (1);
	}

	col = persist_collection_get(db, argv[0], false);
	obj = persist_get(col, argv[1]);
	if (obj == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "cannot find object: %s\n", errmsg);
		return (1);
	}

	print_object(obj);
	return (0);
}

static int
cmd_insert(int argc, char *argv[])
{
	persist_collection_t col;
	rpc_auto_object_t obj = NULL;
	const char *errmsg;

	if (argc < 2) {
		usage();
		return (1);
	}

	col = persist_collection_get(db, argv[0], false);
	if (col == NULL) {

	}

	obj = ingest_object();

	if (obj == NULL)
		return (1);

	if (persist_save(col, obj) != 0) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "cannot save object: %s\n", errmsg);
	}

	return (0);
}

static int
cmd_delete(int argc, char *argv[])
{
	persist_collection_t col;

	if (argc < 2) {
		usage();
		return (1);
	}

	col = persist_collection_get(db, argv[0], false);
	if (col == NULL) {

	}

	if (persist_delete(col, argv[1]) != 0) {

	}
}

static void
usage(void)
{
	g_autofree char *help;

	help = g_option_context_get_help(context, true, NULL);
	fprintf(stderr, "%s", help);
}

int
main(int argc, char *argv[])
{
	GError *err = NULL;
	const char *cmd;
	int nargs;
	int i;

	context = g_option_context_new("<COMMAND> [ARGUMENTS...] - interact with libpersist database");
	g_option_context_add_main_entries(context, options, NULL);

	if (!g_option_context_parse(context, &argc, &argv, &err)) {
		usage();
		return (1);
	}

	if (open_db(file, driver) != 0)
		return (1);

	if (args == NULL) {
		fprintf(stderr, "No command specified.\n");
		usage();
		return (1);
	}

	nargs = g_strv_length(args);
	cmd = args[0];

	for (i = 0; commands[i].name != NULL; i++) {
		if (!g_strcmp0(commands[i].name, cmd))
			return (commands[i].fn(nargs - 1, args + 1));
	}

	fprintf(stderr, "Command %s not found\n", cmd);
	usage();
	return (1);
}
