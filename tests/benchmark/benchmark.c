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
		.short_name = 'f',
		.description = "Database path",
		.arg = G_OPTION_ARG_STRING,
		.arg_data = &filename
	},
	{
		.short_name = 'd',
		.description = "Driver name",
		.arg = G_OPTION_ARG_STRING,
		.arg_data = &driver,
	},
	{
		.short_name = 's',
		.description = "Payload size",
		.arg = G_OPTION_ARG_INT,
		.arg_data = &payload_size
	},
	{
		.short_name = 'n',
		.description = "Number of inserts",
		.arg = G_OPTION_ARG_INT,
		.arg_data = &n_inserts
	},
	{
		.short_name = 't',
		.description = "Number of inserts in a transaction",
		.arg = G_OPTION_ARG_INT,
		.arg_data = &inserts_per_tx
	}
};

int main(int argc, char *argv[])
{
	GError *err = NULL;
	GOptionContext *context;
	persist_db_t db;
	const char *errmsg;
	int i;

	context = g_option_context_new("");
	g_option_context_add_main_entries(context, arguments, NULL);
	g_option_context_parse(context, &argc, &argv, &err);

	db = persist_open(filename, driver, NULL);
	if (db == NULL) {
		persist_get_last_error(&errmsg);
		fprintf(stderr, "Cannot open database: %s\n", errmsg);
		return (1);
	}

	for (i = 0; i < n_inserts; i++) {

	}
}