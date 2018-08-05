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

#include <glib.h>
#include "linker_set.h"
#include "internal.h"

struct error
{
	int	code;
	char *	message;
};

static void persist_error_free(void *);

SET_DECLARE(drv_set, struct persist_driver);
static GPrivate persist_last_error = G_PRIVATE_INIT(persist_error_free);

const struct persist_driver *
persist_find_driver(const char *name)
{
	struct persist_driver **d;

	SET_FOREACH(d, drv_set) {
		if (!g_strcmp0((*d)->pd_name, name))
			return (*d);
	}

	return (NULL);
}

static void
persist_error_free(void *error)
{
	struct error *err = error;

	g_free(err->message);
	g_free(err);
}

int
persist_get_last_error(const char **msgp)
{
	struct error *err;

	err = g_private_get(&persist_last_error);

	if (err != NULL) {
		*msgp = err->message;
		return (err->code);
	}

	*msgp = NULL;
	return (0);
}

void
persist_set_last_error(int code, const char *fmt, ...)
{
	va_list ap;
	struct error *err;

	va_start(ap, fmt);
	err = g_malloc0(sizeof(*err));
	err->code = code;
	err->message = g_strdup_vprintf(fmt, ap);
	va_end(ap);

	g_private_replace(&persist_last_error, err);
}
