/* SPDX-License-Identifier: Apache-2.0
 * Обёртка над libphobos_store. ADR-0006, поправка про горутину-владельцу в
 * ADR-0007, спайк M0-13.
 *
 * Вся работа с памятью C живёт здесь, а не в Go: pho_xfer_desc_clean освобождает
 * то, что выделила библиотека, и переносить эту ответственность через границу
 * cgo — значит плодить пути утечки на каждом возврате.
 */
#ifndef TAPEGW_PHOBOS_SHIM_H
#define TAPEGW_PHOBOS_SHIM_H

#include <stddef.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <phobos_store.h>

/* tid вызывающего потока: спайк M0-13 проверяет фактом из C, что все вызовы
 * libphos приходят из одного потока ОС. */
static inline long tapegw_tid(void) { return (long)syscall(SYS_gettid); }

static inline int tapegw_init(void) { return phobos_init(); }
static inline void tapegw_fini(void) { phobos_fini(); }

/* Записать объект из открытого файла. Данные читаются от текущей позиции,
 * поэтому позицию обязана выставить вызывающая сторона (store.Rewind). */
int tapegw_put(const char *objid, int fd, long size, int family,
               const char *library, const char *layout_name, char **keys,
               char **vals, int nattrs, long *tid_out);

/* Метаданные объекта: размер и версия. *found = 0, если объекта нет. */
int tapegw_getmd(const char *objid, long *size_out, int *version_out,
                 int *found, long *tid_out);

#endif /* TAPEGW_PHOBOS_SHIM_H */
