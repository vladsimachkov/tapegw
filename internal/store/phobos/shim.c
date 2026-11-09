/* SPDX-License-Identifier: Apache-2.0 */
#include "shim.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>

int tapegw_put(const char *objid, int fd, long size, int family,
               const char *library, const char *layout_name, char **keys,
               char **vals, int nattrs, char **lyt_keys, char **lyt_vals,
               int nlyt, char **tags, int ntags, long *tid_out) {
    *tid_out = tapegw_tid();

    struct pho_xfer_desc xd;
    memset(&xd, 0, sizeof(xd));
    xd.xd_op = PHO_XFER_OP_PUT;
    /* Без family Phobos берёт семейство по умолчанию из конфига, и на стенде
     * это лента: put уходит в драйв и получает ENODEV. */
    /* libphobos не читает профиль: нужны явные family, library и layout,
     * иначе -19 ENODEV («No compatible device found for write alloc»),
     * спайк M0-02. */
    xd.xd_params.put.family = (enum rsc_family)family;
    xd.xd_params.put.library = library;
    xd.xd_params.put.layout_name = layout_name;
    for (int i = 0; i < nlyt; i++)
        pho_attr_set(&xd.xd_params.put.lyt_params, lyt_keys[i], lyt_vals[i]);

    xd.xd_ntargets = 1;
    xd.xd_targets = calloc(1, sizeof(struct pho_xfer_target));
    if (xd.xd_targets == NULL) {
        /* clean ниже не дошёл бы — владение тегами ещё на нас. */
        for (int i = 0; i < ntags; i++)
            free(tags[i]);
        free(tags);
        return -ENOMEM;
    }

    /* Владение переходит к xd: pho_xfer_desc_clean освободит массив и строки
     * (xfer_put_param_clean → string_array_free). */
    xd.xd_params.put.tags.strings = tags;
    xd.xd_params.put.tags.count = (size_t)ntags;

    /* pho_xfer_desc_clean освобождает цели и атрибуты; objid принадлежит
     * вызывающему и clean её не трогает (см. заголовок библиотеки). */
    struct pho_xfer_target *t = &xd.xd_targets[0];
    t->xt_objid = objid;
    t->xt_fd = fd;
    t->xt_size = (ssize_t)size;
    for (int i = 0; i < nattrs; i++)
        pho_attr_set(&t->xt_attrs, keys[i], vals[i]);

    int rc = phobos_put(&xd, 1, NULL, NULL);
    if (rc == 0 && xd.xd_rc != 0)
        rc = xd.xd_rc;
    if (rc == 0 && t->xt_rc != 0)
        rc = t->xt_rc;

    pho_xfer_desc_clean(&xd);
    free(xd.xd_targets);
    return rc;
}

int tapegw_get(const char *objid, int fd, int scope, long *tid_out) {
    *tid_out = tapegw_tid();
    struct pho_xfer_desc xd;
    memset(&xd, 0, sizeof(xd));
    xd.xd_op = PHO_XFER_OP_GET;
    xd.xd_params.get.scope = (enum dss_obj_scope)scope;
    xd.xd_ntargets = 1;
    xd.xd_targets = calloc(1, sizeof(struct pho_xfer_target));
    if (xd.xd_targets == NULL)
        return -ENOMEM;
    xd.xd_targets[0].xt_objid = objid;
    xd.xd_targets[0].xt_fd = fd;

    int rc = phobos_get(&xd, 1, NULL, NULL);
    if (rc == 0 && xd.xd_rc != 0)
        rc = xd.xd_rc;
    pho_xfer_desc_clean(&xd);
    return rc;
}

int tapegw_delete(const char *objid, long *tid_out) {
    *tid_out = tapegw_tid();
    struct pho_xfer_desc xd;
    memset(&xd, 0, sizeof(xd));
    xd.xd_op = PHO_XFER_OP_DEL;
    xd.xd_ntargets = 1;
    xd.xd_targets = calloc(1, sizeof(struct pho_xfer_target));
    if (xd.xd_targets == NULL)
        return -ENOMEM;
    xd.xd_targets[0].xt_objid = objid;

    int rc = phobos_delete(&xd, 1);
    if (rc == 0 && xd.xd_rc != 0)
        rc = xd.xd_rc;
    pho_xfer_desc_clean(&xd);
    return rc;
}

int tapegw_getmd(const char *objid, long *size_out, int *version_out,
                 int *found, long *tid_out) {
    *tid_out = tapegw_tid();
    *found = 1;

    struct pho_xfer_desc xd;
    memset(&xd, 0, sizeof(xd));
    xd.xd_op = PHO_XFER_OP_GETMD;
    xd.xd_ntargets = 1;
    xd.xd_targets = calloc(1, sizeof(struct pho_xfer_target));
    if (xd.xd_targets == NULL)
        return -ENOMEM;

    struct pho_xfer_target *t = &xd.xd_targets[0];
    t->xt_objid = objid;

    int rc = phobos_getmd(&xd, 1);
    if (rc == 0 && t->xt_rc != 0)
        rc = t->xt_rc;
    if (rc == -ENOENT) {
        *found = 0;
        pho_xfer_desc_clean(&xd);
        free(xd.xd_targets);
        return 0;
    }
    if (rc == 0) {
        *size_out = (long)t->xt_size;
        *version_out = t->xt_version;
    }
    pho_xfer_desc_clean(&xd);
    free(xd.xd_targets);
    return rc;
}
