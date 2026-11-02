#!/bin/bash
# Проверка целостности данных на LTFS-ленте после переезда между драйвами.
# Отличается от measure-ltfs-cycle.sh тем, что пишет СЛУЧАЙНЫЕ данные и
# сверяет md5, и обязательно проверяет, что каталог действительно смонтирован:
# без этой проверки dd молча пишет в пустой каталог на корневой ФС, и тест
# даёт ложное «расхождение».
set -u

CHANGER=${CHANGER:-/dev/sg10}
SRC_DEV=${SRC_DEV:-/dev/sg3}
DST_DEV=${DST_DEV:-/dev/sg2}
SRC_MNT=${SRC_MNT:-/mnt/tape1}
DST_MNT=${DST_MNT:-/mnt/tape2}
SLOT=${SLOT:-2}
DTE_SRC=${DTE_SRC:-0}
DTE_DST=${DTE_DST:-1}

mounted() { mount | awk -v m="$1" '$3==m {found=1} END{exit !found}'; }

mkdir -p "$SRC_MNT" "$DST_MNT"
umount "$DST_MNT" 2>/dev/null
mounted "$SRC_MNT" || { ltfs -o devname="$SRC_DEV" "$SRC_MNT" >/dev/null 2>&1; sleep 1; }
mounted "$SRC_MNT" || { echo "FAITAL: $SRC_MNT не смонтирован, тест бессмыслен"; exit 1; }

dd if=/dev/urandom of="$SRC_MNT/rand.bin" bs=1M count=32 status=none
sync
A=$(md5sum "$SRC_MNT/rand.bin" | cut -d' ' -f1)
echo "md5 до переезда:     $A"

umount "$SRC_MNT" || { echo "FAITAL: не смог отмонтировать"; exit 1; }
mtx -f "$CHANGER" unload "$SLOT" "$DTE_SRC" >/dev/null 2>&1; sleep 1
mtx -f "$CHANGER" load   "$SLOT" "$DTE_DST" >/dev/null 2>&1; sleep 1
ltfs -o devname="$DST_DEV" "$DST_MNT" >/dev/null 2>&1; sleep 2

mounted "$DST_MNT" || { echo "FAITAL: $DST_MNT не смонтирован"; exit 1; }
B=$(md5sum "$DST_MNT/rand.bin" 2>/dev/null | cut -d' ' -f1)
echo "md5 после переезда:  ${B:-<нет файла>}"
if [ "$A" = "$B" ]; then
    echo "ЦЕЛОСТНОСТЬ: совпало"
else
    echo "ЦЕЛОСТНОСТЬ: РАСХОЖДЕНИЕ"
    exit 1
fi
