#!/bin/bash
# Замер цикла «umount LTFS -> unload -> MOVE MEDIUM -> load -> mount LTFS»
# на виртуальных лентах mhVTL. См. docs/spikes/M0-07-mhvtl-ltfs.md
#
# Перемещение делается через SCSI medium changer (mtx), а не через vtlcmd:
# vtlcmd load на драйв молча возвращает 0 и ничего не делает, если картриджа
# нет в map. TLC в tapegw будет дёргать ровно changer, так что mtx ближе к
# боевому пути.
set -u

CHANGER=${CHANGER:-/dev/sg10}
# ВНИМАНИЕ на соответствие: номер DTE у changer НЕ совпадает с порядком
# sg-устройств. Проверено по vpd_pg83 и mt status: DTE 0 = /dev/st1 = /dev/sg3,
# DTE 1 = /dev/st0 = /dev/sg2. Ошибка в этом соответствии даёт «носителя нет»
# на живом драйве, поэтому klipper-tlc обязан строить карту по серийнику
# привода, а не по порядку появления /dev/sgN.
SRC_DEV=${SRC_DEV:-/dev/sg3}
DST_DEV=${DST_DEV:-/dev/sg2}
SRC_MNT=${SRC_MNT:-/mnt/tape1}
DST_MNT=${DST_MNT:-/mnt/tape2}
SLOT=${SLOT:-1}          # слот, где лежит картридж
DTE_SRC=${DTE_SRC:-0}    # номер Data Transfer Element источника
DTE_DST=${DTE_DST:-1}    # и получателя

now() { date +%s%3N; }

mkdir -p "$SRC_MNT" "$DST_MNT"
umount "$SRC_MNT" 2>/dev/null
umount "$DST_MNT" 2>/dev/null

# привести к известному состоянию: все драйвы пусты, картридж в исходном
for d in 0 1 2 3; do mtx -f "$CHANGER" unload "$SLOT" $d >/dev/null 2>&1; done
sleep 1
mtx -f "$CHANGER" load "$SLOT" "$DTE_SRC" >/dev/null 2>&1
sleep 1
ltfs -o devname="$SRC_DEV" "$SRC_MNT" >/dev/null 2>&1 || {
    echo "не смог смонтировать $SRC_DEV"; exit 1; }
sleep 1
echo "до цикла: на ленте [$(ls "$SRC_MNT" | tr '\n' ' ')]"

t0=$(now); umount "$SRC_MNT";                              t1=$(now)
mtx -f "$CHANGER" unload "$SLOT" "$DTE_SRC";             t2=$(now)
mtx -f "$CHANGER" load "$SLOT" "$DTE_DST";               t3=$(now)
ltfs -o devname="$DST_DEV" "$DST_MNT" >/dev/null 2>&1;    sleep 1; t4=$(now)

awk -v a=$t0 -v b=$t1 -v c=$t2 -v d=$t3 -v e=$t4 'BEGIN{
  printf "umount LTFS        %6.2f s\n", (b-a)/1000
  printf "выгрузка в слот    %6.2f s\n", (c-b)/1000
  printf "загрузка в драйв   %6.2f s\n", (d-c)/1000
  printf "mount LTFS         %6.2f s\n", (e-d)/1000
  printf "--- цикл целиком   %6.2f s\n", (e-a)/1000 }'

echo "после цикла: на ленте [$(ls "$DST_MNT" | tr '\n' ' ')]"
