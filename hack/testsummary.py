#!/usr/bin/env python3
"""Сводка падения по `go test -json`.

Нужна потому, что при `go test ./...` имя упавшего теста тонет в перемешанном
выводе параллельных пакетов: в CI это уже стоило времени — строку
`FAIL пакет` видели, а `--- FAIL: TestX` и `exit status N` искали не там
(grep с якорем по началу строки не сработал из-за timestamp, который Actions
ставит перед каждой строкой).

Печатает только то, что нужно для первого шага разбора: какие тесты упали и
какие сообщения уровня пакета их сопровождали (`exit status`, `DATA RACE`,
`panic`). Полный JSON остаётся артефактом прогона.
"""
import json
import re
import sys

# Сообщения уровня пакета: их не видно в списке тестов, но именно они объясняют
# «FAIL без имени теста».
IMPORTANT = re.compile(r"exit status|DATA RACE|panic:|\[build failed\]|test timed out")


def main(path):
    failed_tests = []      # (пакет, тест)
    pkg_output = {}        # пакет -> буфер вывода
    failed_pkgs = set()

    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            pkg = ev.get("Package", "")
            act = ev.get("Action", "")
            out = ev.get("Output", "")
            test = ev.get("Test", "")

            if out:
                if not test:
                    pkg_output.setdefault(pkg, []).append(out)
                if IMPORTANT.search(out):
                    print(f"::error::[{pkg or '-'}] {out.strip()}")

            if act == "fail":
                if test:
                    failed_tests.append((pkg, test))
                elif not pkg_output.get(pkg):
                    failed_pkgs.add(pkg)
                else:
                    failed_pkgs.add(pkg)

    if failed_tests:
        print("\n=== упавшие тесты ===")
        for pkg, test in failed_tests:
            print(f"  {pkg}  {test}")

    if failed_pkgs:
        print("\n=== пакеты без имени теста (см. сообщения выше) ===")
        for pkg in sorted(failed_pkgs):
            tail = [l.strip() for l in pkg_output.get(pkg, []) if l.strip()]
            print(f"  {pkg}")
            for line in tail[-5:]:
                print(f"      {line}")

    if not failed_tests and not failed_pkgs:
        print("упавших тестов не найдено: сверьтесь с кодом возврата go test")


if __name__ == "__main__":
    main(sys.argv[1])
