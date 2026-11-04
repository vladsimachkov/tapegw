#!/usr/bin/env bash
# mutate.sh — безопасная мутация: сохранить, применить, проверить что применилось,
# прогнать тесты, восстановить.
#
# Правило «откат мутации — копированием файла, а не git checkout» в AGENTS.md не
# сработало дважды: правки до коммита в git нет, и `git checkout <файл>` стирает
# всю работу в этом файле. Механизм надёжнее правила.
#
# Использование:
#   ./hack/mutate.sh <файл> <sed-выражение> <пакет> [шаблон теста]
# Пример:
#   ./hack/mutate.sh internal/meta/lock.go \
#       's/return ErrRetentionShorten/_ = 0/' ./internal/backend/ -run TestCompliance
set -euo pipefail

if [ "$#" -lt 3 ]; then
	echo "использование: $0 <файл> <sed-выражение> <пакет> [-run шаблон]" >&2
	exit 2
fi

file=$1 expr=$2 pkg=$3
shift 3
testargs=("$@")

[ -f "$file" ] || { echo "нет файла $file" >&2; exit 1; }

backup=$(mktemp "${TMPDIR:-/tmp}/mutate.XXXXXX")
cp "$file" "$backup"
# Восстанавливаем из копии при любом выходе — по ошибке тестов, по Ctrl-C, по
# чему угодно. git checkout здесь не используется намеренно.
restore() {
	cp "$backup" "$file"
	rm -f "$backup"
	echo "--- восстановлено из копии ---"
}
trap restore EXIT INT TERM

sed -i "$expr" "$file"

if diff -q "$backup" "$file" >/dev/null; then
	# Самая частая ложная зелёность: мутация не применилась (опечатка в
	# выражении, код уже изменился), тесты прошли — и в отчёте «мутация
	# поймана», хотя ничего не проверялось.
	echo "МУТАЦИЯ НЕ ПРИМЕНИЛАСЬ: $expr ничего не изменило в $file" >&2
	exit 3
fi

echo "### мутация применена: $expr"
if go test -count=1 "$pkg" "${testargs[@]:-}"; then
	echo "ВНИМАНИЕ: тесты прошли несмотря на мутацию — проверка ничего не ловит" >&2
	exit 1
fi
echo "### мутация поймана (тесты упали, как и должно)"
