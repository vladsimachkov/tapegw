# Две цели: check — полный прогон перед пушем, mutate — безопасная мутация.
#
# check существует потому, что CI проверяет больше, чем «go build && go test»:
# gofmt, vet, staticcheck и то, что `go mod tidy` не оставляет изменений.
# Последнее уже поймало меня на пуше: новый прямой импорт (fasthttp) надо
# относить в прямые зависимости go.mod, и обнаружилось это только в CI.

GO       ?= go
# staticcheck лежит в GOBIN, а не обязательно в PATH.
GOBIN    ?= $(shell $(GO) env GOPATH)/bin
PKG      ?= ./...
DSN      ?= postgres://tapegw:tapegw@localhost:55432/phobos?search_path=tapegw_test

hooks:
	@git config core.hooksPath .githooks && echo "### хуки: .githooks (pre-push гоняет make check перед push)"

check:
	@echo "### gofmt"; out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "$$out"; exit 1; fi
	@echo "### go vet";        $(GO) vet $(PKG)
	@echo "### staticcheck";   $(GOBIN)/staticcheck $(PKG)
	@echo "### go mod tidy";   $(GO) mod tidy; \
		if ! git diff --quiet go.mod go.sum; then \
			echo "go mod tidy оставил изменения в go.mod/go.sum — закоммить их"; \
			git diff go.mod go.sum | head -20; exit 1; \
		fi
	@echo "### тесты";         TAPEGW_TEST_DSN="$(DSN)" $(GO) test -race -count=1 $(PKG)
	@echo "### чисто"

# make mutate FILE=internal/meta/lock.go 'EXPR=s/return ErrRetentionShorten/_ = 0/' \
#        PKG=./internal/backend/ ARGS="-run TestCompliance"
#
# Многострочная правка (SQL): MODE=perl, а выражение применяется perl -0777.
# Мутация файла миграции требует DROP_SCHEMA=<имя>, иначе goose держит уже
# применённую версию и правка файла до теста не доходит.
mutate:
	MODE="$(MODE)" DROP_SCHEMA="$(DROP_SCHEMA)" PSQL="$(PSQL)" ./hack/mutate.sh "$(FILE)" "$(EXPR)" "$(PKG)" $(ARGS)

.PHONY: check mutate

# Стендовые скрипты не правим на ВМ: обновляем одной командой из репозитория,
# иначе копия на ВМ расходится с тем, что в git, и вывод перестаёт что-то значить.
STAND_HOST ?= root@192.168.1.144
STAND_DIR  ?= /root/tapegw-src
stand-sync:
	@mkdir -p .build
	@ver=$$(git rev-parse --short HEAD); \
	  if [ -n "$$(git status --porcelain -- hack)" ]; then ver="$$ver-dirty"; fi; \
	  printf '%s\n' "$$ver" > .build/VERSION
	scp hack/stand-status hack/stand-up .build/VERSION $(STAND_HOST):$(STAND_DIR)/hack/
