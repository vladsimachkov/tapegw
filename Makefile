# Единная цель пока — безопасная мутация. См. hack/mutate.sh: правки до коммита
# не лежат в git, и откат через `git checkout <файл>` стирает всю работу в
# файле. Правило в AGENTS.md не сработало дважды, поэтому механизм.
#
#   make mutate FILE=internal/meta/lock.go \
#        'EXPR=s/return ErrRetentionShorten/_ = 0/' \
#        PKG=./internal/backend/ ARGS="-run TestCompliance"
mutate:
	./hack/mutate.sh "$(FILE)" "$(EXPR)" "$(PKG)" $(ARGS)

.PHONY: mutate
