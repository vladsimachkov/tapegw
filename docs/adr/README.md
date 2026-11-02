# ADR

Architectural Decision Records. Порядок задан в `docs/ARCHITECTURE.md` (правило 3 и
раздел «Репозиторий и правила для агента»): любое отступление от документа оформляется
здесь.

Нумерация сквозная, формат — [`0000-template.md`](0000-template.md).

| № | Название | Статус |
| --- | --- | --- |
| [0001](0001-restore-object-status-code.md) | RestoreObject всегда отвечает 200 | принято |
| [0002](0002-metrics-transport.md) | Транспорт метрик: свой Prometheus-экспортёр | принято |
| [0003](0003-standard-storage-class.md) | Классы хранения: входящие отвергаем, исходящие всегда GLACIER | принято |
| [0004](0004-landing-eviction-race.md) | Гонка GET и вытеснения landing-копии | принято |
| [0005](0005-key-rewrite-advisory-lock.md) | Перезапись ключа: advisory-лок вместо ретрая | принято |
| [0006](0006-libphobos-linkage.md) | Линковка с libphobos_store, вызовы сериализованы | принято |
