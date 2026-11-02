// Package metrics — собственный Prometheus-экспортёр tapegw.
//
// Транспорт выбран в ADR-0002: versitygw v1.10.0 метрик в Prometheus не отдаёт
// вообще (только StatsD/DogStatsD), поэтому счётчики — наши, включая S3-операции.
// Снимать их будем middleware-ом, смонтированным через embedgw.Config.S3Options
// и s3api.WithMiddleware — тот же шов закрывает и SlowDown при переполнении landing.
//
// Пакет появится на M2 (каркас) и M9 (полный набор). Имена метрик фиксируются
// до M2, переименовывать после появления дашбордов дорого.
//
// Набор из docs/ARCHITECTURE.md:
//
//	очереди:      объекты и байты по state, возраст самого старого pending,
//	              restore по state и tier
//	архивация:    байт/с и объектов/с, длительность батча, ошибки phobos_put по коду
//	restore:      гистограмма «запрос → restored», ошибки phobos_get
//	диски:        заполненность landing и cache, объём, вытесненный janitor
//	s3:           запросы по операции и коду ответа
package metrics
