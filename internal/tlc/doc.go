// Package tlc — TLC-сервер: манипулятор ленточной библиотеки как штатный
// демон Phobos.
//
// Реализация — этап M7/M8. Здесь зафиксирован контракт, чтобы M1 не пришлось
// его выдумывать задним числом.
//
// Стоковый адаптер lib_scsi в Phobos подключается к klipper-tlc по TCP и не
// подозревает, что на другом конце не phobos_tlc, поэтому Phobos не патчится
// (решение D4).
//
// Протокол (src/core/serializer/pho_proto_tlc.proto в Phobos, protobuf2):
//
//	кадр: 4 байта длины (network byte order) | 1 байт версии протокола (=2) |
//	      PhoTlcRequest | PhoTlcResponse
//	запросы: ping, drive_lookup, load, unload, status, refresh, stat
//	ответ несёт тот же id в req_id либо error{rc=-errno, message}
//
// status.lib_data — JSON-массив ровно в формате scan_element() из
// src/tlc/tlc_library.c. Формат сверяется с кодом конкретного тега Phobos,
// golden-тест сравнивает вывод klipper-tlc с выводом phobos_tlc на mhVTL.
//
// Подпакеты (появляются на M7):
//
//	proto/      pho_proto_tlc.proto с исходным LGPL-заголовком + генерация
//	server/     кадрирование и диспетчер запросов
//	inventory/  состояние слотов и драйвов в SQLite (WAL + busy_timeout)
//	robot/      moonraker | smc | sim
//	drive/      SG_IO: TEST UNIT READY, unload, чтение MAM
package tlc
