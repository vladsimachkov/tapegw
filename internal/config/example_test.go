package config_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/vladsimachkov/tapegw/internal/config"
)

// Конфигурация декодируется в строгом режиме (KnownFields), поэтому пример,
// в котором есть поле, отсутствующее в структуре, — это не «документация чуть
// впереди кода», а отказ запуска у любого, кто этот пример скопирует.
//
// Тест держит примеры документов в согласии с кодом. Без него drift
// обнаруживается только на первом же «запусти-ка я пример из доков».

// exampleBlocks вытаскивает из markdown все fenced-блоки ```yaml.
var yamlBlock = regexp.MustCompile("(?s)```yaml\n(.*?)```")

func TestDeployExampleLoads(t *testing.T) {
	path, err := filepath.Abs(filepath.Join("..", "..", "deploy", "tapegw.yaml.example"))
	if err != nil {
		t.Fatal(err)
	}
	loadExample(t, path)
}

func TestArchitectureExampleLoads(t *testing.T) {
	doc, err := filepath.Abs(filepath.Join("..", "..", "docs", "ARCHITECTURE.md"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(doc)
	if err != nil {
		t.Fatalf("читать %s: %v", doc, err)
	}

	// В документе несколько yaml-блоков; конфигурационный узнаём по обоим
	// обязательным разделам.
	var found int
	for _, m := range yamlBlock.FindAllStringSubmatch(string(raw), -1) {
		block := m[1]
		if !strings.Contains(block, "s3:") || !strings.Contains(block, "migrator:") {
			continue
		}
		found++
		path := filepath.Join(t.TempDir(), "arch.yaml")
		if err := os.WriteFile(path, []byte(block), 0o600); err != nil {
			t.Fatal(err)
		}
		loadExample(t, path)
	}
	if found == 0 {
		t.Errorf("в ARCHITECTURE.md не найден блок конфигурации — тест пора чинить")
	}
}

// loadExample прогоняет файл через настоящий загрузчик. Root-креды в примерах
// намеренно пустые (их задают переменными в systemd-юните), поэтому подставляем
// их через окружение — тот же путь, что в бою.
func loadExample(t *testing.T, path string) {
	t.Helper()

	t.Setenv("TAPEGW_ROOT_ACCESS", "example-access")
	t.Setenv("TAPEGW_ROOT_SECRET", "example-secret")

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("пример %s не загружается: %v", filepath.Base(path), err)
	}
	// Осмысленность цепочки тоже проверяем на примере: max_object_size не больше
	// размера батча, иначе объект крупнее батча не архивируется никогда.
	if cfg.S3.MaxObjectSize > cfg.Migrator.BatchMaxBytes && cfg.Migrator.BatchMaxBytes > 0 {
		t.Errorf("в примере max_object_size (%v) больше batch_max_bytes (%v)",
			cfg.S3.MaxObjectSize, cfg.Migrator.BatchMaxBytes)
	}
}
