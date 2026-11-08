// Package phobosctl — вызовы CLI Phobos для регистрации носителей (Ф1.5).
//
// В libphobos_store API добавления носителя нет в принципе (спайк M0-02:
// экспорт — put/get/getmd/delete и список объектов), поэтому «добавление ленты
// одной операцией tapegw» может выполнить свою Phobos-половину только CLI.
// Тот же приём уже работает в проде: store/phobos читает extents через
// `phobos extent list` (ADR-0008).
//
// Все вызовы «закрыты при сомнении» (правило ревью Ф1.5): ненулевой код
// возврата, не разбираемый вывод или носитель, не подтверждённый повторным
// списком, — ошибка, а не предупреждение. Половинчатый успех здесь означает
// ровно тот дефект, из-за которого Ф1.5 затевалась: «в Phobos есть, у нас нет»
// (или наоборот).
package phobosctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// timeout — разовые административные вызовы; тот же порядок, что у
// `extent list` в store/phobos (30 с), с запасом на format: форматирование
// ленты — минутная операция, но и она обязана когда-то заканчиваться.
// Число не из конфига: это ограничение одного вызова CLI, а не настройка
// развертывания (правило 9 про таймауты очередей и батчей).
const timeout = 5 * time.Minute

// Client дёргает `phobos <family> ...` от имени оператора. Family/Library
// берутся из конфига tapegw (phobos.family, phobos.library), Bin и Timeout
// существуют для тестов.
type Client struct {
	Family  string // "dir" | "tape"
	Library string
	Bin     string        // пусто — "phobos"
	Timeout time.Duration // 0 — const timeout
}

func (c *Client) bin() string {
	if c.Bin == "" {
		return "phobos"
	}
	return c.Bin
}

func (c *Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return timeout
}

func (c *Client) validate() error {
	switch c.Family {
	case "dir", "tape":
	default:
		return fmt.Errorf("phobosctl: family %q, ожидаются dir или tape", c.Family)
	}
	if c.Library == "" {
		return fmt.Errorf("phobosctl: библиотека не задана")
	}
	return nil
}

// Medium — строка `phobos <family> list`, только те поля, на которые опирается
// регистрация. Названия полей JSON — как в выводе CLI (fs.status и т.п.).
type Medium struct {
	Name      string   `json:"name"`
	Tags      []string `json:"tags"`
	AdmStatus string   `json:"adm_status"`
	PutAccess bool     `json:"put_access"`
	FSStatus  string   `json:"fs.status"`
	FreeBytes int64    `json:"stats.phys_spc_free"`
	UsedBytes int64    `json:"stats.phys_spc_used"`
}

const listFields = "name,tags,adm_status,put_access,fs.status,stats.phys_spc_free,stats.phys_spc_used"

// List возвращает состояние носителя по метке. false без ошибки означает
// «Phobos ответил, носителя нет» — это ответ, а не сомнение.
func (c *Client) List(ctx context.Context, label string) (*Medium, bool, error) {
	if err := c.validate(); err != nil {
		return nil, false, err
	}
	out, err := c.run(ctx, "list", "-f", "json", "-o", listFields)
	if err != nil {
		return nil, false, err
	}
	var medias []Medium
	if err := json.Unmarshal(out, &medias); err != nil {
		// Неразбираемый вывод — сомнение, а не «носителя нет»: молча
		// решить «добавим» и получить дубль или пропустить чужую ленту
		// нельзя.
		return nil, false, fmt.Errorf("phobosctl: список носителей не разобран: %w", err)
	}
	for i := range medias {
		if medias[i].Name == label {
			return &medias[i], true, nil
		}
	}
	return nil, false, nil
}

// Add регистрирует носитель в Phobos (сразу разблокированным, с тегами).
// mediumType обязателен для tape (`-t`, технология) и игнорируется для dir.
// Повторное добавление существующего ресурса Phobos отвергает (rc=17,
// «File exists») — идемпотентность обеспечивает вызывающий проверкой List.
func (c *Client) Add(ctx context.Context, label string, tags []string, mediumType string) error {
	if err := c.validate(); err != nil {
		return err
	}
	args := []string{c.Family, "add", "--unlock", "-L", c.Library}
	if c.Family == "tape" {
		if mediumType == "" {
			return fmt.Errorf("phobosctl: для tape нужен тип носителя (-t)")
		}
		args = append(args, "-t", mediumType, "--fs", "LTFS")
	}
	if len(tags) > 0 {
		args = append(args, "-T", strings.Join(tags, ","))
	}
	args = append(args, label)
	_, err := c.runArgs(ctx, args)
	return err
}

// UpdateTags перезаписывает теги носителя: `update -T` заменяет список целиком
// (проверено на стенде), поэтому вызывающий обязан передать полный набор.
func (c *Client) UpdateTags(ctx context.Context, label string, tags []string) error {
	if err := c.validate(); err != nil {
		return err
	}
	_, err := c.runArgs(ctx, []string{c.Family, "update", "-L", c.Library,
		"-T", strings.Join(tags, ","), label})
	return err
}

// Format форматирует носитель и разблокирует его. Для dir это `--fs POSIX`,
// для tape — LTFS; порядок add → (phobosd) → format зафиксирован в
// docs/ops/tape-library.md: format без демона даёт «Transport endpoint...».
func (c *Client) Format(ctx context.Context, label string) error {
	if err := c.validate(); err != nil {
		return err
	}
	fs := "LTFS"
	if c.Family == "dir" {
		fs = "POSIX"
	}
	_, err := c.runArgs(ctx, []string{c.Family, "format", "--unlock", "--fs", fs, label})
	return err
}

// Unlock снимает административный замок.
func (c *Client) Unlock(ctx context.Context, label string) error {
	if err := c.validate(); err != nil {
		return err
	}
	_, err := c.runArgs(ctx, []string{c.Family, "unlock", label})
	return err
}

// ExtentsMedia — метки носителей, на которых лежит объект (для dir — путь
// каталога). Тот же CLI-путь, что у store/phobos: частичного чтения нет, но
// метку нужно знать, чтобы довести висящие копии 'writing' после регистрации.
func (c *Client) ExtentsMedia(ctx context.Context, oid string) ([]string, error) {
	ctxT, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	cmd := exec.CommandContext(ctxT, c.bin(), "extent", "list", "-f", "json",
		"-o", "oid,media_name", oid)
	out, err := cmd.Output()
	if err != nil {
		return nil, execError("extent list", err)
	}
	var rows []struct {
		Oid       string   `json:"oid"`
		MediaName []string `json:"media_name"`
	}
	if err := json.Unmarshal(out, &rows); err != nil {
		return nil, fmt.Errorf("phobosctl: список extents не разобран: %w", err)
	}
	var medias []string
	for _, r := range rows {
		medias = append(medias, r.MediaName...)
	}
	return medias, nil
}

func (c *Client) run(ctx context.Context, verbArgs ...string) ([]byte, error) {
	return c.runArgs(ctx, append([]string{c.Family}, verbArgs...))
}

func (c *Client) runArgs(ctx context.Context, args []string) ([]byte, error) {
	ctxT, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	cmd := exec.CommandContext(ctxT, c.bin(), args...)
	out, err := cmd.Output()
	if err != nil {
		return nil, execError(strings.Join(args, " "), err)
	}
	return out, nil
}

// execError — ошибка с хвостом stderr: Phobos пишет причину в лог-строку
// (`<ERROR> Failed to add media (File exists)`), без неё отказ неотличим
// от таймаута или отсутствующего бинаря.
func execError(what string, err error) error {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		tail := strings.TrimSpace(string(ee.Stderr))
		if len(tail) > 400 {
			tail = tail[len(tail)-400:]
		}
		return fmt.Errorf("phobosctl: %s: %w: %s", what, err, tail)
	}
	return fmt.Errorf("phobosctl: %s: %w", what, err)
}
