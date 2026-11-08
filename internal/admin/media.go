// Package admin — операторские операции `tapegw admin ...`.
//
// Ф1.5: регистрация носителя одной командой. До неё это были четыре ручные
// команды Phobos (add, теги, unlock, format) плюс SQL-вставка в media, и
// пропущенный шаг давал ленту без тегов или без строки реестра — копия на
// такой ленте навсегда оставалась в 'writing' (docs/ops/tape-library.md,
// «Заведение носителя»).
//
// Порядок половин операции: сначала Phobos, потом наш реестр (то же правило,
// что «сначала файл на диске, потом commit» — правило 6). Полусостояние
// «в Phobos есть, у нас нет» переживаемо и доводимо: копия ждёт в 'writing',
// повтор команды идемпотентен и регистрирует до конца. Обратное полусостояние
// — «в реестре есть, в Phobos нет» — обещало бы планировщику место, которого
// не существует, поэтому вставка в media идёт только после того, как Phobos
// подтвердил носителя списком.
package admin

import (
	"context"
	"fmt"
	"strings"

	"github.com/vladsimachkov/tapegw/internal/meta"
	"github.com/vladsimachkov/tapegw/internal/phobosctl"
)

// MediaAddParams — параметры команды `tapegw admin media add`.
type MediaAddParams struct {
	Label      string
	Generation string
	MediumType string // для tape — технология (-t), для dir — имя колонки реестра
	Side       string // "" — сторона не задана
	PoolID     int64
}

// MediaDeps — зависимости операции; разделяются на БД и Phobos, чтобы тест
// подменял CLI скриптом-обманкой.
type MediaDeps struct {
	Meta   *meta.Meta
	Phobos *phobosctl.Client
}

// MediaAddResult — итог: id строки реестра и доводка висящих копий.
type MediaAddResult struct {
	MediaID int64
	// BoundCopies — копии 'writing', привязанные к этому носителю этим заходом.
	BoundCopies int
	// BindErrors — носитель зарегистрирован, но часть висящих копий не удалось
	// проверить. Это не откат (регистрация состоялась), но и не успех: команду
	// надо повторить — она идемпотентна.
	BindErrors []error
}

func MediaAdd(ctx context.Context, d MediaDeps, p MediaAddParams) (MediaAddResult, error) {
	var res MediaAddResult
	if p.Label == "" || p.Generation == "" {
		return res, fmt.Errorf("admin: media add: label и generation обязательны")
	}
	if p.MediumType == "" && d.Phobos.Family == "tape" {
		return res, fmt.Errorf("admin: media add: для tape нужен medium-type (технология, -t в phobos tape add)")
	}
	poolTag, err := d.Meta.PoolTag(ctx, p.PoolID)
	if err != nil {
		return res, err
	}

	// Конфликт метки в нашем реестре отсекаем до Phobos: перетегировать чужой
	// носитель «заодно» — тот же грех, что писать на ленту другого пула.
	if row, exists, err := d.Meta.MediaByLabel(ctx, p.Label); err != nil {
		return res, err
	} else if exists {
		if err := sameRegistration(row, p); err != nil {
			return res, err
		}
	}

	// --- половина Phobos ---
	med, found, err := d.Phobos.List(ctx, p.Label)
	if err != nil {
		return res, fmt.Errorf("admin: состояние носителя в Phobos не получено: %w", err)
	}
	if !found {
		tags, err := planTags(nil, poolTag, p)
		if err != nil {
			return res, err
		}
		if err := d.Phobos.Add(ctx, p.Label, tags, p.MediumType); err != nil {
			return res, fmt.Errorf("admin: регистрация в Phobos не прошла: %w", err)
		}
		med, found, err = d.Phobos.List(ctx, p.Label)
		if err != nil {
			return res, fmt.Errorf("admin: после add Phobos не ответил списком: %w", err)
		}
		// «закрыто при сомнении»: add вернул ноль, но носителя в списке нет —
		// подтверждение не получено, в свой реестр не пишем.
		if !found {
			return res, fmt.Errorf("admin: Phobos не подтвердил добавление: %s нет в списке после add", p.Label)
		}
	} else {
		tags, err := planTags(med.Tags, poolTag, p)
		if err != nil {
			return res, err
		}
		if !sameTagSet(med.Tags, tags) {
			if err := d.Phobos.UpdateTags(ctx, p.Label, tags); err != nil {
				return res, fmt.Errorf("admin: теги носителя не установлены: %w", err)
			}
		}
	}
	if med.AdmStatus != "unlocked" {
		if err := d.Phobos.Unlock(ctx, p.Label); err != nil {
			return res, fmt.Errorf("admin: носитель не разблокирован: %w", err)
		}
		med, found, err = d.Phobos.List(ctx, p.Label)
		if err != nil || !found {
			return res, fmt.Errorf("admin: после unlock Phobos не подтвердил носитель")
		}
	}
	if med.FSStatus == "" || med.FSStatus == "blank" {
		if err := d.Phobos.Format(ctx, p.Label); err != nil {
			return res, fmt.Errorf("admin: форматирование не прошло: %w", err)
		}
		med, found, err = d.Phobos.List(ctx, p.Label)
		if err != nil || !found {
			return res, fmt.Errorf("admin: после format Phobos не подтвердил носитель")
		}
	}
	// Финальная проверка перед записью в свой реестр: видим, разблокирован,
	// пишет, отформатирован, есть место. Любое «нет» — ошибка, а не строка
	// реестра «на авось»: лента без ёмкости дала бы ENOSPC на первой записи.
	if med.AdmStatus != "unlocked" || !med.PutAccess {
		return res, fmt.Errorf("admin: Phobos не разрешает запись на %s (adm_status=%s, put_access=%v)",
			p.Label, med.AdmStatus, med.PutAccess)
	}
	if med.FSStatus == "" || med.FSStatus == "blank" {
		return res, fmt.Errorf("admin: Phobos не подтвердил форматирование %s (fs.status=%q)", p.Label, med.FSStatus)
	}
	if med.FreeBytes <= 0 {
		return res, fmt.Errorf("admin: Phobos сообщает нулевое свободное место на %s — регистрацию проводить нельзя", p.Label)
	}

	// --- половина реестра ---
	id, err := d.Meta.RegisterMedia(ctx, meta.MediaRegistration{
		Label:         p.Label,
		Generation:    p.Generation,
		MediumType:    p.MediumType,
		Side:          p.Side,
		PoolID:        p.PoolID,
		CapacityBytes: med.UsedBytes + med.FreeBytes,
		FreeBytes:     med.FreeBytes,
	})
	if err != nil {
		return res, err
	}
	res.MediaID = id

	// --- доводка копий, застрявших на этом носителе ---
	// Ровно тот случай, ради которого Ф1.5: ленту завели мимо реестра, копии
	// легли и висят в 'writing'. Метка в extents есть — привязываем.
	stuck, err := d.Meta.StuckCopies(ctx)
	if err != nil {
		res.BindErrors = append(res.BindErrors, err)
		return res, nil
	}
	for _, c := range stuck {
		medias, err := d.Phobos.ExtentsMedia(ctx, c.PhobosOID)
		if err != nil {
			res.BindErrors = append(res.BindErrors, fmt.Errorf("копия %s (%s): %w", c.ID, c.PhobosOID, err))
			continue
		}
		for _, m := range medias {
			if m != p.Label {
				continue
			}
			if err := d.Meta.BindCopyMedium(ctx, c.ID, id); err != nil {
				res.BindErrors = append(res.BindErrors, fmt.Errorf("копия %s (%s): %w", c.ID, c.PhobosOID, err))
			} else {
				res.BoundCopies++
			}
			break
		}
	}
	return res, nil
}

// sameRegistration — повтор команды с теми же параметрами допустим (идем
// доводить Phobos-половину), другие параметры на занятой метке — отказ.
func sameRegistration(row *meta.MediaRow, p MediaAddParams) error {
	side := ""
	if row.Side != nil {
		side = *row.Side
	}
	pool := int64(0)
	if row.PoolID != nil {
		pool = *row.PoolID
	}
	if row.Generation != p.Generation || row.MediumType != p.MediumType ||
		pool != p.PoolID || side != p.Side {
		return fmt.Errorf("%w: %q (в реестре %s/%s, пул %d, сторона %q)",
			meta.ErrMediaLabelTaken, p.Label, row.Generation, row.MediumType, pool, side)
	}
	return nil
}

// planTags — полный набор тегов носителя. Ось пула — ровно один тег
// (ADR-0013): чужой пул на ленте — сомнение, отказ, а не «перетегируем».
// Поколение — физическое свойство ленты: несовпадение с запрошенным тоже
// отказ. Сторона: если команда задала сторону, она обязана совпасть с
// существующей; если не задала — существующая сохраняется. Прочие теги
// (не наших трёх осей) бережём: update -T перезаписывает список целиком.
func planTags(existing []string, poolTag string, p MediaAddParams) ([]string, error) {
	desired := []string{poolTag}
	keep := make(map[string]bool)
	for _, t := range existing {
		switch {
		case strings.HasPrefix(t, "pool:"):
			if t != poolTag {
				return nil, fmt.Errorf("admin: носитель уже помечен пулом %q — переназначение пула командой запрещено", t)
			}
		case strings.HasPrefix(t, "gen:"):
			if t != "gen:"+p.Generation {
				return nil, fmt.Errorf("admin: носитель помечен поколением %q, команда просит %q — поколение физическое свойство ленты, уточните параметры", t, p.Generation)
			}
		case strings.HasPrefix(t, "side:"):
			if p.Side != "" && t != "side:"+p.Side {
				return nil, fmt.Errorf("admin: носитель помечен %q, команда просит сторону %q", t, p.Side)
			}
			keep[t] = true
		default:
			keep[t] = true
		}
	}
	desired = append(desired, "gen:"+p.Generation)
	if p.Side != "" {
		desired = append(desired, "side:"+p.Side)
	}
	for t := range keep {
		desired = append(desired, t)
	}
	return desired, nil
}

func sameTagSet(a, b []string) bool {
	setA := tagSet(a)
	setB := tagSet(b)
	if len(setA) != len(setB) {
		return false
	}
	for t := range setA {
		if !setB[t] {
			return false
		}
	}
	return true
}

func tagSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, t := range s {
		m[t] = true
	}
	return m
}
