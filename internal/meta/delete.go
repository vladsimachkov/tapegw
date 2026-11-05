package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrObjectLocked — версию нельзя удалить: на ней legal hold или действующий
// retention. В S3 это AccessDenied, а не NoSuchKey: версия существует и клиент
// вправе знать, что она защищена.
var ErrObjectLocked = errors.New("meta: object version is locked")

// DeleteObjectInput — удаление объекта или конкретной версии.
type DeleteObjectInput struct {
	Bucket string
	Key    string
	// VersionID — "" означает «без versionId»: семантика зависит от состояния
	// версионирования бакета. "null" — null-версия.
	VersionID string
	// BypassGovernance — запрос на обход GOVERNANCE. COMPLIANCE не обходится
	// никогда, legal hold — тоже.
	BypassGovernance bool
}

// DeleteObjectResult — что произошло. По этим полям бэкенд ставит заголовки
// x-amz-delete-marker и x-amz-version-id.
type DeleteObjectResult struct {
	// IsDeleteMarker — создан маркер удаления (удаление без versionId в
	// бакете с версиями) либо удалён маркер удаления.
	IsDeleteMarker bool
	// VersionID — версия, которой закончилась операция: id созданного маркера
	// или id удалённой версии. "null" — для null-версии.
	VersionID string
}

// DeleteObject удаляет объект или конкретную версию.
//
// Три вещи, которые здесь легко сделать не так:
//
//  1. Удаление без versionId в бакете с включёнными версиями НЕ трогает данные:
//     создаётся версия-маркер, а прежняя текущая версия просто перестаёт быть
//     текущей. Иначе «удалить» означало бы уничтожить историю, ради которой
//     версионирование и включали.
//
//  2. Версия под legal hold или действующим retention не удаляется. GOVERNANCE
//     можно обойти явным запросом, COMPLIANCE и legal hold — никогда.
//
//  3. Если удалили текущую версию по versionId, текущей становится версия с
//     максимальным version_seq из оставшихся, а не любая. Иначе «текущая»
//     уехала бы на произвольную, и листинг разошёлся бы с GET.
//
// Порядок локаков — как у PutObject (ADR-0005): сначала advisory-лок ключа,
// потом бакет FOR SHARE. Тот же порядок обязателен: удаление и перезапись одного
// ключа обязаны сериализоваться одним и тем же локом.
func (m *Meta) DeleteObject(ctx context.Context, tenant TenantID, in DeleteObjectInput) (*DeleteObjectResult, error) {
	out := &DeleteObjectResult{}

	err := m.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			in.Bucket+"/"+in.Key); err != nil {
			return fmt.Errorf("advisory lock %s/%s: %w", in.Bucket, in.Key, err)
		}

		var bucketID int64
		var versioning string
		bucketQ, bucketArgs := withTenant(
			`SELECT id, versioning FROM buckets
			  WHERE name = $1 AND deleted_at IS NULL`, tenant, in.Bucket)
		err := tx.QueryRow(ctx, bucketQ+` FOR SHARE`, bucketArgs...).
			Scan(&bucketID, &versioning)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("bucket %s for delete: %w", in.Bucket, err)
		}

		if in.VersionID == "" {
			return m.deleteCurrent(ctx, tx, bucketID, in, versioning, out)
		}
		return m.deleteVersion(ctx, tx, bucketID, in, out)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// deleteCurrent — удаление без versionId.
func (m *Meta) deleteCurrent(ctx context.Context, tx pgx.Tx, bucketID int64,
	in DeleteObjectInput, versioning string, out *DeleteObjectResult) error {

	switch versioning {
	case VersioningUnversioned:
		// Версий нет, маркера тоже: объект удаляется целиком. Строка уходит в
		// deleting — deleter снимет копию с диска и с ленты.
		//
		// markDeleting, а не голый UPDATE: если версия лежит в пачке, её байты
		// снимаются с живого остатка пачки этим же запросом.
		n, err := markDeleting(ctx, tx, "",
			"bucket_id = $1 AND key = $2 AND is_current", bucketID, in.Key)
		if err != nil {
			return fmt.Errorf("delete object %s/%s: %w", in.Bucket, in.Key, err)
		}
		if n == 0 {
			return ErrNotFound
		}
		out.VersionID = "null"
		return nil

	case VersioningSuspended:
		// В приостановленном бакете AWS создаёт маркер удаления с
		// versionId = "null" и заменяет им прежнюю null-версию. Обычную версию,
		// записанную пока версии были включены, не трогает — её клиент
		// сохранил ради истории.
		if _, err := tx.Exec(ctx,
			`UPDATE objects SET is_current = false
			  WHERE bucket_id = $1 AND key = $2 AND is_current AND NOT is_null_version`,
			bucketID, in.Key); err != nil {
			return fmt.Errorf("retire current version of %s/%s: %w", in.Bucket, in.Key, err)
		}
		if _, err := markDeleting(ctx, tx, ", is_null_version = false",
			"bucket_id = $1 AND key = $2 AND is_null_version", bucketID, in.Key); err != nil {
			return fmt.Errorf("replace null version of %s/%s: %w", in.Bucket, in.Key, err)
		}
		id, err := insertDeleteMarker(ctx, tx, bucketID, in, true)
		if err != nil {
			return err
		}
		out.IsDeleteMarker = true
		out.VersionID = "null"
		_ = id
		return nil

	default: // VersioningEnabled
		if _, err := tx.Exec(ctx,
			`UPDATE objects SET is_current = false
			  WHERE bucket_id = $1 AND key = $2 AND is_current`, bucketID, in.Key); err != nil {
			return fmt.Errorf("retire current version of %s/%s: %w", in.Bucket, in.Key, err)
		}
		id, err := insertDeleteMarker(ctx, tx, bucketID, in, false)
		if err != nil {
			return err
		}
		out.IsDeleteMarker = true
		out.VersionID = id
		return nil
	}
}

// insertDeleteMarker — версия без тела: state = 'marker', retired_at = NULL.
//
// Не 'deleted' по двум причинам. Во-первых, фильтры вида
// `state NOT IN ('deleting','deleted')` должны видеть маркер как живую версию:
// иначе удаление текущей версии воскрешает объект, спрятанный маркером.
// Во-вторых, janitor (M6) физически удаляет строки в 'deleted' старше
// meta.terminal_retention, и маркеры исчезли бы вместе с защитой от
// «воскрешения». В 'deleting' маркеру делать нечего — deleter искал бы то,
// чего не существует.
func insertDeleteMarker(ctx context.Context, tx pgx.Tx, bucketID int64,
	in DeleteObjectInput, nullVersion bool) (string, error) {

	id := uuid.New()
	// pool_id у маркера — формальность: колонка NOT NULL без DEFAULT (миграция
	// 0005), а данных у маркера нет и на ленту он не поедет. Тенант при этом
	// обязателен: маркер — строка бакета, и чужому тенанту он виден быть не
	// должен.
	_, err := tx.Exec(ctx,
		`INSERT INTO objects (id, bucket_id, key, size, etag, storage_class,
		                      state, landing, online_until, tenant_id, pool_id,
		                      is_current, is_delete_marker, is_null_version)
		  VALUES ($1,$2,$3,0,'','GLACIER','marker',false,now(),
		          (SELECT tenant_id FROM buckets WHERE id = $2), $5,
		          true, true, $4)`,
		id, bucketID, in.Key, nullVersion, DefaultPoolID)
	if err != nil {
		return "", fmt.Errorf("insert delete marker %s/%s: %w", in.Bucket, in.Key, err)
	}
	return id.String(), nil
}

// deleteVersion — удаление конкретной версии.
func (m *Meta) deleteVersion(ctx context.Context, tx pgx.Tx, bucketID int64,
	in DeleteObjectInput, out *DeleteObjectResult) error {

	var (
		verID         uuid.UUID
		isCurrent     bool
		isMarker      bool
		isNull        bool
		state         string
		legalHold     bool
		retentionMode *string
		retainUntil   *time.Time
	)
	q := `SELECT id, is_current, is_delete_marker, is_null_version, state,
		         legal_hold, retention_mode, retain_until
		    FROM objects
		   WHERE bucket_id = $1 AND key = $2 AND `
	args := []any{bucketID, in.Key}
	if in.VersionID == "null" {
		isNull = true
		q += `is_null_version`
	} else {
		id, err := uuid.Parse(in.VersionID)
		if err != nil {
			return ErrNotFound
		}
		verID = id
		args = append(args, id)
		q += `id = $3::uuid`
	}

	err := tx.QueryRow(ctx, q+` FOR UPDATE`, args...).
		Scan(&verID, &isCurrent, &isMarker, &isNull, &state,
			&legalHold, &retentionMode, &retainUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("find version %s/%s@%s: %w", in.Bucket, in.Key, in.VersionID, err)
	}
	if state == "deleting" || state == "deleted" {
		// Уже удаляется: повторное удаление той же версии — не «успех» и не
		// «нет такой», а та же гонка, что с удалением файла дважды.
		return ErrNotFound
	}

	// Object Lock. legal hold не обходит никто и никогда; GOVERNANCE обходится
	// явным запросом, COMPLIANCE — нет.
	locked := legalHold || (retainUntil != nil && retainUntil.After(time.Now()))
	if locked {
		bypassable := retentionMode != nil && *retentionMode == "GOVERNANCE" &&
			!legalHold && in.BypassGovernance
		if !bypassable {
			return ErrObjectLocked
		}
	}

	// Маркер удаления по versionId — стандартный способ вернуть удалённый
	// объект в S3: удаляешь маркер, и предыдущая версия снова становится
	// текущей. Строка маркера исчезает физически.
	//
	// Перевод в 'deleted' не годится дважды. Во-первых, он нарушает
	// CHECK (is_delete_marker = (state = 'marker')) — то есть клиент получил бы
	// 500 на штатную операцию. Во-вторых, «удалённая версия нулевого размера»
	// — сущность, которой никогда не существовало: данных у маркера нет ни на
	// диске, ни на ленте, deleter ему не нужен. Для аудита хватит журнала (T4).
	//
	// Если на T3 появится квота на число объектов, здесь же нужно уменьшать
	// счётчик: по ADR-0015 маркеры в квоту входят.
	if isMarker {
		if err := deleteMarkerRow(ctx, tx, verID); err != nil {
			return fmt.Errorf("delete marker %s/%s@%s: %w", in.Bucket, in.Key, in.VersionID, err)
		}
	} else if _, err := markDeleting(ctx, tx, ", retired_at = NULL",
		"id = $1", verID); err != nil {
		return fmt.Errorf("delete version %s/%s@%s: %w", in.Bucket, in.Key, in.VersionID, err)
	}

	out.IsDeleteMarker = isMarker
	if isNull {
		out.VersionID = "null"
	} else {
		out.VersionID = verID.String()
	}

	if !isCurrent {
		// Удаляли не текущую версию: текущая осталась, ничего назначать не надо.
		return nil
	}

	// Текущей становится оставшаяся версия с максимальным version_seq — та же
	// логика, на которой держится листинг версий. Строки в deleting/deleted не
	// кандидаты: они уже удаляются. Ноль затронутых строк — версий больше нет,
	// ключа нет, и это нормальный исход, а не ошибка.
	if _, err := tx.Exec(ctx,
		`UPDATE objects SET is_current = true
		  WHERE id = (SELECT id FROM objects
		               WHERE bucket_id = $1 AND key = $2
		                 AND state NOT IN ('deleting','deleted')
		               ORDER BY version_seq DESC LIMIT 1)`,
		bucketID, in.Key); err != nil {
		return fmt.Errorf("promote next version of %s/%s: %w", in.Bucket, in.Key, err)
	}
	return nil
}

// ErrNoSuchVersion — запрошенной версии нет. Отличается от ErrNotFound: «нет
// бакета/ключа» и «нет конкретной версии» — разные ответы клиента, и AWS
// различает их кодами (NoSuchBucket / NoSuchKey против NoSuchVersion).
var ErrNoSuchVersion = errors.New("meta: no such object version")

// LookupVersion ищет конкретную версию по id либо по "null".
//
// Пустой versionId здесь не обрабатывается: «текущая версия» — это GetObject,
// и смешивать их в одной функции значит однажды отдать текущую вместо
// запрошенной.
func (m *Meta) LookupVersion(ctx context.Context, tenant TenantID,
	bucket, key, versionID string) (*Object, error) {

	var cond string
	args := []any{bucket, key}
	if versionID == "null" {
		cond = `o.is_null_version`
	} else {
		id, err := uuid.Parse(versionID)
		if err != nil {
			return nil, ErrNoSuchVersion
		}
		args = append(args, id)
		cond = fmt.Sprintf(`o.id = $%d::uuid`, len(args))
	}

	base := fmt.Sprintf("SELECT "+objectCols("o")+`
			           FROM objects o
			           JOIN buckets b ON b.id = o.bucket_id
			          WHERE b.name = $1 AND b.deleted_at IS NULL
			            AND o.key = $2 AND %s
			            AND o.state NOT IN ('deleting','deleted')`, cond)
	q, qargs := withTenantCol(base, "b.tenant_id", tenant, args...)

	obj, err := scanObject(m.Pool.QueryRow(ctx, q, qargs...))
	if errors.Is(err, ErrNotFound) {
		return nil, ErrNoSuchVersion
	}
	return obj, err
}
