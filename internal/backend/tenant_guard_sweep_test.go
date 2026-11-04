package backend_test

// Тест-перебор (ADR-0017, п. 5).
//
// Рефлексия по интерфейсу backend.Backend годится только чтобы получить СПИСОК
// методов: решить «под стражом метод или нет» по коду она не может. Поэтому
// здесь рефлексией берётся список, каждый метод с бакетом **реально вызывается**
// с чужой учёткой, и ожидается AccessDenied. Классификация методов — руками, в
// таблице ниже; таблица обязана покрывать интерфейс целиком, и это отдельная
// проверка: новый метод интерфейса без записи в таблице роняет тест.

import (
	"context"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/versity/versitygw/auth"
	vbackend "github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3err"

	"github.com/vladsimachkov/tapegw/internal/backend"
)

// victimBucket — бакет тестового тенанта, до которого лезут чужие.
const victimBucket = "victim"

// guardClass — чем является метод интерфейса с точки зрения стража.
type guardClass int

const (
	// guarded — метод с бакетом, реализован бэкендом и обязан отказать чужому.
	guarded guardClass = iota
	// noBucket — бакета нет (или метод не про доступ): проверять нечего.
	noBucket
	// unimplemented — заглушка BackendUnsupported. Обязана отвечать
	// NotImplemented: как только метод реализуют, эта запись перестанет
	// сходиться и тест потребует перевести его в guarded.
	unimplemented
	// noErrorResult — отказ есть, но error в сигнатуре метода нет.
	noErrorResult
)

func (c guardClass) String() string {
	switch c {
	case guarded:
		return "под стражом"
	case noBucket:
		return "без бакета"
	case unimplemented:
		return "заглушка"
	case noErrorResult:
		return "без error в результатах"
	}
	return "?"
}

// guardCase — класс метода и причина этой классификации.
type guardCase struct {
	class  guardClass
	reason string
}

// backendMethodCases — классификация КАЖДОГО метода backend.Backend. Методы
// без бакета — каждый с причиной (ADR-0017, п. 5).
var backendMethodCases = map[string]guardCase{
	// --- под стражом: реализованы и обязаны отказать чужому тенанту ---------
	"HeadBucket":                 {class: guarded},
	"GetBucketAcl":               {class: guarded},
	"PutBucketAcl":               {class: guarded},
	"DeleteBucket":               {class: guarded},
	"GetBucketVersioning":        {class: guarded},
	"PutBucketPolicy":            {class: guarded},
	"GetBucketPolicy":            {class: guarded},
	"DeleteBucketPolicy":         {class: guarded},
	"GetBucketTagging":           {class: guarded},
	"PutBucketTagging":           {class: guarded},
	"DeleteBucketTagging":        {class: guarded},
	"GetObjectLockConfiguration": {class: guarded},
	"PutObject":                  {class: guarded},
	"HeadObject":                 {class: guarded},
	"GetObject":                  {class: guarded},
	// Admin API, но страж здесь есть: versitygw не пускает не-администратора
	// раньше, а отказ обычному ключу — вторая линия на случай, если маршрут
	// когда-нибудь поедет.
	"ChangeBucketOwner": {class: guarded},

	// --- без бакета: проверять нечего, причина против каждой --------------
	"String":               {class: noBucket, reason: "константа, к доступу отношения не имеет"},
	"Shutdown":             {class: noBucket, reason: "остановка процесса, бакета нет"},
	"ListBuckets":          {class: noBucket, reason: "фильтрует по тенанту, а не проверяет один бакет"},
	"CreateBucket":         {class: noBucket, reason: "бакета ещё нет: страж назначает тенанта, проверяется отдельным тестом"},
	"ListBucketsAndOwners": {class: noBucket, reason: "admin API: все бакеты сразу, фильтра по одному бакету нет"},
	"NormalizeObjectKey":   {class: noBucket, reason: "лексическая нормализация имени, БД не трогает"},
	"StoresDotSegmentKeys": {class: noBucket, reason: "свойство реализации"},
	"ValidateUploadID":     {class: noBucket, reason: "формат uploadId, бакета в аргументах нет"},

	// --- заглушки BackendUnsupported ----------------------------------------
	"AbortMultipartUpload":          {class: unimplemented, reason: "multipart вне MVP"},
	"CompleteMultipartUpload":       {class: unimplemented, reason: "multipart вне MVP"},
	"CopyObject":                    {class: unimplemented, reason: "копирование вне MVP; при реализации — проверять оба бакета"},
	"CreateMultipartUpload":         {class: unimplemented, reason: "multipart вне MVP"},
	"DeleteBucketCors":              {class: unimplemented, reason: "CORS вне MVP"},
	"DeleteBucketOwnershipControls": {class: unimplemented, reason: "ownership controls вне MVP"},
	"DeleteBucketWebsite":           {class: unimplemented, reason: "static website вне MVP"},
	"DeleteObject":                  {class: unimplemented, reason: "удаление объектов появится с M3"},
	"DeleteObjectTagging":           {class: unimplemented, reason: "теги объектов появятся с M3"},
	"DeleteObjects":                 {class: unimplemented, reason: "пакетное удаление появится с M3"},
	"GetBucketCors":                 {class: unimplemented, reason: "CORS вне MVP"},
	"GetBucketOwnershipControls":    {class: unimplemented, reason: "ownership controls вне MVP"},
	"GetBucketWebsite":              {class: unimplemented, reason: "static website вне MVP"},
	"GetObjectAcl":                  {class: unimplemented, reason: "ACL объектов появится с M3"},
	"GetObjectAttributes":           {class: unimplemented, reason: "GetObjectAttributes вне MVP"},
	"GetObjectLegalHold":            {class: unimplemented, reason: "Object Lock вне MVP"},
	"GetObjectRetention":            {class: unimplemented, reason: "Object Lock вне MVP"},
	"GetObjectTagging":              {class: unimplemented, reason: "теги объектов появятся с M3"},
	"ListMultipartUploads":          {class: unimplemented, reason: "multipart вне MVP"},
	"ListObjectVersions":            {class: guarded, reason: "листинг версий: чужой бакет — AccessDenied"},
	"ListObjects":                   {class: guarded, reason: "листинг v1: чужой бакет — AccessDenied"},
	"ListObjectsV2":                 {class: guarded, reason: "листинг v2: чужой бакет — AccessDenied"},
	"ListParts":                     {class: unimplemented, reason: "multipart вне MVP"},
	"PutBucketCors":                 {class: unimplemented, reason: "CORS вне MVP"},
	"PutBucketOwnershipControls":    {class: unimplemented, reason: "ownership controls вне MVP"},
	"PutBucketVersioning":           {class: unimplemented, reason: "версии появятся на этапе V"},
	"PutBucketWebsite":              {class: unimplemented, reason: "static website вне MVP"},
	"PutObjectAcl":                  {class: unimplemented, reason: "ACL объектов появится с M3"},
	"PutObjectLegalHold":            {class: unimplemented, reason: "Object Lock вне MVP"},
	"PutObjectLockConfiguration":    {class: unimplemented, reason: "Object Lock вне MVP"},
	"PutObjectRetention":            {class: unimplemented, reason: "Object Lock вне MVP"},
	"PutObjectTagging":              {class: unimplemented, reason: "теги объектов появятся с M3"},
	"RestoreObject":                 {class: unimplemented, reason: "восстановление с ленты появится с M5"},
	"UploadPart":                    {class: unimplemented, reason: "multipart вне MVP"},
	"UploadPartCopy":                {class: unimplemented, reason: "multipart вне MVP; при реализации — проверять оба бакета"},
	// Отказ доставляется внутри потока ответов, error в сигнатуре нет.
	"SelectObjectContent": {class: noErrorResult, reason: "S3 Select вне MVP, а отказ возвращается потоком, а не error"},
}

// backendInterface — тип интерфейса, по которому собираем список методов.
var backendInterface = reflect.TypeOf((*vbackend.Backend)(nil)).Elem()

// TestTenantGuardTableCoversBackendInterface — таблица обязана покрывать
// интерфейс один в один. Без этой проверки перебор тихо перестал бы видеть
// новые методы versitygw: их бы просто не было в списке.
//
// Базы не требует, поэтому работает и без TAPEGW_TEST_DSN.
func TestTenantGuardTableCoversBackendInterface(t *testing.T) {
	var iface []string
	for i := 0; i < backendInterface.NumMethod(); i++ {
		iface = append(iface, backendInterface.Method(i).Name)
	}

	for _, name := range iface {
		if _, ok := backendMethodCases[name]; !ok {
			t.Errorf("метод %s интерфейса backend.Backend не разобран: "+
				"либо он под стражом (guarded), либо в списке без бакета с причиной", name)
		}
	}
	for name := range backendMethodCases {
		if !hasMethod(iface, name) {
			t.Errorf("в таблице есть %s, а в интерфейсе backend.Backend такого метода нет — запись устарела", name)
		}
	}
}

// А. Каждый метод с бакетом вызывается реально, с учёткой чужого тенанта.
func TestTenantGuardSweepForeignTenant(t *testing.T) {
	be, _, _ := newBackend(t)
	createVictim(t, be)

	sweep(t, be, "чужой тенант",
		accountCtx(auth.Account{Access: "intruder", Role: auth.RoleUser, ProjectID: testProjectID + 1}))
}

// Б. Аноним с пустой учёткой — тот же перебор. Пустой auth.Account — ровно то,
// что кладёт в контекст set-default-keys.go:33, поэтому «забыли проверить» и
// «анонимный запрос» закрываются одним условием.
func TestTenantGuardSweepAnonymous(t *testing.T) {
	be, _, _ := newBackend(t)
	createVictim(t, be)

	sweep(t, be, "аноним", accountCtx(auth.Account{}))
}

func createVictim(t *testing.T, be *backend.TapeGW) {
	t.Helper()
	if err := be.CreateBucket(ownerCtx("alice"),
		&s3.CreateBucketInput{Bucket: ptr(victimBucket)}, nil); err != nil {
		t.Fatalf("CreateBucket(%s): %v", victimBucket, err)
	}
}

func sweep(t *testing.T, be *backend.TapeGW, pass string, ctx context.Context) {
	t.Helper()

	value := reflect.ValueOf(be)
	names := make([]string, 0, backendInterface.NumMethod())
	for i := 0; i < backendInterface.NumMethod(); i++ {
		names = append(names, backendInterface.Method(i).Name)
	}
	sort.Strings(names)

	for _, name := range names {
		t.Run(pass+"/"+name, func(t *testing.T) {
			entry := backendMethodCases[name]
			if entry.class == noBucket || entry.class == noErrorResult {
				t.Skipf("%s: %s", entry.class, entry.reason)
			}

			err := callBackend(value, name, ctx)
			switch entry.class {
			case guarded:
				if !isS3Code(err, s3err.ErrAccessDenied) {
					t.Errorf("чужому тенанту разрешено: %v, ожидали AccessDenied — "+
						"метод с бакетом оказался без стража", err)
				}
			case unimplemented:
				if !isS3Code(err, s3err.ErrNotImplemented) {
					t.Errorf("метод %s больше не заглушка (получили %v). Если он реализован — "+
						"он обязан быть под стражом: переведите его в guarded, иначе чужой бакет "+
						"откроется вместе с ним", name, err)
				}
			}
		})
	}
}

// callBackend вызывает метод интерфейса с «жертвой» на месте бакета и
// возвращает error из результатов (nil, если error в сигнатуре нет).
func callBackend(value reflect.Value, name string, ctx context.Context) error {
	_, err := callBackendOut(value, name, ctx)
	return err
}

func callBackendOut(value reflect.Value, name string, ctx context.Context) ([]reflect.Value, error) {
	method, ok := backendInterface.MethodByName(name)
	if !ok {
		panic("нет метода " + name)
	}
	out := value.MethodByName(name).Call(sweepArgs(method, ctx))
	errType := reflect.TypeOf((*error)(nil)).Elem()
	for i := len(out) - 1; i >= 0; i-- {
		if out[i].Type() == errType {
			e, _ := out[i].Interface().(error)
			return out, e
		}
	}
	return out, nil
}

// sweepArgs собирает аргументы метода: контекст, имя бакета-«жертвы» на месте
// первой строки (в backend.Backend первым строковым параметром всегда идёт
// именно бакет) и безобидные значения остального.
func sweepArgs(method reflect.Method, ctx context.Context) []reflect.Value {
	typ := method.Type
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()

	args := make([]reflect.Value, 0, typ.NumIn())
	bucketPlaced := false
	for i := 0; i < typ.NumIn(); i++ {
		in := typ.In(i)
		switch {
		case in == ctxType:
			args = append(args, reflect.ValueOf(ctx))
		case in.Kind() == reflect.String && !bucketPlaced:
			bucketPlaced = true
			args = append(args, reflect.ValueOf(victimBucket).Convert(in))
		default:
			args = append(args, sweepValue(in))
		}
	}
	return args
}

// sweepValue — «безобидное» значение параметра. Смысл в том, чтобы метод успел
// дойти до стража: у PutObject, например, пустой StorageClass отвергся бы
// раньше, чем бэкенд добрался бы до бакета, и проверка превратилась бы в
// проверку заголовков.
func sweepValue(t reflect.Type) reflect.Value {
	switch t.Kind() {
	case reflect.Ptr:
		v := reflect.New(t.Elem())
		fillInput(v.Elem())
		return v
	case reflect.Struct:
		v := reflect.New(t).Elem()
		fillInput(v)
		return v
	case reflect.Slice:
		return reflect.ValueOf([]byte(`{}`)).Convert(t)
	case reflect.Map:
		return reflect.MakeMap(t)
	default:
		return reflect.Zero(t)
	}
}

// fillInput заполняет поля входной структуры так, чтобы запрос был хорошо
// сформирован: бакет, ключ, класс хранения и тело.
func fillInput(v reflect.Value) {
	for _, field := range []string{"Bucket", "Key", "Object"} {
		f := v.FieldByName(field)
		if !f.IsValid() {
			continue
		}
		name := victimBucket
		if field != "Bucket" {
			name = "k"
		}
		switch {
		case f.Kind() == reflect.Ptr && f.Type().Elem().Kind() == reflect.String:
			p := reflect.New(f.Type().Elem())
			p.Elem().SetString(name)
			f.Set(p)
		case f.Kind() == reflect.String:
			f.SetString(name)
		}
	}
	// Класс хранения: пустой versitygw отвергает до обращения к бакету.
	if f := v.FieldByName("StorageClass"); f.IsValid() && f.Kind() == reflect.String {
		f.SetString("GLACIER")
	}
	// Тело: PutObject читает его, nil упал бы раньше отказа.
	if f := v.FieldByName("Body"); f.IsValid() && f.Type() == reflect.TypeOf((*io.Reader)(nil)).Elem() {
		f.Set(reflect.ValueOf(strings.NewReader("x")))
	}
}

func hasMethod(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}
