//go:build cgo && (darwin || linux)

package antfly

/*
#cgo linux LDFLAGS: -ldl
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include "antfly.h"

typedef struct {
    void *library;
    antfly_db *root, *table;
    uint32_t (*abi)(void);
    uint32_t (*options_size)(void);
    uint32_t (*threading_mode)(void);
    antfly_error_code (*options_init)(antfly_open_options*);
    antfly_error_code (*open)(const char*, const antfly_open_options*, antfly_db**);
    antfly_error_code (*create)(const char*, const antfly_open_options*, antfly_db**);
    void (*close)(antfly_db*);
    antfly_error_code (*create_table)(antfly_db*, antfly_slice, antfly_slice);
    antfly_error_code (*open_table)(antfly_db*, antfly_slice, antfly_db**);
    antfly_error_code (*list_tables)(antfly_db*,antfly_buffer*);
    antfly_error_code (*add_index)(antfly_db*,antfly_slice);
    antfly_error_code (*timestamp)(antfly_db*, antfly_slice, uint64_t*);
    antfly_error_code (*lookup)(antfly_db*, antfly_slice, antfly_buffer*);
    antfly_error_code (*batch)(antfly_db*, const antfly_write_intent*,size_t,const antfly_version_predicate*,size_t,uint64_t,uint8_t);
    antfly_error_code (*memory)(antfly_db*,antfly_slice,antfly_slice,antfly_buffer*);
    void (*free_buffer)(antfly_buffer*);
} beads_antfly;

static int beads_load(beads_antfly *a,const char *path) {
    a->library=dlopen(path,RTLD_NOW|RTLD_LOCAL); if(!a->library) return 0;
#define LOAD(field,name) a->field=dlsym(a->library,name); if(!a->field) return 0
    LOAD(abi,"antfly_abi_version");
    LOAD(options_size,"antfly_open_options_size");
    LOAD(threading_mode,"antfly_threading_mode");
    LOAD(options_init,"antfly_open_options_init");
    LOAD(open,"antfly_db_open_with_options");
    LOAD(create,"antfly_db_create_with_options");
    LOAD(close,"antfly_db_close");
    LOAD(create_table,"antfly_db_create_table_json");
    LOAD(open_table,"antfly_db_open_table");
    LOAD(list_tables,"antfly_db_list_tables_json");
    LOAD(add_index,"antfly_db_add_index_json");
    LOAD(timestamp,"antfly_db_get_timestamp");
    LOAD(lookup,"antfly_db_lookup_json");
    LOAD(batch,"antfly_db_batch");
    LOAD(memory,"antfly_db_memory_json");
    LOAD(free_buffer,"antfly_buffer_free_zero");
#undef LOAD
    return a->abi()==ANTFLY_ABI_VERSION && a->options_size()==sizeof(antfly_open_options) && a->threading_mode()==ANTFLY_THREADING_SERIALIZED;
}
static antfly_error_code beads_open(beads_antfly *a,const char *path,int create) {
    antfly_open_options opts; antfly_error_code code=a->options_init(&opts); if(code!=ANTFLY_OK) return code;
    opts.storage_kind=ANTFLY_STORAGE_KIND_LITE; opts.busy_timeout_ms=5000;
    return create?a->create(path,&opts,&a->root):a->open(path,&opts,&a->root);
}
static antfly_error_code beads_table(beads_antfly *a,antfly_slice name) {
    return a->open_table(a->root,name,&a->table);
}
static antfly_error_code beads_list_tables(beads_antfly*a,antfly_buffer*out){return a->list_tables(a->root,out);}
static antfly_error_code beads_create_table(beads_antfly *a,antfly_slice name) {
    antfly_slice schema={(const uint8_t*)"{}",2}; return a->create_table(a->root,name,schema);
}
static antfly_error_code beads_timestamp(beads_antfly*a,antfly_slice key,uint64_t*out){return a->timestamp(a->table,key,out);}
static antfly_error_code beads_lookup(beads_antfly*a,antfly_slice key,antfly_buffer*out){return a->lookup(a->table,key,out);}
static antfly_error_code beads_cas(beads_antfly*a,antfly_slice key,uint64_t version,const antfly_write_intent*writes,size_t count,uint64_t timestamp){
    antfly_version_predicate predicate={key,version};
    return a->batch(a->table,writes,count,&predicate,1,timestamp,0);
}
static antfly_error_code beads_add_graph(beads_antfly*a){const char*cfg="{\"name\":\"beads\",\"type\":\"graph\"}";antfly_slice config={(const uint8_t*)cfg,strlen(cfg)};return a->add_index(a->table,config);}
static antfly_error_code beads_memory(beads_antfly*a,antfly_slice space,antfly_slice body,antfly_buffer*out){return a->memory(a->root,space,body,out);}
static void beads_free_buffer(beads_antfly*a,antfly_buffer*out){a->free_buffer(out);}
static void beads_close(beads_antfly*a){if(a->table)a->close(a->table);if(a->root)a->close(a->root);if(a->library)dlclose(a->library);free(a);}
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"
	"unsafe"
)

type embedded struct {
	mu     sync.Mutex
	native *C.beads_antfly
	key    string
}

func openEmbedded(t Target) (Transport, error) {
	library := t.Library
	if library == "" {
		library = os.Getenv("ANTFLY_LIBRARY_PATH")
	}
	if library == "" {
		return nil, errors.New("embedded Antfly requires --library or ANTFLY_LIBRARY_PATH pointing to libantfly")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	n := (*C.beads_antfly)(C.calloc(1, C.size_t(C.sizeof_beads_antfly)))
	if n == nil {
		return nil, errors.New("allocate Antfly native handle")
	}
	lib := C.CString(library)
	defer C.free(unsafe.Pointer(lib))
	if C.beads_load(n, lib) == 0 {
		C.beads_close(n)
		return nil, errors.New("cannot load libantfly ABI 3 and its required database symbols")
	}
	path := C.CString(t.Path)
	defer C.free(unsafe.Pointer(path))
	_, statErr := os.Stat(t.Path)
	create := C.int(0)
	if errors.Is(statErr, os.ErrNotExist) {
		create = 1
	} else if statErr != nil {
		C.beads_close(n)
		return nil, statErr
	}
	if code := C.beads_open(n, path, create); code != C.ANTFLY_OK {
		C.beads_close(n)
		return nil, nativeError(code)
	}
	name, release := nativeSlice([]byte(t.Table))
	defer release()
	var tablesOut C.antfly_buffer
	code := C.beads_list_tables(n, &tablesOut)
	if code != C.ANTFLY_OK {
		C.beads_close(n)
		return nil, nativeError(code)
	}
	var names []string
	decodeErr := json.Unmarshal(C.GoBytes(unsafe.Pointer(tablesOut.ptr), C.int(tablesOut.len)), &names)
	C.beads_free_buffer(n, &tablesOut)
	if decodeErr != nil {
		C.beads_close(n)
		return nil, decodeErr
	}
	createdTable := !slices.Contains(names, t.Table)
	if createdTable {
		code = C.beads_create_table(n, name)
		if code != C.ANTFLY_OK {
			C.beads_close(n)
			return nil, nativeError(code)
		}
	}
	code = C.beads_table(n, name)
	if code != C.ANTFLY_OK {
		C.beads_close(n)
		return nil, nativeError(code)
	}
	if createdTable {
		if code = C.beads_add_graph(n); code != C.ANTFLY_OK {
			C.beads_close(n)
			return nil, nativeError(code)
		}
	}
	return &embedded{native: n, key: t.Workspace}, nil
}

func nativeSlice(b []byte) (C.antfly_slice, func()) {
	p := C.CBytes(b)
	return C.antfly_slice{ptr: (*C.uint8_t)(p), len: C.size_t(len(b))}, func() { C.free(p) }
}
func nativeError(code C.antfly_error_code) error {
	if code == C.ANTFLY_VERSION_CONFLICT || code == C.ANTFLY_INTENT_CONFLICT {
		return errConflict
	}
	if code == C.ANTFLY_OUTCOME_UNKNOWN {
		return errIndeterminate
	}
	return fmt.Errorf("libantfly error code %d", int(code))
}
func (e *embedded) Load(ctx context.Context) ([]byte, uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if e.native == nil {
		return nil, 0, errors.New("Antfly store closed")
	}
	key, release := nativeSlice([]byte(e.key))
	defer release()
	var version C.uint64_t
	// Read the fence BEFORE the document. A concurrent commit can force a
	// harmless CAS retry but cannot pair stale data with a newer version.
	code := C.beads_timestamp(e.native, key, &version)
	if code == C.ANTFLY_NOT_FOUND {
		return nil, 0, nil
	}
	if code != C.ANTFLY_OK {
		return nil, 0, nativeError(code)
	}
	var out C.antfly_buffer
	code = C.beads_lookup(e.native, key, &out)
	if code == C.ANTFLY_NOT_FOUND {
		return nil, 0, nil
	}
	if code != C.ANTFLY_OK {
		return nil, 0, nativeError(code)
	}
	defer C.beads_free_buffer(e.native, &out)
	if out.len > maxStateBytes {
		return nil, 0, errors.New("Antfly workspace too large")
	}
	return C.GoBytes(unsafe.Pointer(out.ptr), C.int(out.len)), uint64(version), nil
}
func (e *embedded) CompareAndSwap(ctx context.Context, version uint64, data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.native == nil {
		return errors.New("Antfly store closed")
	}
	key, kfree := nativeSlice([]byte(e.key))
	defer kfree()
	docs, err := projections(e.key, data)
	if err != nil {
		return err
	}
	allocation := C.calloc(C.size_t(len(docs)), C.size_t(C.sizeof_antfly_write_intent))
	if allocation == nil {
		return errors.New("allocate Antfly write batch")
	}
	defer C.free(allocation)
	writes := unsafe.Slice((*C.antfly_write_intent)(allocation), len(docs))
	index := 0
	for name, document := range docs {
		k, kfree := nativeSlice([]byte(name))
		defer kfree()
		v, vfree := nativeSlice(document)
		defer vfree()
		writes[index] = C.antfly_write_intent{key: k, value: v}
		index++
	}
	code := C.beads_cas(e.native, key, C.uint64_t(version), (*C.antfly_write_intent)(allocation), C.size_t(len(docs)), C.uint64_t(time.Now().UnixNano()))
	if code != C.ANTFLY_OK {
		return nativeError(code)
	}
	return nil
}
func (e *embedded) Memory(ctx context.Context, space, action, key string, body json.RawMessage) (json.RawMessage, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.native == nil {
		return nil, errors.New("Antfly store closed")
	}
	b, err := json.Marshal(map[string]any{"action": action, "idempotency_key": key, "body": body})
	if err != nil {
		return nil, err
	}
	name, nfree := nativeSlice([]byte(space))
	defer nfree()
	request, rfree := nativeSlice(b)
	defer rfree()
	var out C.antfly_buffer
	code := C.beads_memory(e.native, name, request, &out)
	if code != C.ANTFLY_OK {
		return nil, nativeError(code)
	}
	defer C.beads_free_buffer(e.native, &out)
	if out.len > maxStateBytes {
		return nil, errors.New("memory response too large")
	}
	return C.GoBytes(unsafe.Pointer(out.ptr), C.int(out.len)), nil
}
func (e *embedded) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.native != nil {
		C.beads_close(e.native)
		e.native = nil
	}
	return nil
}
