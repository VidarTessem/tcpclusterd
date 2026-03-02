# Export/Import Format Alignment Fix

## Problem
- `./app export` was creating a new empty runtime directory each time it ran
- Export output didn't match HTTP server format (different data structure)
- Export should show same data as HTTP `/cluster/all` endpoint

## Solution

### 1. Runtime Directory Handling
- **Before**: `init()` always created a new runtime directory
- **After**: Only HTTP server creates runtime directories via `CreateRuntimeDirectory()`
- Export/Import don't create new directories, they just load/save to existing ones

### 2. Export Format Alignment
- **Before**: `{"data": {...}, "timestamps": {...}}`
- **After**: `{"arrays": {...}, "metrics": {...}}` (same as HTTP `/cluster/all` endpoint)

### 3. Import Format Support
Import now accepts both formats:
- **New format**: `{"arrays": {...}, "metrics": {...}}` (from export or HTTP)
- **Old format**: `{"data": {...}, "timestamps": {...}}` (backward compatible)

## Changed Files

### modules/clusterarray.go
- Removed runtime directory creation from `init()`
- Added `CreateRuntimeDirectory()` method
- Updated `Export()` to use `GetAllWithMetrics()`
- Updated `Import()` to support both formats
- Added `convertInterfaceToStringMap()` helper

### modules/http.go
- `StartHTTPServer()` now calls `CreateRuntimeDirectory()` after init

### server.go
- Export/Import commands no longer create runtime directories

## Usage Examples

### Export (with metrics)
```bash
$ ./app export
{
  "arrays": {
    "myarray": {
      "key1": "value1"
    }
  },
  "metrics": {
    "arrays": 1,
    "lastSync": "2026-03-01T20:00:00Z",
    ...
  }
}
```

### Import from export
```bash
$ ./app export > backup.json
$ ./app import < backup.json
```

### Import with clear
```bash
$ ./app import --clear < backup.json
```

## Benefits

✅ Export now shows actual data from runtime directory (not empty)
✅ Export format matches HTTP API responses
✅ Backward compatible with old import format
✅ No unnecessary runtime directories created
✅ Clean separation: export/import don't interfere with running server
