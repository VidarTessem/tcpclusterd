# Export/Import Bug Fix

## Problem
When running `./app export`, the exported data was always empty `{}` even though data existed in the runtime directory.

```
$ ./app export
{
  "data": {},
  "timestamps": {}
}
```

## Root Cause
The `export` and `import` commands did not initialize the cluster from the environment file (`.env`) or load data from disk before exporting/importing.

**What was happening:**
1. `./app export` is called
2. Code calls `modules.Export()` directly
3. But the cluster singleton is only initialized with empty data in `init()`
4. `.env` file is never read
5. Last runtime directory is never loaded
6. Empty data gets exported

## Solution
Modified `server.go` to properly initialize the cluster before export/import:

```go
case "export":
    // Load environment and last config before exporting
    env := modules.LoadEnvFile(".env")
    modules.InitClusterArray(env, true) // Load from last runtime
    if err := modules.Export(); err != nil {
        log.Fatalf("export failed: %v", err)
    }
    return

case "import":
    // Load environment before importing
    env := modules.LoadEnvFile(".env")
    modules.InitClusterArray(env, false)
    clearFirst := false
    if len(os.Args) > 2 && os.Args[2] == "--clear" {
        clearFirst = true
    }
    if err := modules.Import(clearFirst); err != nil {
        log.Fatalf("import failed: %v", err)
    }
    return
```

## Changes Made

1. **Made `loadEnv()` public** → renamed to `LoadEnvFile()` in `modules/http.go`
2. **Export command**: Now loads `.env`, initializes cluster with last runtime data, then exports
3. **Import command**: Now loads `.env` before importing

## Export Workflow (Fixed)

```
./app export
    ↓
Load .env file
    ↓
Initialize cluster (loads from last runtime directory if exists)
    ↓
Export memory data to stdout
```

## Import Workflow (Fixed)

```
./app import < data.json
    ↓
Load .env file
    ↓
Initialize cluster
    ↓
Read JSON from stdin
    ↓
Merge/replace data in cluster
```

## Files Modified

- `server.go` - Added env loading and cluster initialization before export/import
- `modules/http.go` - Made `loadEnv()` public as `LoadEnvFile()`

## Testing

```bash
# Export data (will now include data from last runtime directory)
./app export > backup.json

# Import data
./app import < backup.json

# Import and clear first
./app import --clear < backup.json
```

## Notes

- `export` uses `InitClusterArray(env, true)` - loads from last runtime
- `import` uses `InitClusterArray(env, false)` - doesn't load (will be overwritten anyway)
- Both now respect `.env` configuration (encryption keys, peer settings, etc.)
