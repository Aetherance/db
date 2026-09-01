# db

An embedded LSM-tree based key-value storage engine implemented in C++, inspired by LevelDB.

## [Roadmap](ROADMAP.md)

## Architecture

```
Write → active WAL + MemTable
                     ↓ freeze when full
        previous WAL + immutable MemTable
                     ↓ Env::Schedule
             Level-0 SSTable → compaction → Level-1..6
Read → MemTable → immutable MemTable → current Version's SSTables
```

`DBImpl` keeps one active and at most one immutable MemTable. A full active MemTable switches to a
new WAL, and the immutable MemTable is flushed in the background. Writers wait if both MemTables
are full. Reads and iterators merge both MemTables with the current Version. Flush-triggered
compaction runs with the background job; manual compaction waits for that job and runs on the
calling thread.

An open `DBImpl` holds the database's `LOCK` file until it closes, so another open or `DestroyDB`
on the same path fails instead of modifying the database concurrently. Calls on one DB instance
still follow the public API's single-threaded contract; writer grouping is not implemented.

## Build

```bash
make build
```

## Test

```bash
make test
```

## Benchmark

The `db_bench` target is a Strata port of LevelDB's database benchmark. If LevelDB is checked
out at `../leveldb`, the following command builds both Release binaries and runs the same
sequential-write, random-write, sequential-read, and random-read workload against LevelDB first
and Strata second:

```bash
make benchmark
```

Override `BENCHMARK_ARGS`, `LEVELDB_DIR`, `LEVELDB_BUILD_DIR`, `LEVELDB_BENCHMARK_DB`, or
`STRATA_BENCHMARK_DB` on the `make` command line when needed. Write benchmarks recreate their
separate database directories. Use paths on the same real filesystem instead of `/tmp` when
measuring storage performance.

## Directory Structure

```
include/      Public headers
src/
  db/         MemTable, SkipList, InternalKey
  table/      Block, SSTable, TableBuilder, Filter, Iterator
  util/       Comparator, Coding, CRC32C, Arena, Env, Bloom
tests/        Unit tests
benchmarks/   Performance benchmarks
```
