haskell-vcache
==============

* work with structured acyclic values larger than system memory 
* update persistent variables and STM TVars together, atomically 
* structure sharing; diff structures with reference equality 
* subdirectories for roots support decomposition of applications

Concepts
--------

VCache provides extended, persistent memory with ACID transactions to Haskell. The intention is to support applications that operate on data much larger than system memory, or that desire nearly transparent persistence. VCache uses two main concepts: **VRef** and **PVar**.

A **VRef** is a reference to an immutable value, which is usually stored outside normal Haskell process memory. When dereferenced, the value will be parsed and loaded into Haskell's memory. Typically, a dereferenced value will be cached for a little while, to assist in future dereference operations. Developers can influence or bypass the caching behavior. Structure sharing is implicit: all VRefs with the same serialized data will also use the same address.

A **PVar** is a reference to a mutable variable which contains a value. Operations on PVars are transactional, building upon STM. PVar contents are not cached, but their first read is lazy and they may contain VRefs for cached content.

Values in VCache must be serializable into binary strings, VRefs, and PVars. VCache has a reference-counting garbage collector to remove unused VRefs and PVars. Persistence is supported by named root PVars, which are essentially global persistent variables that will not be GC'd. Holding a VRef or PVar in Haskell process is also sufficient to prevent garbage collection.

VCache is backed by LMDB, a memory-mapped key value store that is known empirically to scale to at least a few terabytes. Theoretically, it could scale to a few exabytes, but you're likely to shard and distribute your app long before you reach that limit. While LMDB is read-optimized, VCache mitigates write performance concerns by batching multiple STM-layer transactions into one LMDB-layer update and writing sequentially within each database.

Compare and Contrast
--------------------

Haskell packages similar to VCache:

* **acid-state** supports a single persistent variable per database, and records this variable in terms of a checkpointed value and a sequence of updates. Acid-state cannot readily integrate atomic updates with multiple variables or non-persistent resources. Acid-state also doesn't perform any memory caching, so a very large persistent resource becomes a significant burden on system memory and the Haskell garbage collector.

* **TCache** provides STM-based DBRefs that are similar to PVars from VCache. However, TCache operates only at the named variable layer; it does not support a notion of structured immutable values, and does not support anonymous variables. TCache also attempts to be generic across databases, which introduces a lot of performance overhead. If you're using TCache with Amazon S3 or shared databases, then VCache can't replace TCache. But if you're using a filesystem-backed TCache, VCache is a superior option.

* **perdure** from Cognimeta has a single persistent variable per file and supports structured values and memory caching. Unfortunately, perdure does not support ephemeral roots, so developers must be careful about laziness and cannot readily separate values from variables. Also, perdure uses a file format created just for perdure, and has not demonstrated scalability beyond 100MB.

Notes and Caveats
-----------------

VCache works best if you open only one instance. Libraries, frameworks, plugins, etc. that use VCache should not instantiate it, instead accepting VCache as an argument. Open VCache in main, then grant each persistent software component a stable subdirectory to resist namespace collisions between PVar roots. 

A VCache file may be opened by only one process at a time, and only once within said process. The LMDB layer is used with `MDB_NOLOCK` and is thus unsuitable for IPC. Concurrent access to VCache is only safe if using multiple threads within one process. A lockfile is used to prevent accidentally opening a VCache resource more than once.

VCache transactions build upon STM. The caveats for STM apply. Long running transactions may be starved by short-running transactions. If heavy contention is anticipated or observed, developers may need to model queues and channels and external patterns for cooperative concurrency. 

VCache is not a database. Features for search, query, query optimization, index, join, import, export, replication, sharding, and so on are not supported. VCache is just a persistence layer. Persistence may be useful for modeling long-running relationships with a real database, or for modeling an ad-hoc database using Haskell types. 

This package does not provide the datatypes to make VCache especially useful. Container types - B-trees, tries, finger-trees, ropes, arrays, hashmaps, etc. - implemented leveraging VRefs type would make it a lot easier to pick up and use VCache. This package does not provide type versioning. A variation of SafeCopy for the VCacheable instance seems a worthy investment, but is not a major priority at this time.

Developers are permitted to model cyclic structures using PVars. However, the naive reference counting used by VCache will not garbage collect cycles. Developers must be careful to break cycles when finished with them or they will leak space at the persistence layer. This isn't an especially difficult burden, but it's something to keep in mind.
