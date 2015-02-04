haskell-vcache
==============

* work with structured acyclic values larger than system memory 
* update persistent variables and STM TVars together, atomically 
* structure sharing; diff structures with reference equality 
* subdirectories for roots support decomposition of applications

Concepts
--------

VCache provides extended, persistent memory with ACID transactions to Haskell. The intention is to support applications that operate on data much larger than system memory, or that desire nearly transparent persistence. VCache uses two main concepts: **VRef** and **PVar**.

A **VRef** is a reference to an immutable value, which is usually stored outside normal Haskell process memory. When dereferenced, the value will be parsed and loaded into Haskell's memory. Typically, a dereferenced value will be cached for a little while, in case of future dereference operations. But developers do have some ability to bypass or control the cache behavior. Constructing a VRef is treated as a pure computation, and structure sharing is applied automatically (values with the same serialized representation will share the same address). Values much larger than system memory can be modeled by structuring from VRefs. 

A **PVar** is a reference to a mutable variable which contains a value. Operations on PVars are transactional, but durability is optional: writes will be batched and persisted in a background thread, and durable transactions additionally wait on a signal that everything has synchronized to disk. The transactions are a thin layer above STM, and arbitrary STM actions allow easy interaction between persistent and ephemeral resources. PVars are not cached, but their first read is lazy and they may easily contain VRefs for cached content.

Persistence is supported by **named roots**, which are essentially global PVars in a virtual filesystem. These PVars may easily be reloaded from one instance of the application to another. Typically, an application should have only one or a few of these global roots, pushing most structure into the domain model. However, there is no restriction on the number of roots, and PVars obtained this way are still first-class.

VCache provides its own garbage collector, which runs in a background thread. Reference counting is used to track persistent objects, and `System.Mem.Weak` ephemeron tables prevent destruction of objects in Haskell memory. Compared to sweeping collectors, reference counting has performance advantages for large scales (e.g. terabytes of live data), but developers must be careful about forming cycles between PVars: either avoid cycles or explicitly break cycles before unlinking from the persistent root.

VCache is backed by LMDB, a memory-mapped key value store that is known to scale to at least a few terabytes and theoretically could scale to a few exabytes. You'll likely distribute and shard your application long before reaching theoretical limits. If applied carefully, VCache could be used as a basis for modeling ad-hoc databases within the application.

Compare and Contrast
--------------------

Haskell packages similar to VCache:

* **acid-state** supports a single persistent variable per database, and records this variable in terms of a checkpointed value and a sequence of updates. Acid-state cannot readily integrate atomic updates with multiple variables or non-persistent resources. Acid-state also doesn't perform any memory caching, so a very large persistent resource becomes a significant burden on system memory and the Haskell garbage collector.

* **TCache** provides STM-based DBRefs that are similar to PVars from VCache. However, TCache operates only at the named variable layer; it does not support a notion of structured immutable values, and does not support anonymous variables. TCache also attempts to be generic across databases, which introduces a lot of performance overhead. If you're using TCache with Amazon S3 or shared databases, then VCache can't replace TCache. But if you're using a filesystem-backed TCache, VCache is a superior option.

* **perdure** from Cognimeta has a single persistent variable per file and supports structured values and memory caching. Unfortunately, perdure does not support ephemeral roots, so developers must be careful about laziness and cannot readily separate values from variables. Also, perdure uses a file format created just for perdure, and has not demonstrated scalability beyond 100MB.

Notes and Caveats
-----------------

Multiple VCache instances won't work together very nicely. PVars cannot be shared between VCache instances. For values that could be shared, implicit deep copies would be a performance headache. Transactions involving PVars from multiple caches cannot be fully atomic. VCache will generally raise a runtime exception if you accidentally mix values from two instances. The assumption is that you'll just be using one instance for the application: open it in main, then grant each persistent subprogram a stable subdirectory for PVar roots.

VCache does not support concurrent instances and mustn't be used for inter-process communication. The LMDB layer uses the `MDB_NOLOCK` flag, and important components (STM, GC) are process local anyway. Without reader locks, the `mdb_copy` utility won't work on a running LMDB database. VCache does use a coarse grained lockfile to resist accidental concurrency (i.e. if you start the app twice with the same LMDB backing file, one of the two should wait briefly on the lock then fail). 

VCache doesn't enforce any type versioning features comparable to the `SafeCopy` class used by acid-state. Developers should either ensure backwards compatibility by hand (e.g. by including type version information for unstable types) or develop a variation of SafeCopy for VCache.

Transactions use optimistic concurrency. While this has many advantages, a stream of short transactions may starve a long running transaction. Where heavy contention is anticipated or observed, developers should utilize external synchronization or cooperation patterns to reduce conflict, such as queues and channels. 

LMDB is memory mapped, and thus is limited by address space. To be used effectively, LMDB requires an address space much larger than available memory.

VCache does not provide search, query, indexing, joins, import or export, replication or sharding, and other database features. VCache shouldn't be considered a replacement for a database. It might be a suitable basis for implementing ad-hoc application specific databases, but any such database should have its own dedicated Haskell packages. The ability to model persistent interactions without requiring a database can usefully reduce burden on 'real' databases.

