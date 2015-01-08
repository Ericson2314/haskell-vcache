haskell-vcache
==============

* work with structured acyclic values larger than system memory 
* update persistent variables and STM TVars together, atomically 
* structure sharing; diff structures with reference equality 
* subdirectories for roots support decomposition of applications

Concepts
--------

VCache provides extended, persistent memory with ACID transactions to Haskell. The intention is to support applications that operate on data much larger than system memory, or that desire nearly transparent persistence. VCache uses two main concepts: **VRef** and **PVar**.

A **VRef** is a reference to an immutable value, which is usually stored outside normal Haskell process memory. When dereferenced, the value will be parsed and loaded into Haskell's memory. Typically, a dereferenced value will be cached for a little while, in case of future dereference operations. But developers do have some ability to bypass or control the cache behavior. Constructing a VRef is treated as a pure computation, and structure sharing is applied automatically (values with the same serialized representation will share the same address). 

A **PVar** is a reference to a mutable variable, which contains a value. Like VRefs, values contained by PVars are usually stored outside normal Haskell process memory, but will be parsed and loaded when necessary. When updated, writes on PVars are batched and persisted atomically to the VCache backend. Durable transactions further wait for a signal that everything is synchronized to disk.

Values serialized to VCache may contain PVars and VRefs internally. This allows working with very large values and ad-hoc domain models, so long as we can break them up using PVars and VRefs as pointers to smaller values. However, there is a major caveat: garbage collection of VCache uses reference counting. Values cannot be cyclic, and developers must either avoid cycles between PVars or be willing to break cycles expliclitly before a PVar goes out of scope. Reference counting is favored because it behaves nicely (compared to space walking collectors) at very large scales. In addition to reference counting, `System.Mem.Weak` prevents collection of ephemeral content contained by PVars or VRefs held in process memory.

Persistence is supported by **named roots**, which are essentially global PVars in a virtual filesystem. These PVars may easily be loaded from one instance of the application to another. Typically, an application should have only one or a few of these global roots, pushing most structure into the domain model. However, there is no restriction on the number of roots, and PVars obtained this way are still first-class.

VCache is backed by LMDB, a memory-mapped key value store that is known to scale to at least a few terabytes and theoretically can scale to a few exabytes. You'll likely distribute and shard your application long before reaching any theoretical limits. VCache uses LMDB without locking (i.e. the `MDB_NOLOCK` option), and has internal structures that must be limited to a single process (e.g. ephemeron tables to track VRefs and PVars in memory). An additional lockfile helps resist the accidental corruption that might occur if a single VCache is used concurrently.

Compare and Contrast
--------------------

Haskell packages similar to VCache:

* **acid-state** supports a single persistent variable per database, and records this variable in terms of a checkpointed value and a sequence of updates. Acid-state cannot readily integrate atomic updates with multiple variables or non-persistent resources. Acid-state also doesn't perform any memory caching, so a very large persistent resource becomes a significant burden on system memory and the Haskell garbage collector.

* **TCache** provides STM-based DBRefs that are similar to PVars from VCache. However, TCache operates only at the variable layer; it does not support a notion of structured immutable values, and does not provide any garbage collection. TCache also attempts to be generic across databases, and this creates a lot of performance overhead. If you're using TCache with Amazon S3 or similar, then VCache can't directly replace it. But if you're using a filesystem-backed TCache, VCache is most likely a superior option.

* **perdure** from Cognimeta has a single persistent variable per file and supports structured values and memory caching. Unfortunately, perdure does not support ephemeral roots, so developers must be careful about laziness and cannot readily separate values from variables. Also, perdure uses a file format created just for perdure, and has not demonstrated scalability beyond 100MB.

Notes and Caveats
-----------------

Multiple VCache instances won't work together very nicely. First-class PVars cannot be shared between VCache spaces. For values that may be shared, implicit deep copies would be a performance headache. VCache will generally raise a runtime exception if you accidentally mix values from two instances. The best use case for VCache is to open one root instance in your main function then pass it to whichever subprograms need it. Persistent libraries and frameworks and plugins and so on should accept VCache as an argument, rather than each loading their own. VCache's subdirectory model can provide a fresh, stable namespace to each subprogram.

VCache does not support concurrent instances and mustn't be used for inter-process communication. The LMDB layer uses the `MDB_NOLOCK` flag, and important components (STM, GC) are process local anyway. Without reader locks, the `mdb_copy` utility won't work on a running LMDB database. VCache does use a coarse grained lockfile to resist accidental concurrency (i.e. if you start the app twice with the same LMDB backing file, one of the two should wait briefly on the lock then fail).

VCache is not a database. VCache is just transparent persistence and extended memory for Haskell. If developers desire rich database features - such as queries, indices, views, searches, import/export - those must be implemented at the application layer. VCache is suitable for developing purpose specific databases within an application, but you probably don't need the full gamut of database features at the VCache layer.

Transactions use optimistic concurrency. While this has many advantages, a stream of short transactions may starve a long running read-write transaction. Where heavy contention is anticipated or observed, developers should utilize external synchronization or cooperation patterns to reduce conflict, such as queues and channels and so on. 

You need a 64-bit system to effectively leverage VCache. Because LMDB is memory mapped, it is limited by available address space (independently of available memory). 

LMDB files are not very portable. Endianness and sensitivity to filesystem block size may interfere with porting LMDB files.
