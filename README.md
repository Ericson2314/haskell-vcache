haskell-vcache
==============

VCache enables Haskell developers using 64-bit systems to:

* work with structured acyclic values larger than system memory 
* update persistent PVars and STM TVars together, atomically 
* achieve a high degree of structure sharing 
* compare or diff large values based on reference equality
* easily decompose applications into persistent subprograms

VCache performs garbage collection at the persistence layer, but tracks ephemeral value references in Haskell memory. This offers a convenient separation of concerns between large memcached values and persistent variables, and greatly reduces synchronization requirements. VCache garbage collection is based on reference counting, and tracking Haskell value references is achieved by use of `System.Mem.Weak` and `mkWeakMVar`.

The type `VRef a` denotes an immutable value reference. To dereference a value or create a value reference is considered a pure computation. Persisted values are serialized as a stream of value references and bytes. Structure sharing is applied for values of identical representations. On dereference, values will typically be cached to avoid parsing and improve structure sharing on future dereference. Developers have some control over caching on a per reference basis. 

The type `PVar a` denotes a persistent variable. Persistent variables are manipulated transactionally. VCache will combine transactions that complete at around the same time into one larger transaction at the persistence layer, thereby amortizing synchronization costs for bursts of small transactions. Full ACID properties are supported, but durability is optional. Transactions may include arbitrary STM computations, which simplifies consistency between the application's volatile and persistent layers.

By confining subprograms each to their own subset of persistent variable resources, it becomes much easier to reason about communication within an application. To support this, VCache provides a simple filesystem-like model, enabling different subprograms to be confined subdirectories to be provided to different subprograms. A single VCache object may easily be shared across many libraries and frameworks, and PVars from parent or sibling directories become securable capabilities. Developers need only to preserve stability of resource identifiers across versions of source code, which is not difficult to do.

VCache is backed by [LMDB](http://symas.com/mdb/), a memory mapped key-value store.

Compare and Contrast
--------------------

Haskell packages similar to VCache:

* **acid-state** supports a single persistent variable per database, and records this variable in terms of a checkpointed value and a sequence of updates. Acid-state cannot readily integrate atomic updates with multiple variables or non-persistent resources. Acid-state also doesn't perform any memory caching, so a very large persistent resource becomes a significant burden on system memory and the Haskell garbage collector.

* **TCache** provides STM-based DBRefs that are similar to PVars from VCache. However, TCache operates only at the variable layer; it does not support a notion of structured immutable values, and does not provide any garbage collection. TCache also attempts to be generic across databases, and this creates a lot of performance overhead. If you're using TCache with Amazon S3 or similar, then VCache can't directly replace it. But if you're using a filesystem-backed TCache, VCache is most likely a superior option.

* **perdure** from Cognimeta has a single persistent variable per file and supports structured values and memory caching. Unfortunately, perdure does not support ephemeral roots, so developers must be careful about laziness and cannot readily separate values from variables. Also, perdure uses a file format created just for perdure, and has not demonstrated scalability beyond 100MB.

Notes and Caveats
-----------------

VCache uses LMDB with the `MDB_NOLOCK` flag, and provides its own concurrency control for the lightweight Haskell threads. This results in a simpler implementation (e.g. no need to deal with bound threads). However, it sacrifices a few of LMDB's nice features; for example, we cannot ensure consistency with the `mdb_copy` command line utility on a VCache backing file (at least, not without halting the application). However, VCache does utilize a simple lockfile to reduce risk of opening a resource twice at the VCache layer.

VCache is not a database. It's just a bunch of persistent variables and values. If developers want database features - such as queries, indices, views - those must be implemented in another layer, e.g. the application or library or framework. It is recommended that libraries or frameworks using VCache should always accept the VCache as an argument, rather than each creating its own file. Doing so improves hierarchical decomposition and the ability for values to easily be shared between subprograms.

PVars are not 'first class': VRef values may not contain PVars. While I could implement first class PVars, and even anonymous GC'd PVars, it isn't clear to me that doing so is a good idea. It would certainly hinder mobility of VRef values (ability to copy them to another cache, or replicate across a network), and it may discourage purely functional domain models. For now, I'm leaving this potential feature outside the scope of VCache. (Even so, VCache is much more expressive than acid-state, perdure, and TCache.)

PVar transactions use optimistic concurrency. While this has many advantages, there is risk of a long stream of short transactions 'starving' a long running read-write transaction. If a PVar has many writers, developers may need to utilize external synchronization or cooperation patterns to reduce conflict. It shouldn't be difficult to model persistent queues and command patterns and so on. 

LMDB files are not especially portable. Endian-ness, filesystem block sizes, and so on hinder porting files.
