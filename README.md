haskell-vcache
==============

VCache enables Haskell developers using 64-bit systems to:

* work with structured acyclic values larger than system memory 
* update persistent PVars and STM TVars together, atomically 
* achieve a high degree of structure sharing 
* compare or diff large values based on reference equality

VCache leverages LMDB as an auxiliary address space. Values in this space are referenced by **VRef**. Referencing or dereferencing a value is treated as a *pure* computation, and dereferencing may be performed lazily in a Haskell computation. When serializing a value into LMDB, VRefs may also be serialized, enabling flexible representation of structured acyclic values. 

The persistence layer allows developers to use named persistent variables of type **PVar**, each of which contains a VRef. Persistent variables may be used together with STM TVars in a transaction, which is convenient for keeping the persistent content consistent with volatile process content. VCache persistence supports the full set of ACID properties (though durability is optional on a per-transaction basis).

VCache has its own garbage collector. VRefs are ephemeral roots (tracked via `mkWeakMVar`) and PVars are persistent roots. GC is based primarily on reference counting. Values are collected concurrently and incrementally by a background thread. The ability to hold large values in memory with a VRef is very convenient; it results in a clean separation-of-concerns between persistence and memcaching. 

VCache caches Haskell values. The cache is actually part of each VRef, so if the VRef is GC'd then so will be the cache. In addition, the VCache garbage collector will heuristically decide whether to clear the cache for each VRef. Developers have some influence over this behavior, i.e. selecting between a few cache modes, and bypassing or explicitly clearing the cache. The default behavior is that values will be kept in memory for at least a few GC passes after the last dereference, then probabilistically cleared on subsequent passes until the cache size (computed heuristically based on serialization sizes) is below a threshold.

VCache supports structure sharing. At the LMDB layer, all values with the same serialized representation are stored at the same address. At the Haskell layer, all instances of a VRef with the same type and address also share the same cache resource. Structure sharing is a rather mixed bag of advantages and overheads - one of those features that, like tail recursion, is easiest to leverage when pervasive and guaranteed. Structure sharing isn't configurable, since it's valuable for observable purity while supporting comparisons on VRefs.

VCache supports hierarchical decomposition of applications in a simplistic and stable manner: by allowing programmers to wrap a simple renaming function around PVars for each subprogram. This can easily model subdirectories, encryption schemes, and other features, in addition to supporting shared variables or limiting access to a variable.

Notes and Caveats
-----------------

VCache opens LMDB with the `MDB_NOLOCK` flag, and provides its own model above it only for Haskell threads. This sacrifices a few of LMDB's nice features, such as hot copy. And there is risk of corruption if the one LMDB file is used concurrently. Developers may wish to provide their own application-level APIs for import, export, backup, etc..

The PVar transactions use optimistic concurrency, leveraging Haskell/GHC STM. Optimistic concurrency has a lot of nice features, such as there are never any deadlocks and read-only or write-only transactions never conflict. But one ugly property is that long-running transactions can easily starve if short-running transactions keep changing the values. If starvation becomes a problem, developers will need to build some mechanisms for synchronization or cooperation in the application layer (perhaps specific to certain PVars).

VCache is not a database. VCache does not support schema or queries or observers. Anything like that must be built above VCache, in the application layer.

Contrast
--------

Haskell packages similar to VCache:

* **acid-state** supports a single persistent variable per database, and records this variable in terms of a checkpointed value and a sequence of updates. Acid-state cannot readily integrate atomic updates with multiple variables or non-persistent resources. Acid-state also doesn't perform any memory caching, so a very large resource becomes a significant burden on system memory and imposes larger idling costs due to sweeps from the the Haskell garbage collector.

* **TCache** provides STM-based DBRefs that are similar to PVars from VCache. However, TCache operates only at the variable layer; it does not support a notion of structured immutable values, and does not provide any garbage collection. TCache also attempts to be generic across databases, and this creates a lot of performance overhead. If you're using TCache with Amazon S3 or similar, then VCache can't directly replace it. But if you're using a filesystem-backed TCache, VCache is most likely a superior option.

* **perdure** from Cognimeta has a single persistent variable per file and supports structured values and memory caching. Unfortunately, perdure does not support ephemeral roots, so developers must be careful about laziness and cannot readily separate values from variables. Also, perdure uses a file format created just for perdure, and has not demonstrated scalability beyond 100MB.
