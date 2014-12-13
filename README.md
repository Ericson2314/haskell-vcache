haskell-vcache
==============

VCache enables Haskell developers using a 64-bit system to:

* work with structured values much larger than system memory 
* atomically persist values, with multiple named roots
* compare or diff values based on references

VCache was created as an alternative to **acid-state** for a use case where I expect to have hundreds of gigabytes of data but a relatively small working set. Keeping large values in memory doesn't scale very nicely: creates too much work for startup, checkpoints, and GC. VCache is very similar to the **perdure** package, but VCache supports ephemeral references (large values don't need to be persistent) and uses LMDB as the backend storage (which has proven scalability). Values used in VCache must have an acyclic representation.

The underlying mechanism of VCache is to leverage LMDB as an alternative address space for values. A value tucked into this address space is identified by a `VRef`, and requires that values serialize to an intermediate, acyclic structures that may contain more such references. Large values must be destructured into smaller ones by use of `VRef`, such that developers may load only what they need. 

VCache performs its own GC, based on reference counting. This GC operates concurrently, via background thread. The VCache GC interacts with Haskell's GC by tracking `VRef` objects in memory. Essentially, every `VRef` counts as an ephemeral root (which is not part of the reference count). Moving a value into VCache will create one of these `VRefs`, initially with zero references. This interaction with Haskell's GC does constrain a VCache database to a single Haskell process (otherwise the GC thread of one process might destroy values in use by the other process).

Structure sharing is enforced: two `VRef` objects are equal if and only if their underlying value is equal. Structure sharing is a mixed bag of advantages and disadvantages. Advantages include easier reasoning about idempotence, efficient computation of differences between values, and potential space savings. Disadvantages include some allocation overhead (e.g. hash lookup) and weaker temporal locality. 

Newly allocated values are appended to the LMDB database. This mitigates insertion overheads and simplifies reasoning about asymptotic cache performance, e.g. serving as an effective basis for reasoning about cache-optimal algorithms in work by Guy Blelloch and Robert Harper [1](http://lambda-the-ultimate.org/node/5021).

The VCache persistence layer is very simple: a table of `(VRef,VRef)` pairs, corresponding to a key-value table. These values are persistent roots, and values held in this table will not be garbage collected.
