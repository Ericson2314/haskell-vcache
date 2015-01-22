
VCache integrates with STM transactions in a simple manner: every PVar contains a TVar, and the transaction monad tracks which PVars are written. Any conflict detection is delegated to STM, and the transactions may involve arbitrary STM behaviors. Thus, at the end, we only need to concern ourselves with tracking which PVars are written, and pushing their values to the LMDB layer.

We have two options:

1. we can track which values are written to each PVar
2. we can track which PVars are written, and have the writer read them

A relevant concern is having a sequence of two transactions:

* first transaction writes A,B,C
* second transaction writes B,C,D

The goal is to put a consistent set of values into the LMDB layer. 

Under the first option, we could write consistent data to A,B,C even though subsequent data has been written to B,C. However, this does introduce a challenge of serializing the two transactions: how do we know that A,B,C is logically prior to B,C,D anyway?

Under the second option, we cannot just write A,B,C. Assuming the second transaction has completed, we must also read and write D, otherwise B,C,D would become inconsistent at the LMDB layer. Thus, we must somehow remain aware of all the relevant transactions.

One option to ensure consistency is to transactionally record the transactions as they complete, e.g. into a queue. This would handle either use case, the serialization of PVars or a final read before write. But it isn't clear to me how much this queue might hinder concurrency. Certainly if two transactions concurrently try their 'validation' phase while holding references to the same queue variables, one of them will fail [1](https://ghc.haskell.org/trac/ghc/wiki/Commentary/Rts/STM). GHC's implementation of optimistic concurrency isn't optimistic enough.

Can we avoid a concurrency/parallelism bottleneck in VCache?

I think there is no way to reconcile option 1 without using a global queue of some sort, since we're forced to serialize transactions regardless. We also cannot resolve concurrency bottlenecks by use of `orElse`, since anything in the left half of `orElse` becomes part of the read set to support `retry`. We'll have a fixed version independent of concurrency.

For option 2, we might be able to diminish the bottleneck by means of registering our intention to write ahead of time. This would require providing our VCache as an argument to the transaction (or that could be optional, falling back to auto-register). We could then read each of the registered variables when deciding how much to write. Conflict would exist between the writer thread and transactions, but not between the transactions. Unfortunately, it isn't clear that registration offers any significant improvements to concurrency.

A likely answer is that we'll be forced to serialize all VCache transactions.

How much will this hurt? Well, for most of my own use cases, I'd be using just a few variables for most transactions anyway, so the conflict is inevitable. 
Batching and laziness potentially allow a lot of transactions to operate mostly in parallel, only resolving their ordering rather than their fully comptued values. A lot of computation can be implicitly shifted into serialization by background threads.

I suspect we can still achieve an acceptable level of parallelism even if the transactions become a bottleneck. But it may require some cleverness from developers.

