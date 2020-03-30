# `subdb` - Substrate Database
### An experimental domain-specific database for Substrate

## Domain

`subdb` is a database designed for use with the Substrate Modified Merkle Patricia Tree ("trie"). Rather than a "normal" flat key/value database, `subdb` makes two requirements on keys:

- Uniform entropy distribution.
- Same size.

This is fine if you know your keys are the output of a cryptographic hasher, but otherwise perhaps not super useful.

Furthermore the design is made to be optimal over certain distribution of sizes of values:

- At least 32 bytes.
- Majority of values of size 32 bytes to around 512-544 bytes, with many sharing the same size.
- Minority of values up to 1MB, with clusters sharing roughly the same size exponentially more likely at the lower end.
- Small numbers of values occasionally found beyond 1MB in size. 

In addition, it supports reference counting on the datagrams stored (allowing each item to be interested and removed multiple times with subsequent insertions and their corresponding removals being fast).

Eventually, it aims to support value links, allowing for each value to have a number of "friend" values, which are stored with it and may be looked-up faster than by using just their hash. This is particularly useful for cryptographic-hash-linked structures such as a Merkle trees and DAGs.

The minimum working size for a `subdb` database instance is currently 400-500 MB (though there are plans to reduce this), and this can be used to store up to around 1 million key/values if the size distribution is optimal. After this the database will grow as needed. No compression is used, but it is designed to be a fairly compact layout with minimal overhead per key/value pair; it should be typically be only 11 additional bytes storage footprint per item stored. It is assumed that hosts will have a sufficient amount of free physical memory to keep the database in memory at once and the disk backing is used only for persistence.

It is designed to be fairly fast to fetch keys by hash and to insert and remove values. In almost all cases, finding, insertion and removal will require just two random access operations; one of them into a 128MB memory-mapped index file and the other into a 2MB mem-mapped content file. Furthermore, it is designed to be twice as fast to fetch data if the address is already known (e.g. because it is stored alongside a referencing entry) as the first 128MB index lookup can be avoided, reducing it to a single access. Insertion and removal requires a third fixed-location access also, however since it is likely held in processor cache when under load, it's unlikely to affect performance.

## Architecture

Sub-DB is formed as a database with only two component types: A single index table and a set of storage tables.

The index table is used exclusively as a means of looking up a key to get an address. It is implemented as a giant hash table, using the first three bytes of the key as an index between 0 and 2**24. For around 200,000 uniformly randomly distributed keys, the chances of collision are minimal however, we handle them in two ways: 
 Index entries are 8 bytes, the first 4 are the first four bytes of the key (TODO: this may change to a 24 bit negative offset from the item). The next bit is the *skipped* flag letting us know if a value placed after this one was done so because of a collision at of before this. The following 31 bits are given over to the items *address*, which has three components; the *datum size*, the *storage table index* and the *storage table entry index*.
 
 Of this 31-bit value, the first 7 bits is the value's *datum size*, encoded in a special logarithmic `DatumSize` format allowing for size precision to be distributed in an optimal way.
 
 The other 24-bits are split between *storage table index* (a unique storage table can be found by combining the `DatumSize` and the this) and the *storage table entry index* (which can identify a specific datum within a storage table). These are split depending on *datum size*, with smaller sizes having more bits dedicated to the *entry index*. The highest sizes have no bits dedicated to the *entry index* at all as their tables have only  single entry. 
 
 The storage tables come in two flavours: fixed size and oversize. Data items over around 2MB are considered oversize. Oversize tables are just a file containing a reference count, and a key (in full) as well as its corresponding value.
 
 Fixed size tables are really just heap slabs with a bump allocator. A simple free list linked-list is maintained for allocated items that have since been freed. Each allocated item has a reference count, as well as its key in full and its value. Since items are fixed size, and since tables are held in memory as a reference, knowing an item's address is enough to get a reference to it without any further I/O.
  