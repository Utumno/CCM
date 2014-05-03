Lock --> Does it lock a page ? (node of B+ or page of the Data File)


APIs:
Concurrency Control Manager (CCM) --> component that helps recover from __deadlocks__
Concurrency Control Manager (CCM) --> basic interaction to the simplified buffer manager

- Executors: multiple threads for the
realization of the work that the clients ask the server to do on their behalf


Assumptions:

Updates == deletion + insertion


TODOs

Buffer manager full ---- DEADLOCKS on POOL_LOCK
BM----------THREAD UNSAFE
BM return Pages and not Frames
LRU or other algorithm
FramePage into buffer manager - check thread safety
MOVE flush out of the insert etc
Extract helper methods in insert
Be sure to throw if insert fails
!!! HF - read from disk if exists (header class - last)
!!! HF --> on creation deduce slots from record size+ page size + page header size
