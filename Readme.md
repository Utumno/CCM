This is a small DB model of sorts, focusing on the transactions handling part (no query optimizer etc)
Features

- a BPlus tree index on disk, supporting lookup delete and insert operations - using locking
- a LockManager supporting two face locking, strict (not really concurrent unless only reads are performed - TODO)
- a draft of deadlock manager (high priority this one)
- a heap file on disk (TODO: generify)
- an API for requesting a transactional operation (subclass Engine.TransactionalOperation, then call Engine.submit)

Comes as an eclipse (Kepler/Luna) project (java 1.7 execution environment) - run .settings/CCM_RUN.launch run configuration (should be autoadded).

DISCLAIMER : pre alpha. Subject to history rewriting. Maybe I'll throw some more time on it but don't hold your breath. Issues welcome though :)
