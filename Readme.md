This is a model of the database operations focusing on the transactions handling part

Model:

- Any transaction is associated with one only thread - henceforth referred to as the client.
- The client requests a transaction (TODO: bounds) and then passes it as argument to the methods of the Engine interface.
- Those methods delegate to threads internal to the engine which may block if a lock needed can not be acquired.
