# LevelDB.NET
Opening a Google LevelDB for read in pure .NET Core

* DISCLAIMER *
This only opens a LevelDB for readonly access, and does not take into account any kind of concurrency access by some other process. So if the DB is open by some other process that is writing to it at the same time, it's going to fail misserably.

For me it's in a state where I can use it for what I need it to do.

- Enumerate all entries.
- Find an entry by key.




