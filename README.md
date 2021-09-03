# SQLiteCombine

A lightweight Swift library exposing SQLite database access through Combine publishers. Bringing all the power of reactive programming it strives to stay close to the SQLite engine. Data can be stored in four types: *Int*, *Double*, *String* and *Data*, each of which can be optional. You create a publisher with SQL query and values. The publisher output type is typically a tuple.

## Usage Example
```swift
import SQLiteCombine

let url = try! FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
    .appendingPathComponent(filename, isDirectory: false).appendingPathExtension("db")
let sqlite = try! SQLite(filename: url.path)

let queue = DispatchQueue(label: "Database Queue")

sqlite.publisher("SELECT id, name, state FROM entities WHERE state!=\(Entity.State.deleted.rawValue)")
    .subscribe(on: queue)
    .tryMap(Entity.init)
    .collect()
    .replaceError(with: [])
    .receive(on: DispatchQueue.main)
    .assign(to: &$entities)
```

## Notes
- SQL statement string interpolation supports keys `"\(K: tableName)"` and multiple value binding `"\(value1, value2, value3)"`.
- The output tuple supports up to 32 elements, use an array when exceeded.
- For write queries with *Void* output, it will be sent to subscribers upon execution and can be counted.
- It is recommended to subscribe to publishers on a serial background queue to avoid database locks.
- The project is not fully documented yet, but it is covered with tests that can be used as a reference.

## License
Copyright © 2021 DnV1eX. All rights reserved. Licensed under the Apache License, Version 2.0.
