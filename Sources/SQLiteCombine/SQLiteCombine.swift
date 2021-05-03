//
//  SQLiteCombine.swift
//  SQLiteCombine
//
//  Created by Alexey Demin on 2021-04-19.
//  Copyright © 2021 DnV1eX. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//

import Foundation
import SQLite3
import Combine


public final class SQLite {
    
    let db: OpaquePointer
    
    
    public init(filename: String = "", flags: OpenFlags = [.readWrite, .create]) throws {
        
        var db: OpaquePointer?
        switch Result(code: sqlite3_open_v2(filename, &db, flags.rawValue, nil)) {
        case .success:
            if let db = db {
                self.db = db
            } else {
                throw InternalError.dbIsNotInitialized(filename, flags)
            }
        case .failure(let error):
            sqlite3_close_v2(db)
            throw error
        }
    }
    
    deinit {
        sqlite3_close_v2(db)
    }
    
    
    public func publisher<Output>(sql: String, _ values: SQLiteValue?..., outputType: Output.Type = Void.self as! Output.Type) -> Publisher<Output> {
        Publisher(db: db, sql: sql, values: values)
    }
    
    
    public func trace(events: TraceEvents = []) {
        
        sqlite3_trace_v2(db, UInt32(events.rawValue), { event, _, p, x in
            switch Int32(event) {
            case SQLITE_TRACE_STMT: print("SQLite trace stmt", p ?? "", x.map { String(cString: $0.assumingMemoryBound(to: CChar.self)) } ?? "")
            case SQLITE_TRACE_PROFILE: print("SQLite trace profile", p ?? "", x.map { TimeInterval($0.load(as: Int64.self)) * 1e-9 } ?? "")
            case SQLITE_TRACE_ROW: print("SQLite trace row", p ?? "")
            case SQLITE_TRACE_CLOSE: print("SQLite trace close", p ?? "")
            default: break
            }
            return 0
        }, nil)
    }
}


public extension SQLite {
    
    struct OpenFlags: OptionSet {
        public let rawValue: Int32
        public init(rawValue: Int32) {
            self.rawValue = rawValue
        }
        public static let readOnly = OpenFlags(rawValue: SQLITE_OPEN_READONLY)
        public static let readWrite = OpenFlags(rawValue: SQLITE_OPEN_READWRITE)
        public static let create = OpenFlags(rawValue: SQLITE_OPEN_CREATE)
        public static let uri = OpenFlags(rawValue: SQLITE_OPEN_URI)
        public static let memory = OpenFlags(rawValue: SQLITE_OPEN_MEMORY)
        public static let noMutex = OpenFlags(rawValue: SQLITE_OPEN_NOMUTEX)
        public static let fullMutex = OpenFlags(rawValue: SQLITE_OPEN_FULLMUTEX)
        public static let sharedCache = OpenFlags(rawValue: SQLITE_OPEN_SHAREDCACHE)
        public static let privateCache = OpenFlags(rawValue: SQLITE_OPEN_PRIVATECACHE)
    }
}


public extension SQLite {
    
    struct TraceEvents: OptionSet {
        public let rawValue: Int32
        public init(rawValue: Int32) {
            self.rawValue = rawValue
        }
        public static let stmt = TraceEvents(rawValue: SQLITE_TRACE_STMT)
        public static let profile = TraceEvents(rawValue: SQLITE_TRACE_PROFILE)
        public static let row = TraceEvents(rawValue: SQLITE_TRACE_ROW)
        public static let close = TraceEvents(rawValue: SQLITE_TRACE_CLOSE)
        
        public static let all: Self = [.stmt, .profile, .row, .close]
    }
}


extension SQLite {
    
    enum InternalError: Error {
        case dbIsNotInitialized(String, OpenFlags)
        case unknownValueType(Any?, index: Int32)
        case unknownColumnType(Int32, index: Int32)
        case mismatchOutputType([SQLiteValue?], Any.Type)
    }
}


extension SQLite {
    
    enum DBResult {
        case ok, row, done
    }
    
    struct DBError: Error {
        let code: Int
        let message: String
    }
}


public extension SQLite {
    
    struct Publisher<Output>: Combine.Publisher {
        
        public typealias Failure = Error
        
        let db: OpaquePointer
        let sql: String
        let values: [SQLiteValue?]

        
        public func receive<S: Subscriber>(subscriber: S) where S.Input == Output, S.Failure == Failure {
            do {
                try subscriber.receive(subscription: Subscription(subscriber, db, sql, values))
            } catch {
                subscriber.receive(completion: .failure(error))
            }
        }
    }
}

extension SQLite.Publisher {
    
    final class Subscription<S: Subscriber>: Combine.Subscription where S.Input == Output, S.Failure == Failure {
        
        let subscriber: S
        var stmt: OpaquePointer?

        
        init(_ subscriber: S, _ db: OpaquePointer, _ sql: String, _ values: [SQLiteValue?]) throws {
            
            do {
                _ = try Result(code: sqlite3_prepare_v2(db, sql, -1, &stmt, nil)).get()
                for (i, value) in values.enumerated().map({ (Int32($0 + 1), $1) }) {
                    switch value {
                    case .none:
                        _ = try Result(code: sqlite3_bind_null(stmt, i)).get()
                    case let int as Int:
                        _ = try Result(code: sqlite3_bind_int64(stmt, i, sqlite3_int64(int))).get()
                    case let double as Double:
                        _ = try Result(code: sqlite3_bind_double(stmt, i, double)).get()
                    case let string as String:
                        _ = try Result(code: sqlite3_bind_text(stmt, i, string, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))).get()
                    case let data as Data:
                        _ = try Result(code: data.withUnsafeBytes { [stmt] in sqlite3_bind_blob(stmt, i, $0.baseAddress, Int32($0.count), unsafeBitCast(-1, to: sqlite3_destructor_type.self)) }).get()
                    default:
                        throw SQLite.InternalError.unknownValueType(value, index: i)
                    }
                }
                self.subscriber = subscriber
            } catch {
                sqlite3_finalize(stmt)
                throw error
            }
        }
        
        deinit {
//            sqlite3_finalize(stmt)
            cancel()
        }
        
        
        func request(_ demand: Subscribers.Demand) {
            
            var demand = demand
            
            while let stmt = stmt, demand > 0 {
                demand -= 1
                switch Result(code: sqlite3_step(stmt)) {
                case .success(let result) where result == .row:
                    do {
                        let input = try row()
                        demand += subscriber.receive(input)
                    } catch {
                        subscriber.receive(completion: .failure(error))
                        sqlite3_finalize(stmt)
                        self.stmt = nil
                    }
                default:
                    cancel()
//                    return
                }
            }
        }
        
        
        func cancel() {
            
            switch Result(code: sqlite3_finalize(stmt)) {
            case .success:
                subscriber.receive(completion: .finished)
            case .failure(let error):
                subscriber.receive(completion: .failure(error))
            }
            stmt = nil
        }
        
        
        private func row() throws -> Output {
            
            var columns: [SQLiteValue?] = []
            for i in 0..<sqlite3_column_count(stmt) {
                let type = sqlite3_column_type(stmt, i)
                switch type {
                case SQLITE_INTEGER:
                    columns.append(Int(sqlite3_column_int64(stmt, i)))
                case SQLITE_FLOAT:
                    columns.append(sqlite3_column_double(stmt, i))
                case SQLITE_TEXT:
                    columns.append(String(cString: sqlite3_column_text(stmt, i)))
                case SQLITE_BLOB:
                    columns.append(sqlite3_column_blob(stmt, i).map { Data(bytes: $0, count: Int(sqlite3_column_bytes(stmt, i))) } ?? Data())
                case SQLITE_NULL:
                    columns.append(nil)
                default:
                    throw SQLite.InternalError.unknownColumnType(type, index: i)
                }
            }
            guard let output = (columns as? Output) ?? (columns.tuple() as? Output) else {
                throw SQLite.InternalError.mismatchOutputType(columns, Output.self)
            }
            
            return output
        }
    }
}



extension Result where Success == SQLite.DBResult, Failure == SQLite.DBError {
    
    init(code: Int32) {
        switch code {
        case SQLITE_OK:
            self = .success(.ok)
        case SQLITE_ROW:
            self = .success(.row)
        case SQLITE_DONE:
            self = .success(.done)
        default:
            self = .failure(.init(code: Int(code), message: String(cString: sqlite3_errstr(code))))
        }
    }
}



extension Array {
    
    func tuple() -> Any? {
        switch count {
        case 0: return ()
        case 1: return (self[0])
        case 2: return (self[0], self[1])
        case 3: return (self[0], self[1], self[2])
        case 4: return (self[0], self[1], self[2], self[3])
        case 5: return (self[0], self[1], self[2], self[3], self[4])
        case 6: return (self[0], self[1], self[2], self[3], self[4], self[5])
        case 7: return (self[0], self[1], self[2], self[3], self[4], self[5], self[6])
        case 8: return (self[0], self[1], self[2], self[3], self[4], self[5], self[6], self[7])
        case 9: return (self[0], self[1], self[2], self[3], self[4], self[5], self[6], self[7], self[8])
        case 10: return (self[0], self[1], self[2], self[3], self[4], self[5], self[6], self[7], self[8], self[9])
        default: return nil
        }
    }
}



public protocol SQLiteValue { }

extension Int: SQLiteValue { }
extension Double: SQLiteValue { }
extension String: SQLiteValue { }
extension Data: SQLiteValue { }
