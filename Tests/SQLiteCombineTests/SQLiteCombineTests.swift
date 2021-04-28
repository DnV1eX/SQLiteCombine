//
//  SQLiteCombineTests.swift
//  SQLiteCombine
//
//  Created by Alexey Demin on 2021-04-19.
//  Copyright © 2021 DnV1eX. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//

import XCTest
import SQLiteCombine
import Combine


final class SQLiteCombineTests: XCTestCase {
    
    var db: SQLite!
    
    
    override func setUpWithError() throws {
        
        if db == nil {
            db = try SQLite()
        }
        
        let completionExpectation = expectation(description: "Create Table completion")
        let valueExpectation = expectation(description: "Create Table value")
        valueExpectation.isInverted = true
        _ = db.publisher(sql: "CREATE TABLE test (one, two, three, four, five)")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                completionExpectation.fulfill()
            } receiveValue: {
                valueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    override func tearDownWithError() throws {
        
        let completionExpectation = expectation(description: "Drop Table completion")
        let valueExpectation = expectation(description: "Drop Table value")
        valueExpectation.isInverted = true
        _ = db.publisher(sql: "DROP TABLE test")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                completionExpectation.fulfill()
            } receiveValue: {
                valueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testValueOutput() throws {

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT name FROM sqlite_master", outputType: String.self)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { name in
                XCTAssertEqual(name, "test")
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testTupleOutput() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
        insertValueExpectation.isInverted = true
        _ = db.publisher(sql: "INSERT INTO test (one, two, three, four, five) VALUES (1, 2.0, '3', x'04', NULL)")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT * FROM test", outputType: (int: Int, double: Double, string: String, data: Data, optional: Any?).self)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { row in
                XCTAssertEqual(row.int, 1)
                XCTAssertEqual(row.double, 2.0)
                XCTAssertEqual(row.string, "3")
                XCTAssertEqual(row.data, Data([0x04]))
                XCTAssertNil(row.optional)
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testArrayOutput() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
        insertValueExpectation.isInverted = true
        _ = db.publisher(sql: "INSERT INTO test (one, two, three, four) VALUES (?, ?, ?, ?)", 0, -1, Int.min, Int.max)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT * FROM test", outputType: [Int?].self)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { row in
                XCTAssertEqual(row, [0, -1, Int.min, Int.max, nil])
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testEmptyOutput() throws {

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT * FROM test", outputType: Any.self)
            .collect()
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { rows in
                XCTAssertEqual(rows.count, 0)
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testMultipleOutput() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
        insertValueExpectation.isInverted = true
        _ = db.publisher(sql: "INSERT INTO test (one) VALUES (?), (?), (?)", "unus", "eins", "odin")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT one FROM test", outputType: String.self)
            .collect()
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { rows in
                XCTAssertEqual(rows, ["unus", "eins", "odin"])
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }

    
    func testStructMapping() throws {

        let t = Test(int: 42, double: .pi, string: "", data: Data(), optional: nil)
        
        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
        insertValueExpectation.isInverted = true
        _ = db.publisher(sql: "INSERT INTO test (one, two, three, four, five) VALUES (?, ?, ?, ?, ?)", t.int, t.double, t.string, t.data, t.optional)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT * FROM test", outputType: (Int, Double, String, Data, Data?).self)
            .map(Test.init)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { test in
                XCTAssertEqual(test, t)
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testAsyncRequest() throws {

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT count(*) FROM sqlite_master", outputType: Int.self)
            .subscribe(on: DispatchQueue.global())
            .receive(on: DispatchQueue.main)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { count in
                XCTAssertEqual(count, 1)
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 1, handler: nil)
    }
}



extension SQLiteCombineTests {
    
    struct Test: Equatable {
        let int: Int
        let double: Double
        let string: String
        let data: Data
        let optional: Data?
    }
}
