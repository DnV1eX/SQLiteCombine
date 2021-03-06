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
            db.trace(events: .all)
        }
        
        let completionExpectation = expectation(description: "Create Table completion")
        let valueExpectation = expectation(description: "Create Table value")
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
        _ = db.publisher(sql: "DROP TABLE IF EXISTS test")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                completionExpectation.fulfill()
            } receiveValue: {
                valueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testOutputValue() throws {

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
    
    
    func testOutputTuple() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
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
        
        let selectImplicitCompletionExpectation = expectation(description: "Select implicit completion")
        let selectImplicitValueExpectation = expectation(description: "Select implicit value")
        _ = db.publisher(sql: "SELECT * FROM test")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectImplicitCompletionExpectation.fulfill()
            } receiveValue: { (int: Int, double: Double, string: String, data: Data, optional: Any?) in
                XCTAssertEqual(int, 1)
                XCTAssertEqual(double, 2.0)
                XCTAssertEqual(string, "3")
                XCTAssertEqual(data, Data([0x04]))
                XCTAssertNil(optional)
                selectImplicitValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testOutputArray() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
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
        
        let selectVoidCompletionExpectation = expectation(description: "Select Void completion")
        let selectVoidValueExpectation = expectation(description: "Select Void value")
        selectVoidValueExpectation.isInverted = true
        _ = db.publisher(sql: "SELECT * FROM test")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectVoidCompletionExpectation.fulfill()
            } receiveValue: {
                selectVoidValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testMultipleOutput() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
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
        _ = db.publisher(sql: "INSERT INTO test (one, two, three, four, five) VALUES (?, ?, ?, ?, ?)", t.int, t.double, t.string, t.data, t.optional)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT * FROM test")
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
    
    
    func testCompletionFailure() throws {

        let failureExpectation = expectation(description: "Create Table failure")
        let successExpectation = expectation(description: "Create Table success")
        successExpectation.isInverted = true
        let valueExpectation = expectation(description: "Create Table value")
        valueExpectation.isInverted = true
        _ = db.publisher(sql: "CREATE TABLE test (one, two, three, four, five)")
            .sink { completion in
                if case let .failure(error) = completion {
                    XCTAssert(error.localizedDescription.contains("DBError error 1"))
                    failureExpectation.fulfill()
                } else {
                    successExpectation.fulfill()
                }
            } receiveValue: {
                valueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    @available(macOS 11.0, iOS 14.0, tvOS 14.0, watchOS 7.0, *)
    func testMultipleRequests() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
        insertValueExpectation.expectedFulfillmentCount = 3
        _ = ["INSERT INTO test (two) VALUES ('duo')",
             "INSERT INTO test (two) VALUES ('zwei')",
             "INSERT INTO test (two) VALUES ('dva')"].publisher
            .flatMap(db.publisher)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT two FROM test", outputType: String.self)
            .collect()
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { rows in
                XCTAssertEqual(rows, ["duo", "zwei", "dva"])
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testDatabaseLock() throws {
        
        let completionExpectation = expectation(description: "Drop Table completion")
        let valueExpectation = expectation(description: "Drop Table value")
        _ = db.publisher(sql: "SELECT count(*) FROM test")
            .catch { _ in self.db.publisher(sql: "DROP TABLE test") }
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                completionExpectation.fulfill()
            } receiveValue: {
                valueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testParameters() throws {

        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
        _ = db.publisher(sql: "INSERT INTO test VALUES (?, :two, :two, ?, ?1)", 0.1, 0.2, 0.3)
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher(sql: "SELECT one, two, three, four, five FROM test")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectCompletionExpectation.fulfill()
            } receiveValue: { (row: [Double]) in
                XCTAssertEqual(row, [0.1, 0.2, 0.2, 0.3, 0.1])
                selectValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testStatement() throws {

        let table = "test"
        let columns = ("one", "two", "three", "four", "five")
        
        let insertCompletionExpectation = expectation(description: "Insert completion")
        let insertValueExpectation = expectation(description: "Insert value")
        _ = db.publisher("INSERT INTO \(K: table) VALUES (\(1, 2.0, "3", Data([0x04]), nil))")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                insertCompletionExpectation.fulfill()
            } receiveValue: {
                insertValueExpectation.fulfill()
            }

        let selectCompletionExpectation = expectation(description: "Select completion")
        let selectValueExpectation = expectation(description: "Select value")
        _ = db.publisher("SELECT * FROM test", outputType: (int: Int, double: Double, string: String, data: Data, optional: Any?).self)
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
        
        let selectImplicitCompletionExpectation = expectation(description: "Select implicit completion")
        let selectImplicitValueExpectation = expectation(description: "Select implicit value")
        _ = db.publisher("SELECT \(K: columns.0, columns.1, columns.2, columns.3, columns.4) FROM \(K: table) WHERE \(K: columns.0)=\(1) AND \(K: columns.1)=\(2.0) AND \(K: columns.2)=\("3") AND \(K: columns.3)=\(Data([0x04])) AND \(K: columns.4) IS \(nil)")
            .sink { completion in
                if case let .failure(error) = completion { XCTFail(String(describing: error)) }
                selectImplicitCompletionExpectation.fulfill()
            } receiveValue: { (int: Int, double: Double, string: String, data: Data, optional: Any?) in
                XCTAssertEqual(int, 1)
                XCTAssertEqual(double, 2.0)
                XCTAssertEqual(string, "3")
                XCTAssertEqual(data, Data([0x04]))
                XCTAssertNil(optional)
                selectImplicitValueExpectation.fulfill()
            }
        
        waitForExpectations(timeout: 0, handler: nil)
    }
    
    
    func testArray2Tuple() throws {
        
        XCTAssert([].tuple() as! () == ())
        XCTAssertEqual([1].tuple() as? Int, 1)
        for i in 2...32 {
            let array = Array(1...i)
            XCTAssertEqual(array.tuple().map(Mirror.init)?.children.map(\.value) as? [Int], array)
        }
        XCTAssertNil(Array(1...33).tuple())
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
