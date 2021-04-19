import XCTest
@testable import SQLiteCombine

final class SQLiteCombineTests: XCTestCase {
    func testExample() {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct
        // results.
        XCTAssertEqual(SQLiteCombine().text, "Hello, World!")
    }

    static var allTests = [
        ("testExample", testExample),
    ]
}
