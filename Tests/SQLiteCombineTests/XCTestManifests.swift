import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(SQLiteCombineTests.allTests),
    ]
}
#endif
