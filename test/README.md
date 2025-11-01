# Testing Node-RED ODBC Nodes

This directory contains unit tests for the Node-RED ODBC nodes.

## Setup

Install test dependencies:

```bash
npm install
```

## Running Tests

Run all tests:

```bash
npm test
```

Run tests with verbose output:

```bash
npm test -- --reporter spec
```

Run a specific test file:

```bash
npx mocha test/odbc_spec.js
```

## Test Structure

The tests use:
- **Mocha** - Test framework
- **node-red-node-test-helper** - Official Node-RED testing helper
- **Sinon** - For mocking and spying
- **Should** - Assertion library

## Test Coverage

Current tests cover:

1. **ODBC Pool Node**
   - Node loading and configuration
   - Pool configuration storage

2. **ODBC Query Node**
   - Node loading
   - Query string handling from config
   - Payload parsing (JSON string and object)
   - Error handling

3. **ODBC Procedure Node**
   - Node loading
   - Catalog/schema/procedure configuration
   - Parameters parsing
   - Null handling for catalog/schema

## Adding New Tests

To add new tests, edit `odbc_spec.js` and add new test cases following the existing pattern:

```javascript
it('should test something', function(done) {
  const flow = [
    { id: 'n1', type: 'ODBC pool', connectionString: 'DSN=test' }
  ];
  helper.load(function(RED) {
    odbcNode(RED);
  }, flow, function() {
    const n1 = helper.getNode('n1');
    // Your assertions here
    done();
  });
});
```

## Note

These tests mock the ODBC connection behavior. They don't require an actual ODBC database connection, but they do require the `odbc` npm package to be installed (which is a dependency).

