const helper = require('node-red-node-test-helper');
const sinon = require('sinon');
const should = require('should');

// Load the node - it will require 'odbc', but we'll mock it in tests
const odbcNode = require('../odbc.js');

describe('ODBC Nodes', function() {
  beforeEach(function(done) {
    helper.startServer(done);
  });

  afterEach(function(done) {
    helper.unload();
    helper.stopServer(done);
  });

  describe('ODBC pool node', function() {
    it('should be loaded', function(done) {
      const flow = [
        { id: 'n1', type: 'ODBC pool', name: 'test pool', connectionString: 'DSN=test' }
      ];
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n1 = helper.getNode('n1');
        n1.should.have.property('id', 'n1');
        n1.should.have.property('type', 'ODBC pool');
        n1.should.have.property('poolConfig');
        done();
      });
    });

    it('should store pool configuration', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test',
          initialSize: 5,
          maxSize: 20
        }
      ];
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n1 = helper.getNode('n1');
        n1.poolConfig.should.have.property('connectionString', 'DSN=test');
        n1.poolConfig.should.have.property('initialSize', 5);
        n1.poolConfig.should.have.property('maxSize', 20);
        done();
      });
    });
  });

  describe('ODBC query node', function() {
    it('should be loaded', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n2', 
          type: 'ODBC query', 
          name: 'test query',
          connection: 'n1',
          query: 'SELECT * FROM test'
        }
      ];
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n2 = helper.getNode('n2');
        n2.should.have.property('id', 'n2');
        n2.should.have.property('type', 'ODBC query');
        n2.should.have.property('queryString', 'SELECT * FROM test');
        done();
      });
    });

    it('should use predefined query from config', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n2', 
          type: 'ODBC query', 
          name: 'test query',
          connection: 'n1',
          query: 'SELECT * FROM users'
        }
      ];
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n2 = helper.getNode('n2');
        n2.queryString.should.equal('SELECT * FROM users');
        done();
      });
    });

    it('should extract query from JSON string payload', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n2', 
          type: 'ODBC query', 
          name: 'test query',
          connection: 'n1',
          query: 'SELECT * FROM default'
        }
      ];
      
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n2 = helper.getNode('n2');
        const message = {
          payload: JSON.stringify({
            query: 'SELECT * FROM custom',
            parameters: [1, 2, 3]
          })
        };
        
        // Mock the pool and connection
        const mockConnection = {
          query: sinon.stub().resolves([{ id: 1, name: 'test' }]),
          close: sinon.stub()
        };
        const mockPool = {
          connect: sinon.stub().resolves(mockConnection)
        };
        n2.poolNode.pool = mockPool;
        n2.poolNode.connecting = false;

        n2.on('input', function(msg) {
          // The queryString should be updated from payload
          n2.queryString.should.equal('SELECT * FROM custom');
          done();
        });

        n2.emit('input', message);
      });
    });

    it('should extract query from object payload', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n2', 
          type: 'ODBC query', 
          name: 'test query',
          connection: 'n1',
          query: 'SELECT * FROM default'
        }
      ];
      
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n2 = helper.getNode('n2');
        const message = {
          payload: {
            query: 'SELECT * FROM custom_object',
            parameters: [10, 20]
          }
        };
        
        const mockConnection = {
          query: sinon.stub().resolves([{ id: 1 }]),
          close: sinon.stub()
        };
        const mockPool = {
          connect: sinon.stub().resolves(mockConnection)
        };
        n2.poolNode.pool = mockPool;
        n2.poolNode.connecting = false;

        n2.on('input', function(msg) {
          n2.queryString.should.equal('SELECT * FROM custom_object');
          done();
        });

        n2.emit('input', message);
      });
    });

    it('should handle connection errors', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n2', 
          type: 'ODBC query', 
          name: 'test query',
          connection: 'n1'
        }
      ];
      
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n2 = helper.getNode('n2');
        const mockPool = {
          connect: sinon.stub().rejects(new Error('Connection failed'))
        };
        n2.poolNode.pool = mockPool;
        n2.poolNode.connecting = false;

        const errorSpy = sinon.spy(n2, 'error');
        
        n2.on('input', function(msg) {
          setTimeout(() => {
            errorSpy.called.should.be.true;
            done();
          }, 100);
        });

        n2.emit('input', { payload: {} });
      });
    });
  });

  describe('ODBC procedure node', function() {
    it('should be loaded', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n3', 
          type: 'ODBC procedure', 
          name: 'test procedure',
          connection: 'n1',
          catalog: 'MYCAT',
          schema: 'MYSCHEMA',
          procedure: 'MYPROC'
        }
      ];
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n3 = helper.getNode('n3');
        n3.should.have.property('id', 'n3');
        n3.should.have.property('type', 'ODBC procedure');
        n3.should.have.property('catalog', 'MYCAT');
        n3.should.have.property('schema', 'MYSCHEMA');
        n3.should.have.property('procedure', 'MYPROC');
        done();
      });
    });

    it('should convert empty catalog/schema to null', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n3', 
          type: 'ODBC procedure', 
          name: 'test procedure',
          connection: 'n1',
          catalog: '',
          schema: '',
          procedure: 'MYPROC'
        }
      ];
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n3 = helper.getNode('n3');
        (n3.catalog === null).should.be.true;
        (n3.schema === null).should.be.true;
        done();
      });
    });

    it('should parse parameters from JSON string', function(done) {
      const flow = [
        { 
          id: 'n1', 
          type: 'ODBC pool', 
          name: 'test pool',
          connectionString: 'DSN=test'
        },
        { 
          id: 'n3', 
          type: 'ODBC procedure', 
          name: 'test procedure',
          connection: 'n1',
          procedure: 'MYPROC',
          parameters: '[1, 2, "test"]'
        }
      ];
      helper.load(function(RED) {
        odbcNode(RED);
      }, flow, function() {
        const n3 = helper.getNode('n3');
        n3.parameters.should.be.an('array');
        n3.parameters.should.deep.equal([1, 2, 'test']);
        done();
      });
    });
  });
});

