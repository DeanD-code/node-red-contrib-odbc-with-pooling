// Copyright 2020 IBM

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module.exports = function(RED) {
  const odbc = require('odbc');

  // Helper function to handle Node-RED errors (1.0 and 0.x compatible)
  function handleNodeError(node, error, message, done) {
    node.error(error);
    node.status({fill: "red", shape: "ring", text: error.message || 'Error'});
    if (done) {
      done(error);
    } else {
      node.error(error, message);
    }
  }

  // Helper function to check if connection error indicates closed connection
  function isConnectionClosedError(error) {
    const errorMessage = (error.odbcErrors && error.odbcErrors[0] 
      ? error.odbcErrors[0].message 
      : error.message) || '';
    
    return errorMessage && (
      errorMessage.toLowerCase().includes('connection closed') ||
      errorMessage.toLowerCase().includes('not connected') ||
      errorMessage.toLowerCase().includes('connection does not exist') ||
      errorMessage.toLowerCase().includes('invalid connection') ||
      error.code === 'IM001' ||
      error.sqlState === '08003'
    );
  }

  // Helper function to parse payload (supports string JSON or object)
  function parsePayload(payload) {
    if (!payload) {
      return null;
    }

    if (typeof payload === 'string') {
      try {
        return JSON.parse(payload);
      } catch (error) {
        throw error; // Re-throw for caller to handle
      }
    }

    if (typeof payload === 'object') {
      return payload;
    }

    return null;
  }

  // Helper function to wrap connection methods to track pool activity
  function wrapConnectionMethods(connection, poolNode) {
    const updatePoolLastUsed = () => {
      poolNode.poolLastUsed = Date.now();
    };

    if (connection.query) {
      const originalQuery = connection.query.bind(connection);
      connection.query = function(...args) {
        updatePoolLastUsed();
        return originalQuery(...args);
      };
    }

    if (connection.callProcedure) {
      const originalCallProcedure = connection.callProcedure.bind(connection);
      connection.callProcedure = function(...args) {
        updatePoolLastUsed();
        return originalCallProcedure(...args);
      };
    }

    // Wrap close to decrement active connection count safely
    if (connection.close) {
      const originalClose = connection.close.bind(connection);
      let hasClosed = false;
      connection.close = async function(...args) {
        if (hasClosed) {
          return originalClose(...args);
        }
        hasClosed = true;
        try {
          return await originalClose(...args);
        } finally {
          if (typeof poolNode.poolActiveConnections === 'number') {
            poolNode.poolActiveConnections = Math.max(0, poolNode.poolActiveConnections - 1);
          }
        }
      };
    }

    return connection;
  }

  function odbcPool(config) {
    RED.nodes.createNode(this, config);

    // Build poolConfig object
    this.poolConfig = { connectionString: config.connectionString };
    
    // Handle numeric pool settings
    const numericSettings = ['initialSize', 'incrementSize', 'maxSize', 'connectionTimeout', 'loginTimeout'];
    numericSettings.forEach(setting => {
      const value = config[setting];
      if (value !== undefined && value !== null && value !== '') {
        const numValue = Number(value);
        if (!isNaN(numValue)) {
          this.poolConfig[setting] = numValue;
        }
      }
    });
    
    // Handle boolean setting
    if (config.shrinkPool !== undefined && config.shrinkPool !== null) {
      this.poolConfig.shrinkPool = config.shrinkPool;
    }

    // Store CloseConnectionIdleTime for idle timeout (convert to milliseconds)
    this.closeConnectionIdleTime = undefined;
    if (config.closeConnectionIdleTime !== undefined && 
        config.closeConnectionIdleTime !== null && 
        config.closeConnectionIdleTime !== '') {
      const numValue = Number(config.closeConnectionIdleTime);
      if (!isNaN(numValue) && numValue > 0) {
        this.closeConnectionIdleTime = numValue * 1000;
      }
    }
    
    // Initialize pool state
    this.pool = null;
    this.connecting = false;
    this.poolLastUsed = null;
    this.cleanupInterval = null;
    this.poolClosedDueToIdle = false;
    this.poolActiveConnections = 0;

    // Cleanup function to close pool if idle
    this.cleanupIdlePool = async () => {
      if (!this.closeConnectionIdleTime || !this.pool || !this.poolLastUsed) {
        return;
      }

      const idleTime = Date.now() - this.poolLastUsed;
      if (idleTime >= this.closeConnectionIdleTime) {
        try {
          await this.closePool();
          this.poolClosedDueToIdle = true;
          this.poolLastUsed = null;
        } catch (error) {
          // Ignore errors when closing idle pool
        }
      }
    };

    // Start cleanup interval if closeConnectionIdleTime is set
    this.startCleanupInterval = () => {
      if (this.closeConnectionIdleTime && !this.cleanupInterval) {
        this.cleanupInterval = setInterval(() => {
          this.cleanupIdlePool().catch(() => {});
        }, 1000);
      }
    };

    // Create or get pool
    this.createPool = async () => {
      if (this.pool !== null) {
        return;
      }

      this.poolClosedDueToIdle = false;
      
      try {
        this.pool = await odbc.pool(this.poolConfig);
        this.connecting = false;
      } catch (error) {
        this.pool = null;
        this.connecting = false;
        throw error;
      }
    };

    // Get connection from pool with error recovery
    this.getConnection = async () => {
      try {
        return await this.pool.connect();
      } catch (connectError) {
        // If connection fails, pool might be invalid - reset and retry
        const errorMessage = connectError.message || '';
        if (errorMessage.includes('closed') || errorMessage.includes('invalid') || 
            connectError.code === 'IM001' || connectError.sqlState === '08003') {
          await this.closePool();
          await this.createPool();
          return await this.pool.connect();
        }
        throw connectError;
      }
    };

    // Main connect method
    this.connect = async () => {
      await this.createPool();
      this.startCleanupInterval();

      const connection = await this.getConnection();
      // Track active connections on checkout
      if (typeof this.poolActiveConnections === 'number') {
        this.poolActiveConnections += 1;
      }
      this.poolLastUsed = Date.now();

      return wrapConnectionMethods(connection, this);
    }

    // Close and reset the pool
    this.closePool = async () => {
      if (this.pool) {
        try {
          if (typeof this.pool.close === 'function') {
            await this.pool.close().catch(() => {});
          } else if (typeof this.pool.disconnect === 'function') {
            await this.pool.disconnect().catch(() => {});
          }
        } catch (error) {
          // Ignore errors
        }
      }
      
      this.pool = null;
      this.connecting = false;
      this.poolActiveConnections = 0;
    };

    // Cleanup on node close
    this.on('close', () => {
      if (this.cleanupInterval) {
        clearInterval(this.cleanupInterval);
        this.cleanupInterval = null;
      }
      this.closePool().catch(() => {});
    });
  }
  
  RED.nodes.registerType('odbc-pooling-pool', odbcPool);

  function odbcQuery(config) {
    RED.nodes.createNode(this, config);
    this.poolNode = RED.nodes.getNode(config.connection);
    this.queryString = config.query;
    this.outfield = config.outField;
    this.name = config.name;
    this.activeQueries = 0; // Track number of active queries
    
    const getStatusText = () => {
      const hasPool = !!(this.poolNode && this.poolNode.pool);
      const active = (this.poolNode && typeof this.poolNode.poolActiveConnections === 'number')
        ? this.poolNode.poolActiveConnections
        : 0;
      
      if (!hasPool) {
        return 'idle';
      }
      
      if (this.activeQueries > 0) {
        return `querying (${active})`;
      }
      
      return `ready (${active})`;
    };

    this.runQuery = async function(message, send, done) {
      let connection;
      
      // Increment active queries counter
      this.activeQueries += 1;
      this.status({fill: "blue", shape: "dot", text: getStatusText()});

      try {
        connection = await this.poolNode.connect();
      } catch (error) {
        this.activeQueries -= 1;
        if (error) {
          handleNodeError(this, error, message, done);
        }
        return;
      }

      // Parse payload to get query and parameters
      let parameters = undefined;
      let queryString = this.queryString;

      try {
        const payloadData = parsePayload(message.payload);
        if (payloadData) {
          queryString = payloadData.query || queryString;
          parameters = payloadData.parameters || undefined;
        }
      } catch (error) {
        this.activeQueries -= 1;
        this.status({fill: "red", shape: "ring", text: error.message});
        await connection.close().catch(() => {});
        if (done) {
          done(error);
        } else {
          this.error(error, message);
        }
        return;
      }

      // Execute query with retry on connection closed
      let result;
      try {
        result = await connection.query(queryString, parameters);
      } catch (error) {
        if (isConnectionClosedError(error)) {
          // Retry with new connection
          try {
            await connection.close().catch(() => {});
            connection = await this.poolNode.connect();
            result = await connection.query(queryString, parameters);
          } catch (retryError) {
            this.activeQueries -= 1;
            handleNodeError(this, retryError, message, done);
            await connection.close().catch(() => {});
            return;
          }
        } else {
          this.activeQueries -= 1;
          handleNodeError(this, error, message, done);
          await connection.close().catch(() => {});
          return;
        }
      }

      await connection.close().catch(() => {});
      this.activeQueries -= 1;
      message.payload = result;
      send(message);
      this.status({fill: 'green', shape: 'dot', text: getStatusText()});
      if (done) {
        done();
      }
    }

    this.checkPool = async function(message, send, done) {
      if (this.poolNode.connecting) {
        setTimeout(() => {
          this.checkPool(message, send, done);
        }, 1000);
        return;
      }

      if (this.poolNode.pool == null) {
        this.poolNode.connecting = true;
      }
      await this.runQuery(message, send, done);
    }
    
    this.on('input', this.checkPool);
    this.status({fill: 'green', shape: 'dot', text: getStatusText()});
  }

  RED.nodes.registerType("odbc-pooling-query", odbcQuery);

  function odbcProcedure(config) {
    RED.nodes.createNode(this, config);
    this.poolNode = RED.nodes.getNode(config.connection);
    this.catalog = config.catalog || null;
    this.schema = config.schema || null;
    this.procedure = config.procedure;
    this.outfield = config.outField;
    this.activeQueries = 0; // Track number of active procedures
    
    const getStatusText = () => {
      const hasPool = !!(this.poolNode && this.poolNode.pool);
      const active = (this.poolNode && typeof this.poolNode.poolActiveConnections === 'number')
        ? this.poolNode.poolActiveConnections
        : 0;
      
      if (!hasPool) {
        return 'idle';
      }
      
      if (this.activeQueries > 0) {
        return `querying (${active})`;
      }
      
      return `ready (${active})`;
    };

    // Parse parameters from config if provided
    this.parameters = undefined;
    if (config.parameters) {
      try {
        this.parameters = JSON.parse(config.parameters);
      } catch (error) {
        this.status({fill: 'red', shape: 'ring', text: error.message});
        return;
      }
    }

    this.runProcedure = async function(message, send, done) {
      let connection;
      
      // Increment active queries counter
      this.activeQueries += 1;
      this.status({fill: "blue", shape: "dot", text: getStatusText()});

      try {
        connection = await this.poolNode.connect();
      } catch (error) {
        this.activeQueries -= 1;
        if (error) {
          handleNodeError(this, error, message, done);
        }
        return;
      }

      // Get procedure parameters from payload or config
      let catalog = this.catalog;
      let schema = this.schema;
      let procedure = this.procedure;
      let parameters = this.parameters;

      try {
        const payloadData = parsePayload(message.payload);
        if (payloadData) {
          catalog = payloadData.catalog || catalog;
          schema = payloadData.schema || schema;
          procedure = payloadData.procedure || procedure;
          parameters = payloadData.parameters || parameters;
        }
      } catch (error) {
        this.activeQueries -= 1;
        this.status({fill: "red", shape: "ring", text: error.message});
        await connection.close().catch(() => {});
        if (done) {
          done(error);
        } else {
          this.error(error, message);
        }
        return;
      }

      // Execute procedure with retry on connection closed
      let result;
      try {
        result = await connection.callProcedure(catalog, schema, procedure, parameters);
      } catch (error) {
        if (isConnectionClosedError(error)) {
          // Retry with new connection
          try {
            await connection.close().catch(() => {});
            connection = await this.poolNode.connect();
            result = await connection.callProcedure(catalog, schema, procedure, parameters);
          } catch (retryError) {
            this.activeQueries -= 1;
            const retryErrorMessage = retryError.odbcErrors && retryError.odbcErrors[0] 
              ? retryError.odbcErrors[0].message 
              : retryError.message;
            this.error(retryError);
            this.status({fill: "red", shape: "ring", text: retryErrorMessage});
            await connection.close().catch(() => {});
            if (done) {
              done(retryError);
            } else {
              this.error(retryError, message);
            }
            return;
          }
        } else {
          this.activeQueries -= 1;
          const errorMessage = error.odbcErrors && error.odbcErrors[0] 
            ? error.odbcErrors[0].message 
            : error.message;
          handleNodeError(this, { ...error, message: errorMessage }, message, done);
          await connection.close().catch(() => {});
          return;
        }
      }

      await connection.close().catch(() => {});
      this.activeQueries -= 1;
      message.payload = result;
      send(message);
      this.status({fill: 'green', shape: 'dot', text: getStatusText()});
      if (done) {
        done();
      }
    }

    this.checkPool = async function(message, send, done) {
      if (this.poolNode.connecting) {
        setTimeout(() => {
          this.checkPool(message, send, done);
        }, 1000);
        return;
      }

      if (this.poolNode.pool == null) {
        this.poolNode.connecting = true;
      }
      await this.runProcedure(message, send, done);
    }
    
    this.on('input', this.runProcedure);
    this.status({fill: 'green', shape: 'dot', text: getStatusText()});
  }

  RED.nodes.registerType("odbc-pooling-procedure", odbcProcedure);
}
