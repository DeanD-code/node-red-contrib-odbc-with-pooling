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
  const process = require('process');

  function odbcPool(config) {
    RED.nodes.createNode(this, config);

    // Build poolConfig object, converting empty strings to undefined and
    // converting numeric strings to numbers. If values are undefined, 
    // odbc.pool will set them to defaults during its execution.
    this.poolConfig = {
      connectionString: config.connectionString
    };
    
    // Handle numeric pool settings
    const numericSettings = ['initialSize', 'incrementSize', 'maxSize', 'connectionTimeout', 'loginTimeout'];
    numericSettings.forEach(setting => {
      if (config[setting] !== undefined && config[setting] !== null && config[setting] !== '') {
        const numValue = Number(config[setting]);
        if (!isNaN(numValue)) {
          this.poolConfig[setting] = numValue;
        }
      }
    });
    
    // Handle boolean setting
    if (config.shrinkPool !== undefined && config.shrinkPool !== null) {
      this.poolConfig.shrinkPool = config.shrinkPool;
    }

    // Store CloseConnectionIdleTime separately for idle timeout handling (in seconds)
    this.closeConnectionIdleTime = undefined;
    if (config.closeConnectionIdleTime !== undefined && config.closeConnectionIdleTime !== null && config.closeConnectionIdleTime !== '') {
      const numValue = Number(config.closeConnectionIdleTime);
      if (!isNaN(numValue) && numValue > 0) {
        this.closeConnectionIdleTime = numValue * 1000; // Convert to milliseconds
      }
    }
    
    this.pool = null;
    this.connecting = false;
    this.activeConnections = new Map(); // Track connections and their last usage time
    this.cleanupInterval = null;

    // Cleanup function to close idle connections
    this.cleanupIdleConnections = () => {
      if (!this.pool || !this.closeConnectionIdleTime) {
        return;
      }

      const now = Date.now();
      const connectionsToClose = [];

      // Check all active connections
      for (const [connection, lastUsed] of this.activeConnections.entries()) {
        const idleTime = now - lastUsed;
        if (idleTime >= this.closeConnectionIdleTime) {
          connectionsToClose.push(connection);
        }
      }

      // Close idle connections
      connectionsToClose.forEach(connection => {
        try {
          if (connection && typeof connection.close === 'function') {
            connection.close().catch(err => {
              // Ignore errors when closing idle connections
            });
          }
          this.activeConnections.delete(connection);
        } catch (error) {
          // Ignore errors when closing idle connections
          this.activeConnections.delete(connection);
        }
      });
    };

    // Start cleanup interval if closeConnectionIdleTime is set
    if (this.closeConnectionIdleTime) {
      // Check every second for idle connections
      this.cleanupInterval = setInterval(() => {
        this.cleanupIdleConnections();
      }, 1000);
    }

    this.connect = async () => {

      let connection;

      if (this.pool == null) {
        try {
          this.pool = await odbc.pool(this.poolConfig);
          this.connecting = false;
        } catch (error) {
          throw(error);
        }
      }

      try {
        connection = await this.pool.connect();
        // Track this connection and update its last usage time
        const poolNode = this;
        this.activeConnections.set(connection, Date.now());

        // Update last usage time whenever connection is used
        const updateLastUsed = () => {
          if (poolNode.activeConnections.has(connection)) {
            poolNode.activeConnections.set(connection, Date.now());
          }
        };

        // Wrap the connection's close method to remove it from tracking
        const originalClose = connection.close.bind(connection);
        connection.close = async function() {
          // Remove from active connections tracking
          poolNode.activeConnections.delete(connection);
          return originalClose();
        };

        // Wrap common connection methods to update last used time
        if (connection.query) {
          const originalQuery = connection.query.bind(connection);
          connection.query = function(...args) {
            updateLastUsed();
            return originalQuery(...args);
          };
        }

        if (connection.callProcedure) {
          const originalCallProcedure = connection.callProcedure.bind(connection);
          connection.callProcedure = function(...args) {
            updateLastUsed();
            return originalCallProcedure(...args);
          };
        }
      } catch (error) {
        throw(error);
      }

      return connection;
    }

    // Cleanup on node close
    this.on('close', () => {
      if (this.cleanupInterval) {
        clearInterval(this.cleanupInterval);
        this.cleanupInterval = null;
      }
      // Close all active connections
      for (const connection of this.activeConnections.keys()) {
        try {
          if (connection && typeof connection.close === 'function') {
            connection.close().catch(() => {});
          }
        } catch (error) {
          // Ignore errors
        }
      }
      this.activeConnections.clear();
    });
  }
  
  RED.nodes.registerType('ODBC pool', odbcPool);

  function odbcQuery(config) {
    RED.nodes.createNode(this, config);
    this.poolNode = RED.nodes.getNode(config.connection);
    this.queryString = config.query;
    this.outfield = config.outField;
    this.name = config.name;

    this.runQuery = async function(message, send, done) {
      let connection;

      try {
        connection = await this.poolNode.connect();
      } catch (error) {
        if (error) {
          this.error(error);
          this.status({fill: "red", shape: "ring", text: error.message});
          if (done) {
            // Node-RED 1.0 compatible
            done(error);
          } else {
            // Node-RED 0.x compatible
            node.error(error, message);
          }
        }
      }

      this.status({
        fill:"blue",
        shape:"dot",
        text:"querying..."
      });

      let parameters = undefined;
      let result;

      // Check if there is a payload.
      // If yes, obtain the query and/or parameters from the payload
      // If no, use the node's predefined query
      if (message.payload) {

        // If the payload is a string, convert to JSON object and get the query
        // and/or parameters
        if (typeof message.payload == 'string')
        {
          let payloadJSON;
          try {
            // string MUST be valid JSON, else fill with error.
            // TODO: throw error?
            payloadJSON = JSON.parse(message.payload);
          } catch (error) {
            this.status({fill: "red", shape: "ring", text: error.message});
            connection.close();
            if (done) {
              // Node-RED 1.0 compatible
              done(error);
            } else {
              // Node-RED 0.x compatible
              node.error(error, message);
            }
          }
          parameters = payloadJSON.parameters || undefined;
          this.queryString = payloadJSON.query || this.queryString;
        } 
        
        // If the payload is an object, get the query and/or parameters directly
        // from the object
        else if (typeof message.payload == 'object') {
          parameters = message.payload.parameters || undefined;
          this.queryString = message.payload.query || this.queryString;
        }
      }

      try {
        result = await connection.query(this.queryString, parameters);
      } catch (error) {
        // Check if connection was closed (common error messages for closed connections)
        const isConnectionClosed = error.message && (
          error.message.toLowerCase().includes('connection closed') ||
          error.message.toLowerCase().includes('not connected') ||
          error.message.toLowerCase().includes('connection does not exist') ||
          error.message.toLowerCase().includes('invalid connection') ||
          error.code === 'IM001' ||
          error.sqlState === '08003' // Connection does not exist
        );

        if (isConnectionClosed) {
          // Try to get a new connection and retry the query
          try {
            connection.close().catch(() => {}); // Close the old connection
            connection = await this.poolNode.connect();
            result = await connection.query(this.queryString, parameters);
          } catch (retryError) {
            this.error(retryError);
            this.status({fill: "red", shape: "ring", text: retryError.message});
            connection.close().catch(() => {});
            if (done) {
              done(retryError);
            } else {
              node.error(retryError, message);
            }
            return;
          }
        } else {
          this.error(error);
          this.status({fill: "red", shape: "ring", text: error.message});
          connection.close();
          if (done) {
            // Node-RED 1.0 compatible
            done(error);
          } else {
            // Node-RED 0.x compatible
            node.error(error, message);
          }
          return;
        }
      }

      connection.close();

      message.payload = result;
      send(message);
      connection.close();
      this.status({fill:'green',shape:'dot',text:'ready'});
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

      // On initialization, pool will be null. Set connecting to true so that
      // other nodes are immediately blocked, then call runQuery (which will
      // actually do the pool initialization)
      if (this.poolNode.pool == null) {
        this.poolNode.connecting = true;
      }
      await this.runQuery(message, send, done);
    }
    
    this.on('input', this.checkPool);
        
    this.status({fill:'green',shape:'dot',text:'ready'});
  }

  RED.nodes.registerType("ODBC query", odbcQuery);

  function odbcProcedure(config) {
    RED.nodes.createNode(this, config);
    this.poolNode = RED.nodes.getNode(config.connection);
    this.catalog = config.catalog;
    this.schema = config.schema;
    this.procedure = config.procedure;
    this.parameters = config.parameters;
    this.outfield = config.outField;

    // If parameters were passed in through the config, they are a string.
    // Need to convert the string to an actual JavaScript array
    if (this.parameters) {
      try {
        this.parameters = JSON.parse(this.parameters);
      } catch (error) {
        this.status({fill:'red',shape:'ring',text: error.message});
        return;
      }
    }

    // If catalog evaluates to false, convert the value to null, as the
    // odbc connector expects
    if (!this.catalog) {
      this.catalog = null;
    }

    // If schema evaluates to false, convert the value to null, as the
    // odbc connector expects
    if (!this.schema) {
      this.schema = null;
    }

    this.runProcedure = async function(message, send, done) {
      let connection;
      let catalog = this.catalog;
      let schema = this.schema;
      let procedure = this.procedure;
      let parameters = this.parameters;

      try {
        connection = await this.poolNode.connect();
      } catch (error) {
        if (error) {
          this.error(error);
          this.status({fill: "red", shape: "ring", text: error.message});
          if (done) {
            // Node-RED 1.0 compatible
            done(error);
          } else {
            // Node-RED 0.x compatible
            node.error(error, message);
          }
        }
      }

      this.status({
        fill:"blue",
        shape:"dot",
        text:"running procedure..."
      });

      let result;
      const payload = message.payload;

      // Check if there is a payload.
      // If yes, obtain the catalog, schema, table and/or parameters from the 
      // payload
      // If no, use the node's predefined values
      if (payload) {

        // If the payload is a string, convert to JSON object and get the
        // catalog, schema, table, and/or parameters.
        if (typeof payload == 'string')
        {
          let payloadJSON;
          try {
            // string MUST be valid JSON, else fill with error.
            // TODO: throw error?
            payloadJSON = JSON.parse(payload);
          } catch (error) {
            this.status({fill: "red", shape: "ring", text: error.message});
            connection.close();
            if (done) {
              // Node-RED 1.0 compatible
              done(error);
            } else {
              // Node-RED 0.x compatible
              node.error(error, message);
            }
          }
          catalog = payloadJSON.catalog || catalog;
          schema = payloadJSON.schema || schema;
          proceudre = payloadJSON.procedure || procedure;
          parameters = payloadJSON.parameters || parameters;
        } 
        
        // If the payload is an object, get the catalog, schema, table, and/or
        // parameters directly from the object
        else if (typeof payload == 'object') {
          catalog = payload.catalog || catalog;
          schema = payload.schema || schema;
          procedure = payload.procedure || procedure;
          parameters = payload.parameters || parameters;
        }
      }

      try {
        result = await connection.callProcedure(catalog, schema, procedure, parameters);
      } catch (error) {
        // Check if connection was closed (common error messages for closed connections)
        const errorMessage = error.odbcErrors && error.odbcErrors[0] ? error.odbcErrors[0].message : error.message;
        const isConnectionClosed = errorMessage && (
          errorMessage.toLowerCase().includes('connection closed') ||
          errorMessage.toLowerCase().includes('not connected') ||
          errorMessage.toLowerCase().includes('connection does not exist') ||
          errorMessage.toLowerCase().includes('invalid connection') ||
          error.code === 'IM001' ||
          error.sqlState === '08003' // Connection does not exist
        );

        if (isConnectionClosed) {
          // Try to get a new connection and retry the procedure call
          try {
            connection.close().catch(() => {}); // Close the old connection
            connection = await this.poolNode.connect();
            result = await connection.callProcedure(catalog, schema, procedure, parameters);
          } catch (retryError) {
            const retryErrorMessage = retryError.odbcErrors && retryError.odbcErrors[0] ? retryError.odbcErrors[0].message : retryError.message;
            this.error(retryError);
            this.status({fill: "red", shape: "ring", text: retryErrorMessage});
            connection.close().catch(() => {});
            if (done) {
              done(retryError);
            } else {
              node.error(retryError, message);
            }
            return;
          }
        } else {
          this.error(error);
          this.status({fill: "red", shape: "ring", text: errorMessage});
          connection.close();
          if (done) {
            // Node-RED 1.0 compatible
            done(error);
          } else {
            // Node-RED 0.x compatible
            node.error(error, message);
          }
          return;
        }
      }

      connection.close();

      message.payload = result;
      send(message);
      connection.close();
      this.status({fill:'green',shape:'dot',text:'ready'});
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

      // On initialization, pool will be null. Set connecting to true so that
      // other nodes are immediately blocked, then call runProcedure (which will
      // actually do the pool initialization)
      if (this.poolNode.pool == null) {
        this.poolNode.connecting = true;
      }
      await this.runProcedure(message, send, done);
    }
    
    this.on('input', this.runProcedure);
  }

  RED.nodes.registerType("ODBC procedure", odbcProcedure);
}