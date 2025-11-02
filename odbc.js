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
    this.poolLastUsed = null; // Track when pool was last used (last connection usage)
    this.cleanupInterval = null;
    this.poolClosedDueToIdle = false; // Flag to track if pool was closed due to idle timeout

    // Cleanup function to close pool if idle
    this.cleanupIdlePool = async () => {
      if (!this.closeConnectionIdleTime) {
        return;
      }

      // If pool is already null or closed, nothing to do
      if (!this.pool) {
        return;
      }

      // If poolLastUsed is null, pool was never used
      if (!this.poolLastUsed) {
        return;
      }

      const now = Date.now();
      const idleTime = now - this.poolLastUsed;
      const idleSeconds = (idleTime / 1000).toFixed(2);
      const thresholdSeconds = (this.closeConnectionIdleTime / 1000).toFixed(2);

      console.log(`[ODBC Pool] Cleanup check: Pool idle for ${idleSeconds}s (threshold: ${thresholdSeconds}s)`);

      if (idleTime >= this.closeConnectionIdleTime) {
        console.log(`[ODBC Pool] Pool has been idle for ${idleSeconds}s, closing pool`);
        try {
          await this.closePool();
          this.poolClosedDueToIdle = true;
          this.poolLastUsed = null;
          console.log(`[ODBC Pool] Pool closed due to idle timeout`);
        } catch (error) {
          console.log(`[ODBC Pool] Error closing idle pool: ${error.message}`);
        }
      }
    };

    this.connect = async () => {

      let connection;

      // If pool is null, create a new one
      // This handles the case where the flow was restarted or pool was closed due to idle
      if (this.pool == null) {
        if (this.poolClosedDueToIdle) {
          console.log(`[ODBC Pool] Pool was closed due to idle timeout, recreating pool`);
          this.poolClosedDueToIdle = false;
        } else {
          console.log(`[ODBC Pool] Creating new pool`);
        }
        
        try {
          this.pool = await odbc.pool(this.poolConfig);
          this.connecting = false;
          console.log(`[ODBC Pool] Pool created successfully`);
        } catch (error) {
          // If pool creation fails, ensure state is clean
          this.pool = null;
          this.connecting = false;
          console.log(`[ODBC Pool] Pool creation failed: ${error.message}`);
          throw(error);
        }
      }
      
      // Ensure cleanup interval is started if closeConnectionIdleTime is set
      // (in case pool was created before or interval was cleared)
      if (this.closeConnectionIdleTime && !this.cleanupInterval) {
        // Check every second for idle pool
        const checkInterval = 1000;
        console.log(`[ODBC Pool] Starting cleanup interval: ${checkInterval}ms, timeout=${this.closeConnectionIdleTime}ms (${(this.closeConnectionIdleTime/1000).toFixed(2)}s)`);
        this.cleanupInterval = setInterval(() => {
          // Call async function without await (fire and forget)
          this.cleanupIdlePool().catch((err) => {
            console.log(`[ODBC Pool] Cleanup interval error: ${err.message}`);
          });
        }, checkInterval);
      }

      try {
        connection = await this.pool.connect();
      } catch (connectError) {
        // If connection fails, pool might be invalid - reset and retry
        const errorMessage = connectError.message || '';
        if (errorMessage.includes('closed') || errorMessage.includes('invalid') || 
            connectError.code === 'IM001' || connectError.sqlState === '08003') {
          // Pool appears invalid, reset it
          console.log(`[ODBC Pool] Pool connection failed, recreating pool`);
          try {
            await this.closePool();
            this.pool = await odbc.pool(this.poolConfig);
            connection = await this.pool.connect();
          } catch (retryError) {
            throw(retryError);
          }
        } else {
          throw(connectError);
        }
      }
      
      // Update pool last used time whenever a connection is obtained
      // This tracks when the pool was last actively used
      this.poolLastUsed = Date.now();
      console.log(`[ODBC Pool] Connection obtained, pool last used updated: ${new Date(this.poolLastUsed).toISOString()}`);
      
      // Wrap connection methods to update pool last used time when actually used
      const poolNode = this;
      const updatePoolLastUsed = () => {
        const newTime = Date.now();
        poolNode.poolLastUsed = newTime;
        console.log(`[ODBC Pool] Pool activity: last used updated to ${new Date(newTime).toISOString()}`);
      };

      // Wrap common connection methods to update pool last used time
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

      return connection;
    }

    // Method to close and reset the pool
    this.closePool = async () => {
      console.log(`[ODBC Pool] Closing pool`);
      
      // Try to close the pool if it has a close method
      if (this.pool) {
        try {
          if (typeof this.pool.close === 'function') {
            await this.pool.close().catch(() => {});
            console.log(`[ODBC Pool] Pool.close() called`);
          } else if (typeof this.pool.disconnect === 'function') {
            await this.pool.disconnect().catch(() => {});
            console.log(`[ODBC Pool] Pool.disconnect() called`);
          } else {
            console.log(`[ODBC Pool] Pool has no close/disconnect method, setting to null`);
          }
        } catch (error) {
          console.log(`[ODBC Pool] Error closing pool: ${error.message}`);
          // Ignore errors - pool might not have a close method
        }
      }
      
      // Reset pool state
      this.pool = null;
      this.connecting = false;
      console.log(`[ODBC Pool] Pool closed and reset`);
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