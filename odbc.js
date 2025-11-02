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

    // Set to track connections that should be closed when checked out
    this.connectionsToClose = new Set();

    // Cleanup function to close idle connections
    this.cleanupIdleConnections = async () => {
      if (!this.pool || !this.closeConnectionIdleTime) {
        if (!this.pool) {
          console.log('[ODBC Pool] Cleanup skipped: pool is null');
        }
        if (!this.closeConnectionIdleTime) {
          console.log('[ODBC Pool] Cleanup skipped: closeConnectionIdleTime not set');
        }
        return;
      }

      const now = Date.now();
      const idleConnections = [];
      const totalTracked = this.activeConnections.size;

      console.log(`[ODBC Pool] Cleanup running: ${totalTracked} connections tracked, timeout=${this.closeConnectionIdleTime}ms`);

      // Check all tracked connections to find idle ones
      for (const [connection, lastUsed] of this.activeConnections.entries()) {
        const idleTime = now - lastUsed;
        const idleSeconds = (idleTime / 1000).toFixed(2);
        console.log(`[ODBC Pool] Connection check: idle=${idleSeconds}s, threshold=${(this.closeConnectionIdleTime/1000).toFixed(2)}s`);
        
        if (idleTime >= this.closeConnectionIdleTime) {
          console.log(`[ODBC Pool] Found idle connection: idle=${idleSeconds}s (threshold=${(this.closeConnectionIdleTime/1000).toFixed(2)}s)`);
          idleConnections.push(connection);
        }
      }
      
      if (idleConnections.length > 0) {
        console.log(`[ODBC Pool] Processing ${idleConnections.length} idle connection(s)`);
      } else {
        console.log(`[ODBC Pool] No idle connections found`);
      }

      // For each idle connection:
      // 1. Mark it for closure (so it gets closed when checked out)
      // 2. Try to close it if it's currently checked out
      // 3. Try to actively check it out from pool and close it
      for (const trackedConnection of idleConnections) {
        try {
          console.log(`[ODBC Pool] Processing idle connection, marking for closure`);
          // Mark for closure
          this.connectionsToClose.add(trackedConnection);
          console.log(`[ODBC Pool] Marked connection for closure. Total marked: ${this.connectionsToClose.size}`);
          
          // Try to close if currently checked out
          if (trackedConnection && typeof trackedConnection.close === 'function') {
            try {
              console.log(`[ODBC Pool] Attempting to close idle connection (might be checked out)`);
              await trackedConnection.close();
              // Successfully closed (was checked out)
              console.log(`[ODBC Pool] Successfully closed idle connection (was checked out)`);
              this.activeConnections.delete(trackedConnection);
              this.connectionsToClose.delete(trackedConnection);
            } catch (err) {
              console.log(`[ODBC Pool] Could not close connection directly: ${err.message}. Connection likely in pool.`);
              // Connection is in pool - try to check it out and close it
              // This is a workaround: we check out a connection to see if it's our idle one
              try {
                console.log(`[ODBC Pool] Attempting to check out connection from pool to close idle one`);
                const testConnection = await this.pool.connect();
                // Check if this is the idle connection
                if (testConnection === trackedConnection || 
                    this.activeConnections.has(testConnection)) {
                  const testLastUsed = this.activeConnections.get(testConnection);
                  const testIdleTime = testLastUsed ? now - testLastUsed : 0;
                  console.log(`[ODBC Pool] Checked out connection, idle time: ${(testIdleTime/1000).toFixed(2)}s`);
                  if (testLastUsed && testIdleTime >= this.closeConnectionIdleTime) {
                    // This connection is idle, close it
                    console.log(`[ODBC Pool] Closing idle connection from pool`);
                    await testConnection.close();
                    this.activeConnections.delete(testConnection);
                    this.connectionsToClose.delete(testConnection);
                    console.log(`[ODBC Pool] Successfully closed idle connection from pool`);
                  } else {
                    // Not idle, return it
                    console.log(`[ODBC Pool] Connection not idle, returning to pool`);
                    await testConnection.close();
                  }
                } else {
                  // Different connection, return it
                  console.log(`[ODBC Pool] Different connection checked out, returning to pool`);
                  await testConnection.close();
                }
              } catch (checkoutErr) {
                console.log(`[ODBC Pool] Could not check out connection from pool: ${checkoutErr.message}`);
                // Couldn't check out - pool might be busy, that's okay
              }
            }
          } else {
            console.log(`[ODBC Pool] Connection object invalid or missing close method`);
          }
        } catch (error) {
          console.log(`[ODBC Pool] Error processing idle connection: ${error.message}`);
        }
      }
      console.log(`[ODBC Pool] Cleanup completed. Remaining tracked: ${this.activeConnections.size}, marked for closure: ${this.connectionsToClose.size}`);
    };

    this.connect = async () => {

      let connection;

      // If pool is null, create a new one
      // This handles the case where the flow was restarted or pool was closed
      if (this.pool == null) {
        // Clear any stale connection tracking
        this.activeConnections.clear();
        
        try {
          this.pool = await odbc.pool(this.poolConfig);
          this.connecting = false;
        } catch (error) {
          // If pool creation fails, ensure state is clean
          this.pool = null;
          this.connecting = false;
          throw(error);
        }
      }
      
      // Ensure cleanup interval is started if closeConnectionIdleTime is set
      // (in case pool was created before or interval was cleared)
      if (this.closeConnectionIdleTime && !this.cleanupInterval) {
        // Check every second for idle connections
        // Use a shorter interval for more responsive cleanup
        const checkInterval = Math.min(1000, this.closeConnectionIdleTime / 2);
        console.log(`[ODBC Pool] Starting cleanup interval: ${checkInterval}ms, timeout=${this.closeConnectionIdleTime}ms (${(this.closeConnectionIdleTime/1000).toFixed(2)}s)`);
        this.cleanupInterval = setInterval(() => {
          // Call async function without await (fire and forget)
          this.cleanupIdleConnections().catch((err) => {
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
      
      // Track this connection and update its last usage time
      const poolNode = this;
      let now = Date.now();
      
      console.log(`[ODBC Pool] connect() called. Tracked connections: ${poolNode.activeConnections.size}, Marked for closure: ${poolNode.connectionsToClose.size}`);
      
      // Check if this connection should be closed due to idle timeout
      let shouldClose = false;
      let closeReason = '';
      
      if (poolNode.connectionsToClose.has(connection)) {
        // Connection was marked for closure
        shouldClose = true;
        closeReason = 'marked for closure';
        console.log(`[ODBC Pool] Connection marked for closure`);
      } else if (poolNode.activeConnections.has(connection)) {
        // Connection was previously tracked, check if it's idle too long
        const lastUsed = poolNode.activeConnections.get(connection);
        const idleTime = now - lastUsed;
        const idleSeconds = (idleTime / 1000).toFixed(2);
        console.log(`[ODBC Pool] Previously tracked connection, idle: ${idleSeconds}s`);
        
        if (poolNode.closeConnectionIdleTime && idleTime >= poolNode.closeConnectionIdleTime) {
          shouldClose = true;
          closeReason = `idle for ${idleSeconds}s`;
          console.log(`[ODBC Pool] Connection idle too long: ${idleSeconds}s >= ${(poolNode.closeConnectionIdleTime/1000).toFixed(2)}s`);
        } else {
          console.log(`[ODBC Pool] Connection still valid (idle ${idleSeconds}s < ${(poolNode.closeConnectionIdleTime/1000).toFixed(2)}s)`);
        }
      } else {
        console.log(`[ODBC Pool] New connection, adding to tracking`);
      }
      
      if (shouldClose) {
        // Connection was idle too long, close it and get a new one
        console.log(`[ODBC Pool] Closing connection (${closeReason}) and getting new one`);
        try {
          await connection.close();
          console.log(`[ODBC Pool] Old connection closed successfully`);
        } catch (e) {
          console.log(`[ODBC Pool] Error closing old connection: ${e.message}`);
          // Ignore close errors - connection might already be closed or in pool
        }
        poolNode.activeConnections.delete(connection);
        poolNode.connectionsToClose.delete(connection);
        // Get a new connection and track it
        console.log(`[ODBC Pool] Getting new connection from pool`);
        connection = await poolNode.pool.connect();
        // Update tracking for the new connection
        now = Date.now();
        poolNode.activeConnections.set(connection, now);
        console.log(`[ODBC Pool] New connection tracked at ${new Date(now).toISOString()}`);
      } else if (poolNode.activeConnections.has(connection)) {
        // Connection is still valid, update its last used time
        poolNode.activeConnections.set(connection, now);
        console.log(`[ODBC Pool] Updated lastUsed for existing connection: ${new Date(now).toISOString()}`);
        // Remove from close list if it was there (connection was reused before timeout)
        if (poolNode.connectionsToClose.has(connection)) {
          poolNode.connectionsToClose.delete(connection);
          console.log(`[ODBC Pool] Removed connection from close list (reused before timeout)`);
        }
      } else {
        // New connection, track it
        this.activeConnections.set(connection, now);
        console.log(`[ODBC Pool] New connection tracked at ${new Date(now).toISOString()}`);
      }

      // Update last usage time whenever connection is used
      const updateLastUsed = () => {
        if (poolNode.activeConnections.has(connection)) {
          const newTime = Date.now();
          poolNode.activeConnections.set(connection, newTime);
          console.log(`[ODBC Pool] Updated lastUsed for connection (used): ${new Date(newTime).toISOString()}`);
        } else {
          console.log(`[ODBC Pool] Warning: updateLastUsed called but connection not in tracking`);
        }
      };

        // Wrap the connection's close method - DON'T remove from tracking
        // We keep tracking even after close() so we can close idle connections
        // IMPORTANT: Don't update lastUsed when closing - keep the last usage time
        // so we can track how long it's been idle in the pool
        const originalClose = connection.close.bind(connection);
        connection.close = async function() {
          const lastUsed = poolNode.activeConnections.get(connection);
          const lastUsedDate = lastUsed ? new Date(lastUsed).toISOString() : 'unknown';
          console.log(`[ODBC Pool] Connection.close() called. Last used: ${lastUsedDate}. Keeping timestamp for idle tracking.`);
          // Don't update lastUsed here - keep the timestamp from when it was last used
          // This allows the cleanup to detect idle connections
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

      return connection;
    }

    // Method to close and reset the pool
    this.closePool = async () => {
      if (this.cleanupInterval) {
        clearInterval(this.cleanupInterval);
        this.cleanupInterval = null;
      }
      
      // Close all tracked connections
      for (const connection of this.activeConnections.keys()) {
        try {
          if (connection && typeof connection.close === 'function') {
            await connection.close().catch(() => {});
          }
        } catch (error) {
          // Ignore errors
        }
      }
      this.activeConnections.clear();
      this.connectionsToClose.clear();
      
      // Try to close the pool if it has a close method
      if (this.pool) {
        try {
          if (typeof this.pool.close === 'function') {
            await this.pool.close().catch(() => {});
          } else if (typeof this.pool.disconnect === 'function') {
            await this.pool.disconnect().catch(() => {});
          }
        } catch (error) {
          // Ignore errors - pool might not have a close method
        }
      }
      
      // Reset pool state
      this.pool = null;
      this.connecting = false;
    };

    // Cleanup on node close
    this.on('close', () => {
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