#!/usr/bin/env node

/**
 * Post-install script to check for ODBC runtime libraries
 * This provides helpful error messages if ODBC libraries are missing
 */

const fs = require('fs');
const { execSync } = require('child_process');
const os = require('os');
const path = require('path');

function checkODBCLibrary() {
  const platform = os.platform();
  
  console.log('Checking for ODBC runtime libraries...\n');
  
  let libraryFound = false;
  let installCommand = '';
  let errorMessage = '';
  
  switch (platform) {
    case 'linux':
      // Check for libodbc.so.2 or libodbc.so
      try {
        execSync('ldconfig -p | grep -q libodbc', { stdio: 'ignore' });
        libraryFound = true;
      } catch (e) {
        // Try checking common library paths
        const libPaths = [
          '/usr/lib/x86_64-linux-gnu/libodbc.so.2',
          '/usr/lib/libodbc.so.2',
          '/lib/libodbc.so.2',
          '/usr/local/lib/libodbc.so.2'
        ];
        
        for (const libPath of libPaths) {
          if (fs.existsSync(libPath)) {
            libraryFound = true;
            break;
          }
        }
      }
      
      // Detect Linux distribution
      let distro = 'debian';
      try {
        if (fs.existsSync('/etc/os-release')) {
          const osRelease = fs.readFileSync('/etc/os-release', 'utf8');
          if (osRelease.includes('Alpine')) {
            distro = 'alpine';
            installCommand = 'apk add --no-cache unixodbc';
          } else if (osRelease.includes('CentOS') || osRelease.includes('Red Hat') || osRelease.includes('Rocky')) {
            distro = 'rhel';
            installCommand = 'yum install unixODBC  # or: dnf install unixODBC';
          } else {
            installCommand = 'apt-get update && apt-get install -y unixodbc';
          }
        }
      } catch (e) {
        installCommand = 'apt-get update && apt-get install -y unixodbc';
      }
      
      errorMessage = `ODBC runtime library (libodbc.so.2) not found.\n` +
        `Install it using:\n` +
        `  ${installCommand}\n\n` +
        `For Docker containers, add to your Dockerfile:\n` +
        `  RUN ${installCommand}\n\n` +
        `After installing, restart Node-RED.`;
      break;
      
    case 'darwin': // macOS
      try {
        execSync('brew list unixodbc > /dev/null 2>&1', { stdio: 'ignore' });
        libraryFound = true;
      } catch (e) {
        // Check if installed via other means
        const macPaths = [
          '/usr/local/lib/libodbc.dylib',
          '/opt/homebrew/lib/libodbc.dylib'
        ];
        for (const libPath of macPaths) {
          if (fs.existsSync(libPath)) {
            libraryFound = true;
            break;
          }
        }
      }
      installCommand = 'brew install unixodbc';
      errorMessage = `ODBC runtime library not found on macOS.\n` +
        `Install it using:\n` +
        `  ${installCommand}\n\n` +
        `After installing, restart Node-RED.`;
      break;
      
    case 'win32':
      // Windows typically has ODBC drivers installed separately
      // Just warn that they need appropriate drivers
      libraryFound = true; // Don't fail on Windows, drivers are installed separately
      console.log('Windows detected - ensure you have the appropriate ODBC driver installed for your database.');
      console.log('Drivers are typically installed from the database vendor\'s website.\n');
      return;
      
    default:
      console.log(`Platform ${platform} detected. Please ensure ODBC runtime libraries are installed.\n`);
      return;
  }
  
  if (!libraryFound) {
    console.error('⚠️  WARNING: ODBC runtime library not found!\n');
    console.error(errorMessage);
    console.error('\nThe package will be installed, but Node-RED will fail to load this node');
    console.error('until the ODBC libraries are installed.\n');
    console.error('For more information, see: https://github.com/DeanD-code/node-red-contrib-odbc-with-pooling\n');
    
    // Don't fail the install, just warn
    // The actual error will show when Node-RED tries to load the node
  } else {
    console.log('✓ ODBC runtime library found. Package should work correctly.\n');
  }
}

// Run the check
try {
  checkODBCLibrary();
} catch (error) {
  // Don't fail the install if the check itself fails
  console.warn('Warning: Could not verify ODBC library installation:', error.message);
  console.warn('Package installed, but please ensure ODBC runtime libraries are available.\n');
}

