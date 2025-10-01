module.exports = {
    apps: [{
      name: 'carbon-backend',
      script: 'dist/src/main.js', // Direct path to compiled JS
      cwd: process.cwd(), // Use current working directory
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: 'production',
        TZ: 'UTC',
        DATABASE_URL: process.env.DATABASE_URL,
        HTTPS: 'true',
        TRUST_PROXY: 'true'
      },
      env_production: {
        NODE_ENV: 'production',
        TZ: 'UTC',
        DATABASE_URL: process.env.DATABASE_URL,
        HTTPS: 'true',
        TRUST_PROXY: 'true'
      },
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      kill_timeout: 5000,
      wait_ready: true,
      listen_timeout: 10000,
      reload_delay: 1000,
    }]
  };
