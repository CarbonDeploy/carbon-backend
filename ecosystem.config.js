module.exports = {
    apps: [{
      name: 'carbon-backend',
      script: 'npm',
      args: 'run start:prod',
      cwd: '/root/carbon-backend',
      instances: 1,
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: 'production',
        TZ: 'UTC',
        HTTPS: 'true',  // Add this if your app needs to know it's behind HTTPS
        TRUST_PROXY: 'true'  // Add this for proper IP forwarding
      },
      env_production: {
        NODE_ENV: 'production',
        TZ: 'UTC',
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