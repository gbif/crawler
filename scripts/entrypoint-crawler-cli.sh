#!/bin/sh
set -eu

JVM_OPTIONS="${JVM_OPTIONS:--XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Xms256m -Xmx512m}"
CRAWLER_COMMAND="${CRAWLER_COMMAND:-dwcdp-metasync}"
CRAWLER_CONF="${CRAWLER_CONF:-/app/.tmp/crawler.yaml}"
INCLUDE_MESSAGING="${INCLUDE_MESSAGING:-true}"
INCLUDE_ZOOKEEPER="${INCLUDE_ZOOKEEPER:-true}"
INCLUDE_REGISTRY="${INCLUDE_REGISTRY:-true}"

if [ "$#" -gt 0 ]; then
  # Allow explicit command/flags for debugging or one-off runs.
  exec sh -c "exec java ${JVM_OPTIONS} -jar /app/crawler-cli.jar \"$@\"" -- "$@"
fi

if [ ! -f "$CRAWLER_CONF" ]; then
  : > "$CRAWLER_CONF"

  # The CLI expects YAML config, while Helm injects common connection details as env vars.
  if [ "$INCLUDE_MESSAGING" = "true" ]; then
    cat >> "$CRAWLER_CONF" <<EOF
messaging:
  host: ${RABBIT_HOST:-localhost}
  port: ${RABBIT_PORT:-5672}
  virtualHost: ${RABBIT_VHOST:-/}
  username: ${RABBIT_USER:-guest}
  password: ${RABBIT_PASSWORD:-guest}

EOF
  fi

  if [ "$INCLUDE_ZOOKEEPER" = "true" ]; then
    cat >> "$CRAWLER_CONF" <<EOF
zooKeeper:
  connectionString: ${ZOOKEEPER_CONNECTION_STRING:-localhost:2181}
  namespace: ${ZOOKEEPER_NAMESPACE:-crawler}
  sleepTime: ${ZOOKEEPER_SLEEP_TIME:-1000}
  maxRetries: ${ZOOKEEPER_MAX_RETRIES:-3}

EOF
  fi

  if [ "$INCLUDE_REGISTRY" = "true" ]; then
    cat >> "$CRAWLER_CONF" <<EOF
registry:
  wsUrl: ${REGISTRY_WS_URL:-http://localhost:8080}
  user: ${REGISTRY_USER:-}
  password: ${REGISTRY_PASSWORD:-}

EOF
  fi

  if [ -n "${CRAWLER_EXTRA_CONFIG_YAML:-}" ]; then
    printf '%s\n' "$CRAWLER_EXTRA_CONFIG_YAML" >> "$CRAWLER_CONF"
  fi
fi

# Standard deployment path: command and generated config come from env.
exec sh -c "exec java ${JVM_OPTIONS} -jar /app/crawler-cli.jar ${CRAWLER_COMMAND} --conf ${CRAWLER_CONF}"
