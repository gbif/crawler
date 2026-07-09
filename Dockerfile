# Default to the GBIF internal "third-party" mirror used in CI/CD.
# Override for local builds: docker build --build-arg BASE_REGISTRY=docker.io ...
ARG BASE_REGISTRY=third-party
ARG MAVEN_UPDATE_SNAPSHOTS=false

# Build stage
FROM ${BASE_REGISTRY}/maven:3-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .

RUN --mount=type=cache,target=/root/.m2 \
    if [ "$MAVEN_UPDATE_SNAPSHOTS" = "true" ]; then \
      mvn -pl crawler-cli -am -DskipTests -U package; \
    else \
      mvn -pl crawler-cli -am -DskipTests package; \
    fi

# Run stage
FROM ${BASE_REGISTRY}/eclipse-temurin:17-jre
LABEL authors="gbif"

# Reuse existing group/user if UID/GID 1000 already exists in the base image
# (e.g. Ubuntu 24.04+ ships a default 'ubuntu' user/group at 1000). We keep
# 1000:1000 fixed to match GBIF dev NFS mount ownership, so we rename rather
# than fail — renaming is safe since nothing else depends on the old name.
ARG CRAWLER_GID=1000
ARG CRAWLER_UID=1000
RUN if getent group ${CRAWLER_GID} > /dev/null; then \
        groupmod -n crawler $(getent group ${CRAWLER_GID} | cut -d: -f1); \
    else \
        groupadd --system --gid ${CRAWLER_GID} crawler; \
    fi && \
    if id -u ${CRAWLER_UID} > /dev/null 2>&1; then \
        usermod -l crawler -g ${CRAWLER_GID} -s /bin/false $(getent passwd ${CRAWLER_UID} | cut -d: -f1); \
    else \
        useradd --system --uid ${CRAWLER_UID} --gid ${CRAWLER_GID} --no-create-home --shell /bin/false crawler; \
    fi
WORKDIR /app

# Runtime dirs are writable by the non-root user for generated config, mounted logback config and archives.
RUN mkdir -p /app/.tmp /app/config /data \
    && chown -R crawler /app /data

COPY --chown=crawler --from=build /build/crawler-cli/target/crawler-cli.jar /app/crawler-cli.jar
COPY --chown=crawler scripts/entrypoint-crawler-cli.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# -Xmx is omitted so MaxRAMPercentage tracks the actual container memory limit set by Helm/Kubernetes.
ENV JVM_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Xms256m" \
    CRAWLER_COMMAND="dwcdp-metasync" \
    CRAWLER_CONF="/app/.tmp/crawler.yaml"

# Match the GBIF service pattern by running the app without root privileges.
USER crawler

ENTRYPOINT ["/app/entrypoint.sh"]
