# Default to the GBIF internal "third-party" mirror used in CI/CD.
# Override for local builds: docker build --build-arg BASE_REGISTRY=docker.io ...
ARG BASE_REGISTRY=third-party

# Build stage
FROM ${BASE_REGISTRY}/maven:3-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
# -U forces a fresh check of -SNAPSHOT dependencies so the build picks up
# new bytecode pushed to Nexus even when the version string is unchanged.
RUN --mount=type=cache,target=/root/.m2 mvn -pl crawler-cli -am -DskipTests -U package

# Run stage
FROM ${BASE_REGISTRY}/eclipse-temurin:17-jre
LABEL authors="gbif"

# Non-root user inside the image. Defaults suit typical local builds; GBIF dev NFS expects
# uid/gid 2001 — build with: --build-arg CRAWLER_UID=2001 --build-arg CRAWLER_GID=2001
# so this matches Helm podSecurityContext when using shared storage (e.g. GBIF dev NFS uid/gid 2001).
ARG CRAWLER_GID=1000
ARG CRAWLER_UID=1000
RUN groupadd --system --gid ${CRAWLER_GID} crawler && useradd --system --uid ${CRAWLER_UID} --gid ${CRAWLER_GID} --no-create-home --shell /bin/false crawler
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
