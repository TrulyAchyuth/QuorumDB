# QuorumDB node image. Build from the repository root:
#   docker build -f docker/Dockerfile -t quorumdb:local .
# The official Gradle image ships the same Gradle version as the wrapper (9.9.1), so the build does not
# depend on downloading a distribution at build time.
FROM gradle:9.9.1-jdk21 AS build
WORKDIR /src
# Dependency layer first so source edits do not re-download libraries.
COPY settings.gradle build.gradle gradle.properties ./
COPY core/build.gradle core/
COPY simulation/build.gradle simulation/
COPY backend/build.gradle backend/
COPY benchmarks/build.gradle benchmarks/
RUN gradle --no-daemon -q :backend:dependencies > /dev/null
COPY proto proto
COPY core/src core/src
COPY simulation/src simulation/src
COPY backend/src backend/src
RUN gradle --no-daemon -q :backend:bootJar -x test

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /var/lib/quorumdb quorumdb \
    && mkdir -p /var/lib/quorumdb && chown quorumdb /var/lib/quorumdb
COPY --from=build /src/backend/build/libs/quorumdb-server.jar /opt/quorumdb/quorumdb-server.jar
USER quorumdb
VOLUME /var/lib/quorumdb
ENV QUORUMDB_DATADIR=/var/lib/quorumdb \
    SERVER_PORT=8080 \
    QUORUMDB_GRPCPORT=9100 \
    JAVA_OPTS="-Xmx512m"
EXPOSE 8080 9100
HEALTHCHECK --interval=5s --timeout=2s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8080/api/v1/health > /dev/null || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /opt/quorumdb/quorumdb-server.jar"]
