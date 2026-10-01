# A Camel/Java connector sidecar. Build from the repository's root (the
# protocol lives in proto/):
#   docker build -f connectors/Dockerfile --build-arg CONNECTOR=sap-ecc -t turgon-connector-sap-ecc .
FROM gradle:8.14.3-jdk21 AS build
ARG CONNECTOR=sap-ecc
WORKDIR /src
COPY proto/ proto/
COPY connectors/ connectors/
WORKDIR /src/connectors
RUN gradle --no-daemon -q ":${CONNECTOR}:installDist" \
    && mv "${CONNECTOR}/build/install/turgon-connector-${CONNECTOR}" /out
WORKDIR /out/native
RUN jar xf /out/lib/grpc-netty-shaded-*.jar META-INF/native \
    && mv META-INF/native/*epoll*.so . && rm -r META-INF

FROM gcr.io/distroless/java21-debian12:nonroot
COPY --from=build /out/lib/ /opt/turgon/connector/
# Netty's epoll library (the Unix socket), loaded from here rather than
# extracted to /tmp, which may be mounted noexec.
COPY --from=build /out/native/ /opt/turgon/native/
# Licensed libraries the image cannot ship (SAP JCo: sapjco3.jar and
# libsapjco3.so) are mounted here.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "-Djava.library.path=/opt/turgon/native:/opt/turgon/lib", "-cp", "/opt/turgon/connector/*:/opt/turgon/lib/*", "dev.turgon.connector.sdk.ConnectorServer"]
