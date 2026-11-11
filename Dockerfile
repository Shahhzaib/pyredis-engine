FROM python:3.11.13-slim-bookworm@sha256:86adf8dbadc3d6e82ee5dd2c74bec2e1c2467cdad47886280501df722372d2e1 AS build
ENV SOURCE_DATE_EPOCH=1704067200
WORKDIR /build
COPY docker/build-requirements.txt /build/requirements.txt
RUN python -m pip install --no-cache-dir --require-hashes -r requirements.txt
COPY pyproject.toml README.md LICENSE /build/
COPY src /build/src
RUN python -m pip wheel --no-cache-dir --no-deps --no-build-isolation --wheel-dir /wheels .

FROM scratch AS artifacts
COPY --from=build /wheels /

FROM python:3.11.13-slim-bookworm@sha256:86adf8dbadc3d6e82ee5dd2c74bec2e1c2467cdad47886280501df722372d2e1 AS runtime
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYREDIS_HEALTH_PORT=6380
COPY --from=build /wheels /wheels
RUN python -m pip install --no-cache-dir --no-deps /wheels/*.whl \
    && rm -rf /wheels \
    && groupadd --gid 10001 pyredis \
    && useradd --uid 10001 --gid 10001 --no-create-home pyredis \
    && mkdir /data && chown 10001:10001 /data
USER 10001:10001
WORKDIR /data
VOLUME ["/data"]
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD python -c "import os,socket; s=socket.create_connection(('127.0.0.1',int(os.environ['PYREDIS_HEALTH_PORT'])),2); s.close()"
ENTRYPOINT ["pyredis-engine"]
CMD ["--port", "6380"]
