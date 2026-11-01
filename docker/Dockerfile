FROM python:3.12.14-slim-trixie@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f

ENV PYTHONUNBUFFERED=1 PYSPARK_PYTHON=/usr/local/bin/python SPARK_LOCAL_IP=127.0.0.1
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless=21.0.13+9-1~deb13u2 ca-certificates && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname $(dirname $(readlink -f $(which java))))" /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /opt/kinda
COPY requirements.lock ./
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY docker/ivysettings.xml ./
COPY scripts/resolve_connector.py ./
RUN python resolve_connector.py
COPY maven.lock.json scripts/check_dependency_locks.py ./
RUN python check_dependency_locks.py
COPY pyproject.toml ./
COPY contracts ./contracts
COPY migrations ./migrations
COPY src ./src
RUN pip install --no-deps --no-build-isolation . \
    && useradd -u 10001 -m kinda && mkdir -p /checkpoints && chown kinda:kinda /checkpoints
COPY tests ./tests
USER kinda
CMD ["kinda", "--help"]
