FROM python:3.14-slim
WORKDIR /app
# The base image is only as patched as the day it was tagged, and `ignore-unfixed` means
# trivy fails on exactly the CVEs Debian has ALREADY shipped a fix for. Applying the
# security archive here is what closes that gap without waiting for an upstream rebuild:
# the tag stays pinned, the patches do not. It is what clears CVE-2026-53615 (util-linux
# 2.41-5, fixed in 2.41.5-0+deb13u1), which arrives through nine transitive Debian
# packages that nothing in this project asked for and that the pip removal below cannot
# touch, being OS packages rather than Python ones.
# `upgrade`, not `dist-upgrade`: patches for what is already here, never new packages.
# DEBIAN_FRONTEND is not decoration — a conffile prompt in a layer with no tty is a build
# that hangs until the job times out, which reads as CI being broken rather than as a
# question nobody answered.
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 1000 raft \
    && mkdir -p /data /var/log/raftkv \
    && chown -R raft /data /var/log/raftkv
# Hash-checked install from the exported lock: image builds cannot float transitives.
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps .
# pip and setuptools are build-time tools that nothing here needs at runtime: the
# container's whole job is to exec uvicorn against an already-installed package. Left in
# place they ship their own CVEs anyway -- CVE-2025-47273 in the setuptools that comes
# with the base image, and GHSA-6v7p-g79w-8964 in the msgpack that pip vendors -- neither
# of which appears in requirements.lock, because neither is a dependency of this project.
# Removing them is what makes the trivy gate pass on the merits. The alternative was
# relaxing the gate to let a fixed HIGH through, which would have made every future
# finding easier to wave past.
RUN pip uninstall --yes pip setuptools \
    && rm -rf /usr/local/lib/python*/site-packages/pkg_resources
USER raft
ENV RAFT_DB_PATH=/data/raft.db RAFT_LOG_DIR=/var/log/raftkv
EXPOSE 8000
CMD ["uvicorn", "--factory", "raftkv.app:create_app", "--host", "0.0.0.0", "--port", "8000"]
