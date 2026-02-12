# Development container Dockerfile based on the root Dockerfile
# This is optimized for local development with VS Code devcontainers

# Stage 1: Install Google Cloud SDK using APT
FROM python:3.10.18-slim AS gcloud-apt-install

RUN apt-get update && \
    apt-get install -y curl gnupg lsb-release && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && \
    apt-get install --no-install-recommends -y \
        google-cloud-cli \
        google-cloud-cli-gke-gcloud-auth-plugin && \
    apt-get clean && rm -rf /usr/lib/google-cloud-sdk/platform/bundledpythonunix \
    /var/lib/apt/lists/*


# Stage 2: Main development image
FROM python:3.10.18-slim

# Copy Google Cloud SDK from Stage 1
COPY --from=gcloud-apt-install /usr/lib/google-cloud-sdk /opt/google-cloud-sdk

# Set environment variable
ENV PATH="/opt/google-cloud-sdk/bin:$PATH"

# Detect architecture
ARG TARGETARCH

# Install system packages (including development tools)
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install --no-install-recommends -y \
        git gcc rsync sudo patch openssh-server \
        pciutils nano fuse socat netcat-openbsd curl tini autossh jq logrotate \
        vim less procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install the session manager plugin for AWS CLI
RUN ARCH=$(case "${TARGETARCH:-$(uname -m)}" in \
        "amd64"|"x86_64") echo "64bit" ;; \
        "aarch64") echo "arm64" ;; \
        *) echo "${TARGETARCH:-$(uname -m)}" ;; \
    esac) && \
    echo "Installing session manager plugin for AWS CLI for ${ARCH}" && \
    curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_${ARCH}/session-manager-plugin.deb" -o "session-manager-plugin.deb" && \
    sudo dpkg -i session-manager-plugin.deb && \
    rm session-manager-plugin.deb

# Install kubectl based on architecture
RUN ARCH=${TARGETARCH:-$(case "$(uname -m)" in \
        "x86_64") echo "amd64" ;; \
        "aarch64") echo "arm64" ;; \
        *) echo "$(uname -m)" ;; \
    esac)} && \
    curl -LO "https://dl.k8s.io/release/v1.33.5/bin/linux/$ARCH/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl

# Install Nebius CLI
RUN curl -sSL https://storage.eu-north1.nebius.cloud/cli/install.sh | NEBIUS_INSTALL_FOLDER=/usr/local/bin bash

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    ~/.local/bin/uv pip install --prerelease allow azure-cli --system && \
    # Upgrade setuptools in base image to mitigate CVE-2024-6345
    ~/.local/bin/uv pip install --system --upgrade setuptools==78.1.1 && \
    # Cleanup caches to reduce image size
    ~/.local/bin/uv cache clean && \
    rm -rf ~/.cache/pip ~/.cache/uv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add uv to PATH
ENV PATH="/root/.local/bin:$PATH"

# Set working directory
WORKDIR /workspaces/skypilot

# Note: The postCreateCommand in devcontainer.json will install the package
# in editable mode after the container is created with the source mounted
