FROM jhginn/devcontainer:ubuntu2204-pyuv312-20241007-gcloud

RUN apt-get update \
    && apt-get -y install libpq-dev clang \
        curl \
        ca-certificates \
        build-essential \
        libssl-dev \
        libffi-dev \
        libsasl2-dev \
        python3-dev \
        pkg-config \
        libsasl2-2 \
        libsasl2-modules \
        autoconf \
        automake \
        libtool \
        curl \
        make \
        g++ \
        unzip \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN pipx install --force uv==0.4.27
