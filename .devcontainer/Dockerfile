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
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/protocolbuffers/protobuf/releases/download/v22.4/protoc-22.4-linux-x86_64.zip \
    && unzip protoc-22.4-linux-x86_64.zip -d /usr/local \
    && rm protoc-22.4-linux-x86_64.zip

RUN pipx install --force uv==0.4.27
