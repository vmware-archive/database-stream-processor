VERSION 0.7
FROM ubuntu:22.04

RUN apt-get update && apt-get install --yes sudo

WORKDIR /dbsp
ENV RUSTUP_HOME=$HOME/.rustup
ENV CARGO_HOME=$HOME/.cargo
ENV PATH=$HOME/.cargo/bin:$PATH
ENV RUST_VERSION=1.69.0

install-deps:
    RUN apt-get update
    RUN apt-get install --yes build-essential curl libssl-dev build-essential pkg-config \
                              cmake git gcc clang libclang-dev python3-pip python3-plumbum \
                              hub numactl cmake openjdk-19-jre-headless maven netcat jq \
                              libsasl2-dev docker.io

install-rust:
    FROM +install-deps
    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
        -y \
        --default-toolchain $RUST_VERSION \
        --profile minimal \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN chmod -R a+w $RUSTUP_HOME $CARGO_HOME
    RUN rustup --version
    RUN cargo --version
    RUN rustc --version

install-nextjs:
    RUN sudo apt-get update
    RUN sudo apt-get install git sudo curl
    RUN curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
    RUN sudo apt-get install -y nodejs
    RUN npm install --global yarn
    RUN npm install --global openapi-typescript-codegen

install-chef:
    FROM +install-rust
    RUN cargo install --debug cargo-chef

prepare-cache:
    FROM +install-chef

    RUN mkdir -p crates/dataflow-jit
    RUN mkdir -p crates/nexmark
    RUN mkdir -p crates/dbsp
    RUN mkdir -p crates/adapters
    RUN mkdir -p crates/pipeline_manager
    #RUN mkdir -p crates/webui-tester

    COPY --keep-ts Cargo.toml .
    COPY --keep-ts Cargo.lock .
    COPY --keep-ts crates/dataflow-jit/Cargo.toml crates/dataflow-jit/
    COPY --keep-ts crates/nexmark/Cargo.toml crates/nexmark/
    COPY --keep-ts crates/dbsp/Cargo.toml crates/dbsp/
    COPY --keep-ts crates/adapters/Cargo.toml crates/adapters/
    COPY --keep-ts crates/pipeline_manager/Cargo.toml crates/pipeline_manager/
    #COPY --keep-ts crates/webui-tester/Cargo.toml crates/webui-tester/

    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/lib.rs
    RUN mkdir -p crates/nexmark/src && touch crates/nexmark/src/lib.rs
    RUN mkdir -p crates/dbsp/src && touch crates/dbsp/src/lib.rs
    RUN mkdir -p crates/adapters/src && touch crates/adapters/src/lib.rs
    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark-gen && touch crates/nexmark/benches/nexmark-gen/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark && touch crates/nexmark/benches/nexmark/main.rs
    RUN mkdir -p crates/dbsp/benches/gdelt && touch crates/dbsp/benches/gdelt/main.rs
    RUN mkdir -p crates/dbsp/benches/ldbc-graphalytics && touch crates/dbsp/benches/ldbc-graphalytics/main.rs
    RUN mkdir -p crates/pipeline_manager/src && touch crates/pipeline_manager/src/main.rs
    #RUN mkdir -p crates/webui-tester/src && touch crates/webui-tester/src/lib.rs

    RUN cargo chef prepare
    SAVE ARTIFACT recipe.json

build-cache:
    FROM +install-chef
    COPY +prepare-cache/recipe.json ./
    RUN cargo chef cook --all-targets
    SAVE ARTIFACT target
    SAVE ARTIFACT $CARGO_HOME cargo_home

build-crates:
    FROM +install-rust
    COPY --keep-ts +build-cache/target target
    COPY --keep-ts +build-cache/cargo_home $CARGO_HOME
    COPY --keep-ts --dir crates .
    COPY --keep-ts Cargo.toml Cargo.lock README.md .
    RUN cargo build --all-targets
    RUN cargo test --all-targets --no-run

test-dataflow-jit:
    FROM +build-crates
    COPY  --keep-ts demo/project_demo01-TimeSeriesEnrich demo/project_demo01-TimeSeriesEnrich
    RUN cargo test --package dataflow-jit

test-dbsp-adapters:
    FROM +build-crates
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v22.3.11
        RUN docker run --name redpanda -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v22.3.11 && \
            cargo test --package dbsp_adapters
    END

test-dbsp:
    FROM +build-crates
    COPY demo/project_demo01-TimeSeriesEnrich demo/project_demo01-TimeSeriesEnrich
    RUN cargo test --package dbsp

test-manager:
    FROM +build-crates
    COPY deploy/docker-compose-dev.yml deploy/docker-compose-dev.yml
    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV RUST_LOG=error

    WITH DOCKER --pull postgres
        RUN docker run -p 5432:5432 --name postgres -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres && \
            cargo test --package dbsp_pipeline_manager --no-default-features
    END