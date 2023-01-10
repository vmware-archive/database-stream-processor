FROM ubuntu:latest

# Skip past interactive prompts during apt install
ENV DEBIAN_FRONTEND noninteractive

RUN apt update && apt install libssl-dev build-essential pkg-config \
     git gcc clang libclang-dev python3-pip hub numactl cmake \
     curl openjdk-19-jdk maven postgresql-client postgresql netcat jq -y 

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

RUN git clone --branch pipeline_server http://www.github.com/lalithsuresh/database-stream-processor \
    && git clone http://www.github.com/vmware/sql-to-dbsp-compiler

RUN cd database-stream-processor && ~/.cargo/bin/cargo build

RUN cd sql-to-dbsp-compiler/SQL-compiler && mvn -DskipTests package

# Install rpk
RUN curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | bash && \
    apt install redpanda -y 

####### UNSAFE ######## 
###### Do not use outside of demo ########
RUN sed -i "s/scram-sha-256/trust/g" /etc/postgresql/14/main/pg_hba.conf \
   && service postgresql restart \
   && createuser -h localhost -U postgres dbsp
