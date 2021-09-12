FROM rust:latest
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/* 
COPY core-site.xml /etc/hadoop/conf/
