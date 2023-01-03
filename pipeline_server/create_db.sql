CREATE DATABASE dbsp WITH OWNER dbsp;
\c dbsp dbsp;

CREATE TABLE project (
    id bigint,
    version bigint,
    name varchar,
    code varchar,
    status varchar,
    error varchar,
    status_since timestamp,
    PRIMARY KEY (id)
);

CREATE SEQUENCE project_id_seq AS bigint;

CREATE TABLE pipeline (
    id bigint,
    project_id bigint,
    project_version bigint,
    created timestamp,
    PRIMARY KEY (id),
    FOREIGN KEY (project_id) REFERENCEC project(id) ON DELETE SET NULL
);

CREATE SEQUENCE pipeline_id_seq AS bigint;
