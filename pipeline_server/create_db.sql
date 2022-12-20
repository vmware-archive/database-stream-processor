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
    primary key (id)
);

CREATE SEQUENCE project_id_seq AS bigint;
