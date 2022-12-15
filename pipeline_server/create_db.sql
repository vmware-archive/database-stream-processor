CREATE DATABASE dbsp;
\c dbsp;

CREATE TABLE project (
    id bigint,
    name varchar,
    code varchar,
    status varchar,
    error varchar,
    primary key (id)
);

CREATE SEQUENCE project_id_seq AS bigint;

ALTER DATABASE dbsp OWNER to dbsp;
