-- Initialize extra databases expected by some services
-- Create an 'admin' database in case clients attempt to connect to it
-- This file is mounted as /docker-entrypoint-initdb.d/init.sql and runs on first container init

CREATE DATABASE admin;
\connect admin
-- No further objects needed here; role ownership is handled by Postgres entrypoint
