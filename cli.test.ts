#!/usr/bin/env node

import {execSync} from 'child_process'

/*
PGUSER=dbuser \
PGHOST=database.server.com \
PGPASSWORD=secretpassword \
PGDATABASE=mydb \
PGPORT=3211 \
node script.js

# Default values:
PGHOST='localhost'
PGUSER=process.env.USER
PGDATABASE=process.env.USER
PGPASSWORD=null
PGPORT=5432
*/

const config = {
    host: 'localhost',
    port: 5432,
    database: 'pgeon_tmp_database',
    user: 'pgeon_tmp_user',
    password: 'pgeon_tmp_password'
}

const psqlAnon = (cmd: string) => execSync(`sudo -u postgres psql ${cmd}`)
const psqlAuth = (cmd: string) => execSync(`sudo -u postgres psql "host=${config.host} port=${config.port} user=${config.user} dbname=${config.database} password='${config.password}'" ${cmd}`)

try {
    // Set up fresh database

    psqlAnon(`-c "drop database if exists ${config.database}"`)
    psqlAnon(`-c "drop role if exists ${config.user}"`)
    psqlAnon(`-c "create role ${config.user} superuser login encrypted password '${config.password}'"`)
    psqlAnon(`-c "create database ${config.database} owner ${config.user} encoding 'UTF8'"`)
    psqlAuth(`-c "alter schema public owner to ${config.user}"`)

    console.log('> Creating main schema.')
    // psqlAuth(`-f db.sql`)
    // TODO Inline.
    /*
    create table users (
        id serial primary key,
        name varchar(50) not null,
        gender char(1) not null,
        is_human bool not null,
        descents smallint not null,
        joined_year int,
        points float not null,
        birthday date not null,
        picture bytea not null
    );
    */

    // Run tests

    // TODO

} finally {
    // Tear down database

    psqlAnon(`-c "drop database if exists ${config.database}"`)
    psqlAnon(`-c "drop role if exists ${config.user}"`)
}
