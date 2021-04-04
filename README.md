# pgeon

This library is:
1. A zero-dependencies, decently fast Postgres client.
2. A webpack loader that type-checks your SQL queries at compile time (seriously).

## Installation

```sh
npm install --save pgeon
```

## Usage

For a first look, check out the small [example application](./example.ts) and its corresponding [webpack configuration](./webpack.config.ts). Provided Docker is running, you can try starting it with the command below. Notice that there is a type error which will be caught at compile time – try fixing it!

```sh
./docker-npm start
```
