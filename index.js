import bluebird from 'bluebird/js/release/promise'
import pg from 'pg'
import fs from 'fs'
import path from 'path'
import sql from '@eknkc/sqltag'

const Promise = bluebird()

Promise.prototype.rows = function () {
  return this.then(result => result.rows || [])
}

Promise.prototype.first = function (field, def = null) {
  var first = this.rows().get(0)

  if (field)
    first = first.get(field)

  if (typeof first === 'undefined')
    first = def

  return first
}

let options = {};

export function config(opts = {}) {
  options = Object.assign(options, opts);
}

export var db = function (...args) {
  if (args.length == 1 && typeof args[0] == 'string')
    args = [args]

  return Promise.using(getDisposer(), conn => conn.sql(...args))
}

export var tx = (gen, ctx = null) => {
  return Promise.using(getDisposer(), function (conn) {
    return conn.begin()
      .then(function () {
        return gen(conn)
          .then(data => conn.commit().return(data))
          .catch(err => conn.rollback().throw(err))
      })
  })
}

function getDisposer () {
  return getConnection().disposer(conn => conn.release())
}

function getConnection () {
  return Promise.fromNode(function (next) {
    pg.connect(options, function (err, client, done) {
      if (err) return next(err)
      next(null, new Connection(client, done))
    })
  })
}

const namematch = /^\$([a-z\-_]+)\$\s*/;

function build (args) {
  var res = sql(...args).sql()
  var nmatch = namematch.exec(res.text)

  if (nmatch) {
    res.name = nmatch[1]
    res.text = res.text.substr(nmatch[0].length)
  }

  return res
}

export function migrate(path) {
  return handleMigrations(path);
}

class Connection {
  constructor(conn, releaser) {
    this.conn = conn
    this.done = releaser
    this.query = Promise.promisify(this.conn.query, { context: this.conn })
  }

  release(err) {
    if (err)
      return this.done(err)

    if (this.intransaction)
      return this.rollback().then(() => this.done(), err => this.done(err))

    this.done()
    return Promise.resolve(true)
  }

  begin() {
    this.intransaction = true
    return this.query('BEGIN')
  }

  commit() { return this.query('COMMIT').then(() => this.intransaction = false); }
  rollback() { return this.query('ROLLBACK').then(() => this.intransaction = false); }
  sql(...args) {
    args = build(args);

    return this.query(args).catch(err => {
      err.query = args;
      throw err;
    })
  }
}

export { sql }

const readDir = Promise.promisify(fs.readdir);
const readFile = Promise.promisify(fs.readFile);

function handleMigrations(rootpath) {
  function compare(a, b) {
    a = parseInt(a)
    b = parseInt(b)
    return a - b;
  }

  return tx(async function(conn) {
    await conn.sql`create table if not exists migrations(name text primary key, applied_at timestamptz not null default now())`;
    await conn.sql`lock table migrations in ACCESS EXCLUSIVE mode`;

    let latest = await conn.sql`select name from migrations order by split_part(name, '_', 1)::int desc limit 1`.first();
    if (!latest) latest = "0";
    else latest = latest.name;

    let files = (await readDir(rootpath)).filter(f => /\.sql$/.test(f) && compare(f, latest) > 0);
    files.sort(compare);

    for (var i = 0; i < files.length; i++) {
      console.log(`Applying db migrations: ${files[i]}`);
      let file = await readFile(path.join(rootpath, files[i]), { encoding: 'utf8' });
      await conn.query(file);
      await conn.sql`insert into migrations (name) values (${files[i]})`;
    }
  });
};
