'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.sql = exports.tx = exports.db = undefined;
exports.config = config;
exports.migrate = migrate;

var _promise = require('bluebird/js/release/promise');

var _promise2 = _interopRequireDefault(_promise);

var _pg = require('pg');

var _pg2 = _interopRequireDefault(_pg);

var _fs = require('fs');

var _fs2 = _interopRequireDefault(_fs);

var _path = require('path');

var _path2 = _interopRequireDefault(_path);

var _sqltag = require('@eknkc/sqltag');

var _sqltag2 = _interopRequireDefault(_sqltag);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } step("next"); }); }; }

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) arr2[i] = arr[i]; return arr2; } else { return Array.from(arr); } }

const Promise = (0, _promise2.default)();

Promise.prototype.rows = function () {
  return this.then(result => result.rows || []);
};

Promise.prototype.first = function (field) {
  let def = arguments.length <= 1 || arguments[1] === undefined ? null : arguments[1];

  var first = this.rows().get(0);

  if (field) first = first.get(field);

  if (typeof first === 'undefined') first = def;

  return first;
};

let options = {};

function config() {
  let opts = arguments.length <= 0 || arguments[0] === undefined ? {} : arguments[0];

  options = Object.assign(options, opts);
}

var db = exports.db = function db() {
  for (var _len = arguments.length, args = Array(_len), _key = 0; _key < _len; _key++) {
    args[_key] = arguments[_key];
  }

  if (args.length == 1 && typeof args[0] == 'string') args = [args];

  return Promise.using(getDisposer(), conn => conn.sql.apply(conn, _toConsumableArray(args)));
};

var tx = exports.tx = function tx(gen) {
  let ctx = arguments.length <= 1 || arguments[1] === undefined ? null : arguments[1];

  return Promise.using(getDisposer(), function (conn) {
    return conn.begin().then(function () {
      return gen(conn).then(data => conn.commit().return(data)).catch(err => conn.rollback().throw(err));
    });
  });
};

function getDisposer() {
  return getConnection().disposer(conn => conn.release());
}

function getConnection() {
  return Promise.fromNode(function (next) {
    _pg2.default.connect(options, function (err, client, done) {
      if (err) return next(err);
      next(null, new Connection(client, done));
    });
  });
}

const namematch = /^\$([a-z\-_]+)\$\s*/;

function build(args) {
  var res = _sqltag2.default.apply(undefined, _toConsumableArray(args)).sql();
  var nmatch = namematch.exec(res.text);

  if (nmatch) {
    res.name = nmatch[1];
    res.text = res.text.substr(nmatch[0].length);
  }

  return res;
}

function migrate(path) {
  return handleMigrations(path);
}

class Connection {
  constructor(conn, releaser) {
    this.conn = conn;
    this.done = releaser;
    this.query = Promise.promisify(this.conn.query, { context: this.conn });
  }

  release(err) {
    if (err) return this.done(err);

    if (this.intransaction) return this.rollback().then(() => this.done(), err => this.done(err));

    this.done();
    return Promise.resolve(true);
  }

  begin() {
    this.intransaction = true;
    return this.query('BEGIN');
  }

  commit() {
    return this.query('COMMIT').then(() => this.intransaction = false);
  }
  rollback() {
    return this.query('ROLLBACK').then(() => this.intransaction = false);
  }
  sql() {
    for (var _len2 = arguments.length, args = Array(_len2), _key2 = 0; _key2 < _len2; _key2++) {
      args[_key2] = arguments[_key2];
    }

    args = build(args);

    return this.query(args).catch(err => {
      err.query = args;
      throw err;
    });
  }
}

exports.sql = _sqltag2.default;

const readDir = Promise.promisify(_fs2.default.readdir);
const readFile = Promise.promisify(_fs2.default.readFile);

function handleMigrations(rootpath) {
  function compare(a, b) {
    a = parseInt(a);
    b = parseInt(b);
    return a - b;
  }

  return tx(function () {
    var ref = _asyncToGenerator(function* (conn) {
      yield conn.sql`create table if not exists migrations(name text primary key, applied_at timestamptz not null default now())`;
      yield conn.sql`lock table migrations in ACCESS EXCLUSIVE mode`;

      let latest = yield conn.sql`select name from migrations order by split_part(name, '_', 1)::int desc limit 1`.first();
      if (!latest) latest = "0";else latest = latest.name;

      let files = (yield readDir(rootpath)).filter(f => /\.sql$/.test(f) && compare(f, latest) > 0);
      files.sort(compare);

      for (var i = 0; i < files.length; i++) {
        console.log(`Applying db migrations: ${ files[i] }`);
        let file = yield readFile(_path2.default.join(rootpath, files[i]), { encoding: 'utf8' });
        yield conn.query(file);
        yield conn.sql`insert into migrations (name) values (${ files[i] })`;
      }
    });

    return function (_x4) {
      return ref.apply(this, arguments);
    };
  }());
};
