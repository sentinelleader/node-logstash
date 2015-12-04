var thrift = require('thrift'),
  util = require('util'),
  events = require('events'),
  logger = require('log4node');
  var scribe = require('gen-nodejs/scribe');
  var scribe_types = require('gen-nodejs/scribe_types');

function ScribeConnectionManager(host, port) {
  events.EventEmitter.call(this);
  this.host = host;
  this.port = port;

  logger.info('Connecting to Scribe', this.host + ':' + this.port);

  this.connection = thrift.createConnection(this.host, this.port);
  this.client = thrift.createClient(scribe, this.connection);
  this.end_callback = function() {
    logger.info('Scribe connection lost to ' + this.host + ':' + this.port);
  }.bind(this);

  this.connection.on('end', function() {
    this.end_callback();
  }.bind(this));

  this.connection.on('error', function(err) {
    this.emit('error', err);
  }.bind(this));

  this.connection.on('connect', function() {
    logger.info('Connected to Scribe', this.host + ':' + this.port);
    this.emit('connect', this.client);
  }.bind(this));
}

util.inherits(ScribeConnectionManager, events.EventEmitter);

ScribeConnectionManager.prototype.msg_parser = function(msg_type, category, data) {
  // Split the actual JSON message from our Forwarded syslog message
  if (msg_type === 'syslog') {
    var m = JSON.parse(data);
    data = m.message.split(':::')[1].trim()
  }
  var log_msg = new scribe_types.LogEntry({
    category : category,
    message : data
  });
  return log_msg;
};

ScribeConnectionManager.prototype.quit = function(callback) {
  logger.info('Closing connection to Scribe', this.host + ':' + this.port);
  this.emit('before_quit', this.client);
  this.end_callback = callback;
  logger.info("before connection end");
  this.connection.end();
  logger.info("after connection end");
  delete this.client;
  delete this.connection;
  callback();
};

exports.create = function(host, port, retry) {
  return new ScribeConnectionManager(host, port);
};
