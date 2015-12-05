var base_output = require('../lib/base_output'),
  util = require('util'),
  logger = require('log4node'),
  thrift = require('thrift'),
  error_buffer = require('../lib/error_buffer');

var scribe = require('../gen-nodejs/scribe');
var scribe_types = require('../gen-nodejs/scribe_types');

var options = {
   transport: thrift.TFramedTransport,
   protocol: thrift.TBinaryProtocol,
};

function OutputScribe() {
  base_output.BaseOutput.call(this);
  this.mergeConfig(this.serializer_config());
  this.mergeConfig(error_buffer.config(function() {
    return 'output Scribe to ' + this.host + ':' + this.port;
  }));
  this.mergeConfig({
    name: 'Scribe',
    host_field: 'host',
    port_field: 'port',
    required_params: ['msg_type', 'category'],
    start_hook: this.start,
  });
}

util.inherits(OutputScribe, base_output.BaseOutput);

OutputScribe.prototype.createScribeConn = function(host, port) {
  connection = thrift.createConnection(host, port, options);
  return connection;
};

OutputScribe.prototype.createScribeMsg = function(msg_type, category, data) {
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

OutputScribe.prototype.sendScribeMsg = function(data) {
  var scr_msg = this.createScribeMsg(this.msg_type, this.category, this.serialize_data(data));
  var connection = this.createScribeConn(this.host, this.port);
  var client = thrift.createClient(scribe, connection);
  client.Log([scr_msg], function(err, result) {
    if (result === 1 || err) {
      this.error_buffer.emit('error', 'Unable to publish message to Scribe ');
    }
    connection.end();
  });
};

OutputScribe.prototype.start = function(callback) {
  logger.info('Starting Scribe Output Plugin');
  this.send = this.sendScribeMsg.bind(this);
  callback();
};

OutputScribe.prototype.process = function(data) {
  this.send(data);
}
OutputScribe.prototype.close = function(callback) {
  logger.info('Starting Scribe Output Plugin');
  callback();
};

exports.create = function() {
  return new OutputScribe();
};
