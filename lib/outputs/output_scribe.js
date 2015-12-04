var base_output = require('../lib/base_output'),
  util = require('util'),
  scribe_connection_manager = require('../lib/scribe_connection_manager'),
  logger = require('log4node'),
  error_buffer = require('../lib/error_buffer');

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

OutputScribe.prototype.sendScribeMsg = function(data) {
  var scr_msg = this.scribe_connection_manager.msg_parser(this.msg_type, this.category, this.serialize_data(data));
  this.client.Log(scr_msg, function(err, resultCode) {
    if (resultCode === 1 || err) {
      this.error_buffer.emit('error', 'Unable to publish message to Scribe ');
    }
  });
};

OutputScribe.prototype.start = function(callback) {
  this.send = this.sendScribeMsg.bind(this);

  this.scribe_connection_manager = scribe_connection_manager.create(this.host, this.port);

  this.scribe_connection_manager.on('error', function(err) {
    this.error_buffer.emit('error', err);
  }.bind(this));

  this.scribe_connection_manager.on('connect', function() {
    this.error_buffer.emit('ok');
  }.bind(this));

  this.scribe_connection_manager.once('connect', function(client) {
    this.client = client;
  }.bind(this));

  callback();
};

OutputScribe.prototype.process = function(data) {
  if (this.client) {
    this.send(data);
  }
  else {
    this.error_buffer.emit('ok');
  }
};

OutputScribe.prototype.close = function(callback) {
  this.scribe_connection_manager.quit(callback);
};

exports.create = function() {
  return new OutputScribe();
};
