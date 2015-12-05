Scribe output plugin
---

This plugin is used to sent data to a Thrift server.

They are two mandatory params `category` and `msg_type`. `msg_type` can be of two:
* syslog : If the messages are read from a Central syslog server (make sure to modify `ScribeConnectionManager.prototype.msg_parser` to extract the proper JSON message)
* raw : If the messages are Raw JSON strings

Example:
Config using url: `output://scribe://localhost:1541?msg_type=raw&category=nodejs`

Parameters:

* ``host``: ip of the Thrift server.
* ``port``: port of the Thrift server.
* ``msg_type``: Type of incoming message. Raw JSON or syslog forwarded
* ``category``: Category for Scribe
