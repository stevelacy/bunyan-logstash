
{Writable} = require 'stream'
lumberjack = require 'lumberjack-protocol'
bunyan     = require 'bunyan'

levels =
  10: 'trace'
  20: 'debug'
  30: 'info'
  40: 'warn'
  50: 'error'
  60: 'fatal'

class BunyanLumberjackStream extends Writable
  constructor: (tlsOptions, lumberjackOptions={}, options={}) ->
    super {objectMode: true}

    @_client = lumberjack.client tlsOptions, lumberjackOptions

    @_client.on 'connect', (count) => @emit 'connect', count
    @_client.on 'dropped', (count) => @emit 'dropped', count
    @_client.on 'disconnect', (err) => @emit 'disconnect', err

    @_host = require('os').hostname()
    @_tags = options.tags ? ['bunyan']
    @_type = options.type ? 'json'
    @_application = options.appName ? process.title

    @on 'finish', =>
      @_client.close()

  _write: (entry, encoding, done) ->

    entry = JSON.parse entry
    host = entry.hostname ? @_host

    # Massage the entry to look like a logstash entry.
    bunyanLevel = entry.level

    entry.level = levels[entry.level]

    entry.message = entry.msg ? ''
    delete entry.msg

    entry['@timestamp'] = entry.time
    delete entry.time

    delete entry.v

    # Add some extra fields
    entry.tags ?= @_tags
    entry.source = "#{host}/#{@_application}"

    dataFrame =
      line: JSON.stringify(entry)
      host: host
      bunyanLevel: bunyanLevel

    # Set type directly on the data frame, so we can use it for conditionals up in
    # logstash filters section.
    if @_type? then dataFrame.type = @_type

    @_client.writeDataFrame dataFrame

    done()

module.exports = (options={}) ->
  return new BunyanLumberjackStream options.tlsOptions, options.lumberjackOptions, options
