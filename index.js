var
	moment = require('moment'),
	async  = require('async'),
	redis  = require('redis-client').createClient(),
	Iconv  = require('iconv').Iconv,
	amqp   = require('amqp').createConnection({ host: 'localhost' }),
	http   = require('http'),
	_      = require('underscore')

exports.Ubertrager = function(options) {
	/*
	Options:
		"channels" - dictionary with channels descriptions
		"direct"   - direct exchange name
		"fanout"   - fanout exchange name
	*/

	return Object.create({

		'options': options,

		// Publish object in queue
		publish: function(route, object) {
			this.webExchange.publish(route, JSON.stringify(object), options={'headers': { 'name': route }})
			this.logExchange.publish(route, JSON.stringify(object))
		},

		start: function() {
			var thiz = this

			amqp.addListener('ready', function () {
				console.log('Connected to AMQP server')

				thiz.logExchange = thiz.options.direct
				thiz.webExchange = thiz.options.fanout

				thiz.resetChannels(thiz.options.channels)
			})
		},

		// Replace current channels list with new one
		resetChannels: function(channels) {
			this.channels._value = {}
			this.channels        = channels

			return this.channels
		},

		// Default uniquiness filter based on redis
		filter: function(result, channel, callback) {
			redis.hkeys(channel.rediskey, function(err, keys) {
				if(err) {
					console.log(channel.name + ': redis error: ' + err)
					return result
				}

				keys = _.map(keys, function(key) { return key.toString() })

				callback(null, _.map(
					_.filter(result, function(R) { return ! _.include(keys, R.id) }),
					function(R) { redis.hset(channel.rediskey, R.id, moment()); return R }
				))
			});
		}

	}, {

		// webExchange is a historical variable to store fanout exchange
		'webExchange': {
			configurable: true,

			get: function(name) { return this.webExchangeValue },

			set: function(name) {
				this.webExchangeValue = amqp.exchange(name, options={ 'type': 'fanout' })
			}
		},

		// logExchange variable is historicaly for direct exchange
		'logExchange': {
			configurable: false,

			get: function() { return this.logExchangeValue },

			set: function(name) {
				this.logExchangeValue = amqp.exchange(name, options={ 'type': 'direct', 'durable': true, 'autoDelete': false })
			}
		},

		// Process channel description dictionary and add it in time loop
		'channels': {
			configurable: false,

			get: function() { return this.channelsValue || {} },

			// Add channel into time loop
			set: function(channels) {
				this.channelsValue = this.channelsValue || {}
				channels           = _.isArray(channels) ? channels : [channels]

				_.each(channels, function(channel) {
					var thiz = this

					// Main loop
					setInterval(function(channel) {

						var
							rawLen = 0,
							raw    = new Buffer(200000)

						http.get({
							host: _.isFunction(channel.host) ? channel.host() : channel.host,
							path: _.isFunction(channel.path) ? channel.path() : channel.path,
							port: 80
						})
							.on('error', function(e) { console.log(e.message) })
						
							.on('response', function(response) {
								
								// No Content-Type? Piss off!
								try {
									var encoding = response.headers['content-type'].match(/charset=([\w\d\-]+)(;|\s|$)/i)[1]
								} catch(e) {
									console.log(channel.name + ': Problem while getting server response content-type. Setting to utf-8.')
									var encoding = 'utf-8'
								}

								// Only responses with http 200 are considered to be responses
								if(response.statusCode != '200') {
									console.log(channel.name + ': Wrong http response status: ' + response.statusCode)
									return false
								}

								response
									.on('data', function(chunk) {
										chunk.copy(raw, rawLen, 0)
										rawLen = rawLen + chunk.length
									})
								
									.on('end', function() {
										
										// Convert buffer into utf-8 string
										if(encoding == 'utf-8') {
											raw = raw.toString('utf8', 0, rawLen)
										} else {
											raw = (new Iconv(encoding, 'utf-8')).convert(raw.slice(0, rawLen)).toString()
										}
										
										if(response.headers['content-length'] && rawLen != response.headers['content-length']) {
											console.log(channel.name + ': Размер данных не соответствует ожидаемому (битый ответ)')
											console.log('Расчетный размер: ' + rawLen)
											console.log('Указанный: ' + response.headers['content-length'])
											return false
										}

										var result = channel.process(channel.name, raw)

										// Apply channel specific filters in series
										async.reduce(
											(_.isArray(channel.filters) ? channel.filters : []).concat([thiz.filter]),

											result,

											function(result, filter, callback) { filter(result, channel, callback) },

											function(err, results) {
												if(err) {
													console.log(channel.name + ': filtering error: ' + err)
													return false
												}

												// Emit data into direct and fanout exchanges
												results.length ? thiz.publish(channel.name, results) : false
											}
										)
									});
							});

					}, channel.interval, channel)

					this.channelsValue[channel.name] = channel
				}, this)
			}
		}
	})
}