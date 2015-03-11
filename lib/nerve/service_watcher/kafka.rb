require 'nerve/service_watcher/base'
require 'poseidon'

module Nerve
  module ServiceCheck
    class KafkaServiceCheck < BaseServiceCheck

      def initialize(opts={})
        super

        raise ArgumentError, "missing required argument 'port' in kafka check" unless opts['port']
        raise ArgumentError, "missing required argument 'topic' in kafka check" unless opts['topic']

        @port = opts['port']
        @topic = opts['topic']
        @host = opts['host']            || '127.0.0.1'
        @producer_id = opts['producer_id']  || 'nerveProducer'
        @consumer_id = opts['consumer_id']  || 'nerveConsumer'
      end

      def check
        log.debug "nerve: running kafka health check #{@name}"

        # try to post a message
        log.debug "nerve: publishing to kafka"
        producer = Poseidon::Producer.new([@host+':'+@port.to_s], @client_id)

        messages = []
        messages << Poseidon::MessageToSend.new(@topic, 'kafka test message')
        producer.send_messages(messages)

        # try to read the last message
        log.debug "nerve: consuming from kafka"
        consumer = Poseidon::PartitionConsumer.new(@consumer_id, @host, @port, @topic, 0, :latest_offset)

        messages = consumer.fetch
        if messages.size > 0
          return true
        else
          log.debug "nerve: no messages fetched"
          return false
        end
      end
    end

    CHECKS ||= {}
    CHECKS['kafka'] = KafkaServiceCheck
  end
end
