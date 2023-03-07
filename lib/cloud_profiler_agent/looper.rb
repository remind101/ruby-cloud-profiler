# frozen_string_literal: true

require 'google/cloud/profiler/v2'

module CloudProfilerAgent
  # Looper is responsible for the main loop of the agent. It calls a
  # block repeatedly, handling errors, backing off, and retrying as
  # appropriate.
  class Looper
    def initialize(
      min_iteration_sec: 10,
      max_iteration_sec: 60 * 60,
      backoff_factor: 1.5,
      debug_logging: false,
      sleeper: ->(sec) { sleep(sec) },
      clock: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) },
      rander: -> { rand }
    )

      # the minimum and maximum time between iterations of the profiler loop,
      # in seconds. Normally the Cloud Profiler API tells us how fast to go,
      # but we back off in case of error.
      @min_iteration_sec = min_iteration_sec
      @max_iteration_sec = max_iteration_sec
      @backoff_factor = backoff_factor

      # stubbable for testing
      @sleeper = sleeper
      @clock = clock
      @rander = rander

      @debug_logging = debug_logging
    end

    attr_reader :min_iteration_sec, :max_iteration_sec, :backoff_factor

    def run(max_iterations = 0)
      iterations = 0
      iteration_time = @min_iteration_sec
      loop do
        start_time = @clock.call
        iterations += 1
        begin
          yield
        rescue ::Google::Cloud::Error => e
          backoff = backoff_duration(e)
          if backoff.nil?
            iteration_time = @max_iteration_sec
          else
            debug_log("sleeping for #{backoff} at request of server")
            # This might be longer than max_iteration_sec and that's OK: with
            # a very large number of agents it might be necessary to achieve
            # the objective of 1 profile per minute.
            @sleeper.call(backoff)
            iteration_time = @min_iteration_sec
          end
        rescue StandardError => e
          iteration_time *= @backoff_factor + @rander.call / 2
          elapsed = @clock.call - start_time
          debug_log("Cloud Profiler agent encountered error after #{elapsed} seconds, will retry: #{e.inspect}")
        else
          iteration_time = @min_iteration_sec
        end

        return unless iterations < max_iterations || max_iterations.zero?

        iteration_time = [@max_iteration_sec, iteration_time].min
        next_time = start_time + iteration_time
        delay = next_time - @clock.call
        @sleeper.call(delay) if delay.positive?
      end
    end

    private

    def backoff_duration(error)
      # It's unclear how this should work, so this is based on a guess.
      #
      # https://github.com/googleapis/google-api-ruby-client/issues/1498
      match = /backoff for (?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?/.match(error.message)
      return nil if match.nil?

      hours = Integer(match[1] || 0)
      minutes = Integer(match[2] || 0)
      seconds = Integer(match[3] || 0)

      seconds + minutes * 60 + hours * 60 * 60
    end

    def debug_log(message)
      puts(message) if @debug_logging
    end
  end
end
