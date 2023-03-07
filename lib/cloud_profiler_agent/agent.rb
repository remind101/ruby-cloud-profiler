# frozen_string_literal: true

require 'google/cloud/profiler/v2'
require 'googleauth'
require 'stackprof'

module CloudProfilerAgent
  Cloudprofiler = ::Google::Cloud::Profiler::V2

  PROFILE_TYPES = {
    :CPU => :cpu,
    :WALL => :wall,
    :HEAP_ALLOC => :object
  }.freeze
  SERVICE_REGEXP = /^[a-z]([-a-z0-9_.]{0,253}[a-z0-9])?$/.freeze

  # Agent interfaces with the CloudProfiler API.
  class Agent
    def initialize(service:, project_id:, service_version: nil, debug_logging: false, instance: nil, zone: nil)
      raise ArgumentError, "service must match #{SERVICE_REGEXP}" unless SERVICE_REGEXP =~ service

      @service = service
      @project_id = project_id
      @debug_logging = debug_logging

      @labels = { language: 'ruby' }
      @labels[:version] = service_version unless service_version.nil?
      @labels[:zone] = zone unless zone.nil?

      @deployment = Cloudprofiler::Deployment.new(project_id: project_id, target: service, labels: @labels)

      @profile_labels = {}
      @profile_labels[:instance] = instance unless instance.nil?

      @profiler = Cloudprofiler::ProfilerService::Client.new

      # <https://github.com/googleapis/googleapis/blob/7e17784e6465431981f36806e6376d69de1fc424/google/devtools/cloudprofiler/v2/profiler.proto#L39-L45>
      #
      #   The request may fail with ABORTED error if the creation is not
      #   available within ~1m, the response will indicate the duration of the
      #   backoff the client should take before attempting creating a profile
      #   again. The backoff duration is returned in google.rpc.RetryInfo
      #   extension on the response status. To a gRPC client, the extension
      #   will be return as a binary-serialized proto in the trailing metadata
      #   item named "google.rpc.retryinfo-bin".
      #
      # Unfortunately, it's unclear how this maps to the JSON API as used by
      # Google::Apis. Some guess is made: see Looper#backoff_duration.
      #
      # However, emperical testing shows that if what appears to be this
      # throttling message occurs, it happens as late as 230 seconds, not 1
      # minute as the documentation suggests. So, we must increase the timeout.
      Cloudprofiler::ProfilerService::Client.configure do |config|
        config.timeout = 300
      end
    end

    attr_reader :service, :project_id, :labels, :deployment, :profile_labels

    def create_profile
      req = Cloudprofiler::CreateProfileRequest.new(deployment: deployment, profile_type: PROFILE_TYPES.keys)
      debug_log('creating profile')
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      profile = @profiler.create_profile(req)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
      debug_log("got profile after #{elapsed} seconds")
      profile
    end

    def update_profile(profile)
      debug_log('updating profile')
      @profiler.update_profile(profile: profile)
      debug_log('profile updated')
    end

    # start will begin creating profiles in a background thread, looping
    # forever. Exceptions are rescued and logged, and retries are made with
    # exponential backoff.
    def start
      return if !@thread.nil? && @thread.alive?

      @thread = Thread.new do
        Looper.new(debug_logging: @debug_logging).run do
          profile = create_profile
          profile_and_upload(profile)
        end
      end
    end

    private

    def profile(duration, mode)
      start_time = Time.now
      # interval is in microseconds for :cpu and :wall, number of allocations for :object
      stackprof = StackProf.run(mode: mode, raw: true, interval: 1000) do
        sleep(duration)
      end

      CloudProfilerAgent::PprofBuilder.convert_stackprof(stackprof, start_time, Time.now)
    end

    def profile_and_upload(profile)
      debug_log("profiling #{profile.profile_type} for #{profile.duration}")
      profile.profile_bytes = profile(profile.duration.seconds, PROFILE_TYPES.fetch(profile.profile_type))
      update_profile(profile)
    end

    def debug_log(message)
      puts(message) if @debug_logging
    end
  end
end
