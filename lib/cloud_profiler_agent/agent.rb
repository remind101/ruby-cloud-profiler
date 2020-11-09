# frozen_string_literal: true

require 'google/apis/cloudprofiler_v2'
require 'googleauth'
require 'stackprof'

module CloudProfilerAgent
  Cloudprofiler = Google::Apis::CloudprofilerV2

  PROFILE_TYPES = {
    'CPU' => :cpu,
    'WALL' => :wall,
    'HEAP_ALLOC' => :object
  }.freeze
  SERVICE_REGEXP = /^[a-z]([-a-z0-9_.]{0,253}[a-z0-9])?$/.freeze
  SCOPES = ['https://www.googleapis.com/auth/cloud-platform'].freeze

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

      @profiler = Cloudprofiler::CloudProfilerService.new
      @profiler.authorization = Google::Auth.get_application_default(SCOPES)

      # the minimum and maximum time between iterations of the profiler loop,
      # in seconds. Normally the Cloud Profiler API tells us how fast to go,
      # but we back off in case of error.
      @min_iteration_time = 10
      @max_iteration_time = 60 * 60
    end

    attr_reader :service, :project_id, :labels, :deployment, :profile_labels

    def create_profile
      req = Cloudprofiler::CreateProfileRequest.new(deployment: deployment, profile_type: PROFILE_TYPES.keys)
      debug_log('creating profile')
      @profiler.create_profile("projects/#{deployment.project_id}", req)
    end

    def update_profile(profile)
      debug_log('updating profile')
      @profiler.patch_project_profile(profile.name, profile)
      debug_log('profile updated')
    end

    # parse_duration converts duration-as-a-string, as returned by the Profiler
    # API, to a duration in seconds. Can't find any documentation on the format,
    # and only have the single example "10s" to go on. If the duration can't be
    # parsed then it returns 10.
    def parse_duration(duration)
      m = /^(\d+)s$/.match(duration)
      return 10 if m.nil?

      Integer(m[1])
    end

    # start will begin creating profiles in a background thread, looping
    # forever. Exceptions are rescued and logged, and retries are made with
    # exponential backoff.
    def start
      return if !@thread.nil? && @thread.alive?

      @thread = Thread.new do
        delay = @min_iteration_time
        loop do
          start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          begin
            profile = create_profile
            profile_and_upload(profile)
          rescue StandardError => e
            delay *= 2 + rand / 2
            puts "Cloud Profiler agent encountered error, will retry: #{e}"
          else
            delay = @min_iteration_time
          end

          elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
          delay = [delay, @max_iteration_time].min
          sleep([0, delay - elapsed].max)
        end
      end
    end

    private

    def profile(duration, mode)
      start_time = Time.now
      stackprof = StackProf.run(mode: mode, raw: true) do
        sleep(duration)
      end

      CloudProfilerAgent::PprofBuilder.convert_stackprof(stackprof, start_time, Time.now)
    end

    def profile_and_upload(profile)
      debug_log("profiling #{profile.profile_type} for #{profile.duration}")
      profile.profile_bytes = profile(parse_duration(profile.duration), PROFILE_TYPES.fetch(profile.profile_type))
      update_profile(profile)
    end

    def debug_log(message)
      puts(message) if @debug_logging
    end
  end
end
