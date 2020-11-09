# frozen_string_literal: true

module CloudProfilerAgent
  VERSION = '0.0.1.pre'
  autoload :Agent, 'cloud_profiler_agent/agent'
  autoload :PprofBuilder, 'cloud_profiler_agent/pprof_builder'
end

module Perftools
  autoload :Profiles, 'profile_pb'
end
