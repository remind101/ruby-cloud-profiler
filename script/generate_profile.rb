#!/usr/bin/env ruby
# frozen_string_literal: true

require 'prime'
require 'stackprof'
require 'pp'

StackProf.run(mode: :cpu, raw: true, interval: 100, out: 'spec/cpu.stackprof') do
  (1..1000).each { |i| Prime.prime_division(i) }
end

StackProf.run(mode: :wall, raw: true, interval: 100, out: 'spec/wall.stackprof') do
  sleep(1)
end

StackProf.run(mode: :object, raw: true, interval: 100, out: 'spec/object.stackprof') do
  (1..1000).each { |i| Prime.prime_division(i) }
end
