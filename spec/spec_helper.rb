$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'ffi/hiredis_vip/core'

require 'rubygems'
require 'bundler'
Bundler.require(:default, :development, :test)

require 'minitest/mock'
require 'minitest/spec'
require 'minitest/autorun'
require 'mocha/api'
