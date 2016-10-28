require "ffi/hiredis_vip/core/version"

require 'rubygems'
require 'ffi'

module FFI
  module HiredisVip
    module Core
      extend ::FFI::Library
      ffi_lib_flags :now, :global

      ##
      # ffi-rzmq-core for reference
      #
      # https://github.com/chuckremes/ffi-rzmq-core/blob/master/lib/ffi-rzmq-core/libzmq.rb
      #
      begin
        # bias the library discovery to a path inside the gem first, then
        # to the usual system paths
        inside_gem = File.join(File.dirname(__FILE__), '..', '..', 'ext')
        local_path = FFI::Platform::IS_WINDOWS ? ENV['PATH'].split(';') : ENV['PATH'].split(':')
        env_path = [ ENV['HIREDIS_VIP_LIB_PATH'] ].compact
        rbconfig_path = RbConfig::CONFIG["libdir"]
        homebrew_path = nil

        # RUBYOPT set by RVM breaks 'brew' so we need to unset it.
        rubyopt = ENV.delete('RUBYOPT')

        begin
          stdout, stderr, status = Open3.capture3("brew", "--prefix")
          homebrew_path  = if status.success?
                             "#{stdout.chomp}/lib"
                           else
                             '/usr/local/homebrew/lib'
                           end
        rescue
          # Homebrew doesn't exist
        end

        # Restore RUBYOPT after executing 'brew' above.
        ENV['RUBYOPT'] = rubyopt

        # Search for libhiredis_vip in the following order...
        HIREDIS_VIP_LIB_PATHS = ([inside_gem] + env_path + local_path + [rbconfig_path] + [
          '/usr/local/lib', '/opt/local/lib', homebrew_path, '/usr/lib64'
        ]).compact.map{|path| "#{path}/libhiredis_vip.#{FFI::Platform::LIBSUFFIX}"}
        ffi_lib(HIREDIS_VIP_LIB_PATHS + %w{libhiredis_vip})

      rescue LoadError
        if HIREDIS_VIP_LIB_PATHS.any? {|path|
          File.file? File.join(path, "libhiredis_vip.#{FFI::Platform::LIBSUFFIX}")}
          warn "Unable to load this gem. The libhiredis_vip library exists, but cannot be loaded."
          warn "Set HIREDIS_VIP_LIB_PATH if custom load path is desired"
          warn "If this is Windows:"
          warn "-  Check that you have MSVC runtime installed or statically linked"
          warn "-  Check that your DLL is compiled for #{FFI::Platform::ADDRESS_SIZE} bit"
        else
          warn "Unable to load this gem. The libhiredis_vip library (or DLL) could not be found."
          warn "Set HIREDIS_VIP_LIB_PATH if custom load path is desired"
          warn "If this is a Windows platform, make sure libhiredis_vip.dll is on the PATH."
          warn "If the DLL was built with mingw, make sure the other two dependent DLLs,"
          warn "libgcc_s_sjlj-1.dll and libstdc++6.dll, are also on the PATH."
          warn "For non-Windows platforms, make sure libhiredis_vip is located in this search path:"
          warn HIREDIS_VIP_LIB_PATHS.inspect
        end
        raise LoadError, "The libhiredis_vip library (or DLL) could not be loaded"
      end

      RedisClusterFlags = enum :HIRCLUSTER_FLAG_NULL, 0x0,
        :HIRCLUSTER_FLAG_ADD_SLAVE, 0x1000, #/* The flag to decide whether add slave node in redisClusterContext->nodes. This is set in the
        #* least significant bit of the flags field in redisClusterContext. (1000000000000) */
        :HIRCLUSTER_FLAG_ADD_OPENSLOT, 0x2000, #/* The flag to decide whether add open slot for master node. (10000000000000) */
        :HIRCLUSTER_FLAG_ROUTE_USE_SLOTS, 0x4000 # /* The flag to decide whether add open slot for master node. (100000000000000) */

      RedisReplyType = enum :REDIS_REPLY_STRING, 1,
        :REDIS_REPLY_ARRAY, 2,
        :REDIS_REPLY_INTEGER, 3,
        :REDIS_REPLY_NIL, 4,
        :REDIS_REPLY_STATUS, 5,
        :REDIS_REPLY_ERROR, 6

      RedisOkType = enum :REDIS_OK, 0,
        :REDIS_ERR, -1,
        :REDIS_ERR_IO, 1, # /* Error in read or write */
        :REDIS_ERR_OTHER, 2, # /* Everything else... */
        :REDIS_ERR_EOF, 3, # /* End of file */
        :REDIS_ERR_PROTOCOL, 4, # /* Protocol error */
        :REDIS_ERR_OOM, 5, # /* Out of memory */
        :REDIS_ERR_CLUSTER_TOO_MANY_REDIRECT, 6

      class Timeval < FFI::Struct
        layout :tv_sec, :long,
          :tv_usec, :long
      end

      class RedisReply < ::FFI::Struct
        layout :type, ::FFI::HiredisVip::Core::RedisReplyType,
          :integer, :long_long,
          :len, :int,
          :str, :string,
          :elements, :size_t,
          :element, :pointer
      end

      attach_function :freeReplyObject, [:pointer], :void, :blocking => true
      attach_function :redisReplyElement, [:pointer, :size_t], RedisReply.ptr, :blocking => true
      attach_function :redisConnect, [:string, :int], :pointer, :blocking => true
      attach_function :redisReconnect, [:pointer], RedisOkType, :blocking => true # :pointer => redisContext
      attach_function :redisEnableKeepAlive, [:pointer], RedisOkType, :blocking => true # :pointer => redisContext
      attach_function :redisCommand, [:pointer, :string, :varargs], :pointer, :blocking => true
      attach_function :redisFree, [:pointer], :void, :blocking => true # :pointer => redisContext from redisConnect

      attach_function :redisClusterFree, [:pointer], :void, :blocking => true # :pointer => redisClusterContext
      attach_function :redisClusterConnect, [:string, :int], :pointer, :blocking => true # string => addresses, :int => flags
      attach_function :redisClusterConnectWithTimeout, [:string, Timeval.by_value, :int], :pointer, :blocking => true # string => addresses, :timeval => timeout, :int => flags
      attach_function :redisClusterConnectNonBlock, [:string, :int], :pointer, :blocking => true
      attach_function :redisClusterCommand, [:pointer, :string, :varargs], :pointer, :blocking => true
      attach_function :redisClusterSetMaxRedirect, [:pointer, :int], :void, :blocking => true # :pointer => redisContext, :int => max redirect
      attach_function :redisClusterReset, [:pointer], :void, :blocking => true # :pointer => redisClusterContext

      def self.command(connection, command, *args)
        ::FFI::HiredisVip::Core::RedisReply.new(::FFI::AutoPointer.new(::FFI::HiredisVip::Core.redisCommand(connection, command, *args), ::FFI::HiredisVip::Core.method(:freeReplyObject)))
      end

      def self.connect(host, port)
        ::FFI::AutoPointer.new(::FFI::HiredisVip::Core.redisConnect(host, port), ::FFI::HiredisVip::Core.method(:redisFree))
      end

      def self.cluster_command(cluster_context, command, *args)
        ::FFI::AutoPointer.new(::FFI::HiredisVip::Core.redisClusterCommand(cluster_context, command, *args), ::FFI::HiredisVip::Core.method(:freeReplyObject))
      end

      def self.cluster_connect(addresses, flags = nil)
        flags ||= RedisClusterFlags[:HIRCLUSTER_FLAG_ADD_SLAVE] # Not sure what default should be
        ::FFI::AutoPointer.new(::FFI::HiredisVip::Core.redisClusterConnect(addresses, flags), ::FFI::HiredisVip::Core.method(:redisClusterFree))
      end

      # TODO: extract into ffi-hiredis_vip-benchmark
      def self.bench(address = "127.0.0.1", port = 6379)
        require "benchmark"

        conn = connect(address, port)
        n = (ARGV.shift || 20000).to_i

        elapsed = Benchmark.realtime do
          # n sets, n gets
          n.times do |i|
            key = "foo#{i}"
            value = key * 10

            command(conn, "SET %b %b", :string, key, :size_t, key.size, :string, value, :size_t, value.size)
            command(conn, "GET %b", :string, key, :size_t, key.size)
          end
        end

        puts '%.2f Kops' % (2 * n / 1000 / elapsed)
      end

      #def self.test_set_get
      #  connection = FFIHIREDISVIP.redisConnect("127.0.0.1", 6379)

      #  reply_raw = FFIHIREDISVIP.redisCommand(connection, "SET %b %b", :string, "bar", :size_t, 3, :string, "hello", :size_t, 5)
      #  FFIHIREDISVIP.freeReplyObject(reply_raw)

      #  get_reply_raw = FFIHIREDISVIP.redisCommand(connection, "GET bar")
      #  reply = RedisReply.new(get_reply_raw)
      #  puts reply[:str]
      #  FFIHIREDISVIP.freeReplyObject(get_reply_raw)

      #  FFIHIREDISVIP.redisFree(connection)
      #end
    end
  end
end
