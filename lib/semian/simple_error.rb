require 'thread'

module Semian
  module Simple
    class Error #:nodoc:
      attr_accessor :value, :last_error_time

      def initialize
        reset
      end

      def increment(val = 1)
        @value += val
      end

      def reset
        @value = 0
        @last_error_time = nil
      end

      def destroy
        reset
      end

      def last_error_at(time)
        @last_error_time = time
      end

    end
  end

  module ThreadSafe
    class Error < Simple::Error
      def initialize(*)
        super
        @lock = Mutex.new
      end

      def increment(*)
        @lock.synchronize { super }
      end

      def last_error_at(time)
        @lock.synchronize { super(time) }
      end
    end
  end
end
