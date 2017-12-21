module Semian
  class CircuitBreaker #:nodoc:
    extend Forwardable

    def_delegators :@state, :closed?, :open?, :half_open?

    attr_reader :name

    def initialize(name, exceptions:, success_threshold:, error_threshold:, error_timeout:, implementation:, dryrun:)
      @name = name.to_sym
      @success_count_threshold = success_threshold
      @error_count_threshold = error_threshold
      @error_timeout = error_timeout
      @exceptions = exceptions
      @dryrun = dryrun

      @errors = implementation::Error.new
      @successes = implementation::Integer.new
      @state = implementation::State.new
    end

    # Conditions to be sure with dryrun -
    # In open state should not call mark_failed, mark_success.
    # In closed state Errors should be reset when there only few failures which are followed by a success.
    # Success threshold increment and state transition to closed should only be done in half_open state.

    def acquire
      half_open if open? && error_timeout_expired?

      unless request_allowed?
        if @dryrun
          Semian.logger.info("Dryrun message: Throwing Open Circuit Error for [#{@name}]")
        else
          raise OpenCircuitError
        end
      end

      result = nil
      begin
        result = yield
      rescue *@exceptions => error
        mark_failed(error) unless open?
        raise error
      else
        mark_success unless open?
      end
      result
    end

    def request_allowed?
      closed? ||
        half_open? ||
        # The circuit breaker is officially open, but it will transition to half-open on the next attempt.
        (open? && error_timeout_expired?)
    end

    def mark_failed(_error)
      @errors.increment
      Semian.logger.info("Errors count is #{@errors.value}. Current state is #{@state.value}. Marking resource failure in Semian for [#{@name}]- #{_error.class.name} : #{_error.message}")
      set_last_error_time
      if closed?
        open if error_threshold_reached?
      elsif half_open?
        open
      end
    end

    def mark_success
      @errors.reset
      return unless half_open?
      @successes.increment
      Semian.logger.info("Incrementing success. Success count is #{@successes.value}")
      close if success_threshold_reached?
    end

    def reset
      @errors.reset
      @successes.reset
      close
    end

    def destroy
      @errors.destroy
      @successes.destroy
      @state.destroy
    end

    private

    def close
      log_state_transition(:closed, Time.now)
      @state.close
      @errors.reset
      @successes.reset # Bug fix for log_state_transition.
    end

    def open
      log_state_transition(:open, Time.now)
      @state.open
      #@errors.reset # Not needed, the next state is half_open and reset happens there.
    end

    def half_open
      log_state_transition(:half_open, Time.now)
      @state.half_open
      @errors.reset
      @successes.reset
    end

    def success_threshold_reached?
      @successes.value >= @success_count_threshold
    end

    def error_threshold_reached?
      @errors.value >= @error_count_threshold
    end

    def error_timeout_expired?
      return false unless @errors.last_error_time
      Time.at(@errors.last_error_time) + @error_timeout < Time.now
    end

    def set_last_error_time(time: Time.now)
      @errors.last_error_at(time.to_i)
    end

    def log_state_transition(new_state, occur_time)
      return if @state.nil? || new_state == @state.value

      str = "[#{self.class.name}] State transition for [#{@name}] from #{@state.value} to #{new_state} at #{occur_time}."
      str << " success_count=#{@successes.value} error_count=#{@errors.value}"
      str << " success_count_threshold=#{@success_count_threshold} error_count_threshold=#{@error_count_threshold}"
      str << " error_timeout=#{@error_timeout} error_last_at=\"#{@errors.last_error_time ? Time.at(@errors.last_error_time) : ''}\""
      Semian.logger.info(str)
    end
  end
end
