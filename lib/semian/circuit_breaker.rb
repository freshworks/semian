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

    # Conditions to check with dryrun
    # In open state should not calle mark_failed, mark_success.
    # mark_success should only be called during half_open state.

    def acquire
      half_open if open? && error_timeout_expired?

      unless request_allowed?
        if @dryrun
          Semian.logger.info("Throwing Open Circuit Error")
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
      Semian.logger.info("Marking resource failure in Semian - #{_error.class.name} : #{_error.message}")
      @errors.increment
      set_last_error_time
      if closed?
        open if error_threshold_reached?
      elsif half_open?
        open
      end
    end

    def mark_success
      return unless half_open?
      @errors.reset
      @successes.increment
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
      #@errors.reset # Not needed because the next state it going to be half_open and reset there.
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

      str = "[#{self.class.name}] State transition from #{@state.value} to #{new_state} at #{occur_time}."
      str << " success_count=#{@successes.value} error_count=#{@errors.value}"
      str << " success_count_threshold=#{@success_count_threshold} error_count_threshold=#{@error_count_threshold}"
      str << " error_timeout=#{@error_timeout} error_last_at=\"#{@errors.last_error_time ? Time.at(@errors.last_error_time) : ''}\""
      Semian.logger.info(str)
    end
  end
end
