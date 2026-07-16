# frozen_string_literal: true

# Meta-protocol response<->key correlation test
#
# WHAT THIS TEST PROVES:
#   The Dalli meta-protocol `get` reads its response frame *positionally* from
#   the socket — it calls ResponseProcessor#meta_get_with_value which does
#   `read_line` and accepts whatever VA/EN/HD frame arrives next.  There is NO
#   correlation between the response frame and the key that was requested.
#
#   If a complete, well-formed `VA` frame for request A is still buffered in
#   the socket when request B is issued, Dalli will consume A's frame as B's
#   response and return A's value for B's key — without raising any error.
#
#   The mock simulates a server that, for request A (mg key_A v), emits an `EN`
#   (miss) *immediately followed by* a complete `VA` frame containing value_A.
#   Dalli reads the `EN`, returns nil for key_A, and the `VA` frame remains in
#   the socket buffer.  When request B (mg key_B v) is issued, Dalli writes the
#   request but then reads the stale `VA` frame from A as B's response.
#
# WHAT THIS TEST DOES NOT PROVE:
#   It does NOT prove that production memcached ever generated such a stale /
#   duplicate / late `VA` frame.  The mock deliberately crafts this scenario to
#   expose the missing correlation check in Dalli's response handling.

require 'minitest/autorun'
require 'minitest/spec'
require 'socket'
require 'thread'
require 'timeout'
require 'dalli'

describe 'meta-protocol get with a stale/duplicate VA frame (no response<->key correlation)' do
  # ------------------------------------------------------------------
  # Mock TCP server that speaks just enough memcached meta protocol.
  #
  # Synchronization: the Dalli `get` calls are synchronous — they block until
  # the response is fully read from the socket.  So after each `get` returns,
  # the mock has already observed and responded to the corresponding request
  # line.  We rely on this for ordering: no timing-only sleeps.
  #
  # The mock also uses a Queue so the test can block-wait (with a bounded
  # timeout) for the mock thread to have *accepted* the TCP connection, which
  # is the only async step.
  # ------------------------------------------------------------------
  class StaleFrameMockServer
    attr_reader :port

    TIMEOUT = 5

    def initialize
      @server = TCPServer.new('127.0.0.1', 0)
      @port = @server.addr[1]
      @observed_keys = []
      @keys_mutex = Mutex.new
      @keys_cond = ConditionVariable.new
      @accepted = Queue.new
      @done = false
      @thread = Thread.new { run }
    end

    # Block until the mock has accepted a client connection (or timeout).
    def wait_for_accept
      Timeout.timeout(TIMEOUT) { @accepted.pop }
    end

    # Block until the mock has recorded a request line matching `pattern`
    # (or timeout).  Uses a condvar — no timing-only sleeps.
    def wait_for_key(pattern)
      Timeout.timeout(TIMEOUT) do
        @keys_mutex.synchronize do
          until @observed_keys.any? { |k| pattern === k }
            @keys_cond.wait(@keys_mutex, TIMEOUT)
          end
        end
      end
    end

    def observed_keys
      @keys_mutex.synchronize { @observed_keys.dup }
    end

    def shutdown
      @done = true
      @thread.join(TIMEOUT)
    ensure
      @server.close
    end

    private

    def record_key(line)
      @keys_mutex.synchronize do
        @observed_keys << line
        @keys_cond.broadcast
      end
    end

    def run
      client = @server.accept
      @accepted.push(true)
      client.sync = true

      until @done
        line = client.gets("\r\n")
        break if line.nil?

        record_key(line.chomp("\r\n"))

        case line
        when "version\r\n"
          client.write("VERSION 1.6.0_mock\r\n")

        when "mg key_A v\r\n"
          # Reply: EN (miss for A) immediately followed by a complete VA frame
          # containing value_A.  Dalli reads EN and returns nil; the VA frame
          # stays buffered in the socket.
          value_a = 'value_A'
          client.write("EN\r\n")
          client.write("VA #{value_a.bytesize} f0\r\n")
          client.write(value_a)
          client.write("\r\n")

        when "mg key_B v\r\n"
          # B's actual response is a miss (EN).  But Dalli will have already
          # consumed the stale VA frame from A, so this EN stays unread.
          client.write("EN\r\n")

        else
          # Default: respond with EN for any unrecognized mg command
          client.write("EN\r\n") if line.start_with?('mg ')
        end
      end
    rescue StandardError
      # Connection closed or error — expected during shutdown
    ensure
      client&.close
    end
  end

  before do
    @mock = StaleFrameMockServer.new
  end

  after do
    @mock.shutdown
  end

  it 'returns A\'s value for B\'s key when a stale VA frame is buffered (no correlation)' do
    # Use raw: true so Dalli doesn't Marshal.load the value.
    # threadsafe: false to avoid Monitor overhead.
    client = Dalli::Client.new("127.0.0.1:#{@mock.port}", raw: true, threadsafe: false)

    # --- Request A: mg key_A v ---
    # The first get triggers connect (version handshake) then the mg request.
    # Both are synchronous — get blocks until the response is read.
    val_a = client.get('key_A')

    # A should be a miss (EN) — Dalli returns nil.
    # The stale VA frame is now buffered in the socket.
    assert_nil val_a, 'key_A should be a miss (EN), but got a value'

    # --- Request B: mg key_B v ---
    val_b = client.get('key_B')

    # THIS IS THE BUG: Dalli returns value_A (the stale frame from A)
    # as the response to key_B, without raising.
    #
    # We assert that val_b is 'value_A' to demonstrate the root gap.
    # If Dalli ever adds response<->key correlation, this assertion
    # would fail (val_b would be nil), which is the desired end state.
    assert_equal 'value_A', val_b,
                 'Dalli returned the stale VA frame from key_A as key_B\'s value — missing response<->key correlation'

    # Wait for the mock to have observed both mg requests.
    # Because the stale VA frame is already buffered, Dalli's get('key_B')
    # can return before the mock thread has read the mg key_B line — so we
    # must block-wait (condvar, bounded timeout) for the mock to record it.
    @mock.wait_for_key(/mg key_A/)
    @mock.wait_for_key(/mg key_B/)

    # Verify the mock observed both keys on the same connection.
    keys = @mock.observed_keys.select { |k| k.start_with?('mg ') }
    assert_includes keys, 'mg key_A v', 'mock should have observed mg key_A'
    assert_includes keys, 'mg key_B v', 'mock should have observed mg key_B'

    # Sanity: Dalli did NOT raise — we reached this point.
    # (If it had raised, the test would have errored before here.)

    client.close
  end
end
