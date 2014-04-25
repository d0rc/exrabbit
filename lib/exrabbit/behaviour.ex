defmodule Exrabbit.Subscriber do
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour :gen_server
      import Exrabbit.Utils

      def init(args) do
        amqp = connect args
        channel = channel amqp
        case {args[:queue], args[:exchange]} do
          {nil, nil} -> raise "You should either choose exchange or queue for rabbitmq subscription"
          {_, nil}   -> subscribe channel, args[:queue]
          {nil, _}   ->
            queue = declare_queue channel
            bind_queue channel, queue, args[:exchange]
            subscribe channel, queue 
        end
        amqp_monitor = :erlang.monitor :process, amqp
        channel_monitor = :erlang.monitor :process, channel
        { :ok, [connection: amqp, channel: channel, amqp_monitor: amqp_monitor, channel_monitor: channel] }
      end

      def handle_call(_request, _from, state) do
        { :noreply, state }
      end

      def handle_info(message = {:'DOWN', monitor_ref, type, object, info}, state) do
        amqp_monitor = state[:amqp_monitor]
        channel_monitor = state[:channel_monitor]
        case monitor_ref do
          ^amqp_monitor ->
            Exrabbit.Utils.channel_close state[:channel]
          ^channel_monitor ->
            Exrabbit.Utils.disconnect state[:amqp]
        end
        raise "Exrabit.Subscriber: somebody died, we should do it too..."
      end
      def handle_info(_msg, state) do
        handle_message(_msg, state)
        { :noreply, state }
      end

      def handle_cast(_msg, state) do
        { :noreply, state }
      end

      def terminate(_reason, _state) do
        :ok
      end

      def code_change(_old, state, _extra) do
        { :ok, state }
      end

      defoverridable [init: 1, handle_call: 3, handle_info: 2,
        handle_cast: 2, terminate: 2, code_change: 3]
    end
  end
end