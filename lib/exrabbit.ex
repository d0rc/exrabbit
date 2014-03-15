defmodule Exrabbit do
  use Application.Behaviour

  def start(_type, _args) do
    Exrabbit.Supervisor.start_link
  end
end

defmodule Exrabbit.Framing do
	defrecord :'P_basic', Record.extract(:'P_basic', from_lib: "rabbit_common/include/rabbit_framing.hrl")
end

defmodule Exrabbit.Defs do
	require Exrabbit.Framing

	@rabbit_common "rabbit_common/include/rabbit_framing.hrl"
	@rabbit_framing "rabbit_common/include/rabbit_framing.hrl"
	@amqp_client "amqp_client/include/amqp_client.hrl"

	defrecord :amqp_params_network, Record.extract(:amqp_params_network, from_lib: @amqp_client)
	defrecord :'basic.publish', Record.extract(:'basic.publish', from_lib: @rabbit_framing)
	lc record inlist [
		:'queue.declare',
		:'queue.declare_ok',
		:'queue.bind',
		:'queue.bind_ok',
		:'basic.get', 
		:'basic.get_ok', 
		:'basic.get_empty', 
		:'basic.ack',
		:'basic.consume',
		:'basic.consume_ok',
		:'basic.cancel',
		:'basic.cancel_ok',
		:'basic.deliver'] do
			defrecord record, Record.extract(record, from_lib: @rabbit_common)
	end
	defrecord :amqp_msg, [props: :'P_basic'[], payload: ""]
end

defmodule Exrabbit.Utils do
	defp get_host do
		'localhost'
	end
	def connect, do: connect([])
	def connect(args) do
		connection_settings = [
			username: "guest", 
			password: "guest", 
			host: get_host,
			virtual_host: "/",
			heartbeat: 1]
		|> Enum.map(fn {name, value} ->
			case args[name] do
				nil -> {name, value}
				arg_value -> {name, arg_value}
			end
		end)
		
		{:ok, connection} = :amqp_connection.start(:amqp_params_network[
			username: connection_settings[:username],
			password: connection_settings[:password],
			host: connection_settings[:host],
			virtual_host: connection_settings[:virtual_host],
			heartbeat: connection_settings[:heartbeat]
		])
		connection
	end

	defp get_payload(:amqp_msg[payload: payload]), do: payload

	def channel_open(connection), do: channel(connection)
	def channel(connection) when is_pid(connection) do
		{:ok, channel} = :amqp_connection.open_channel(connection)
		channel
	end
	def disconnect(connection), do: :amqp_connection.close(connection)
	def channel_close(channel), do: :amqp_channel.close(channel)

	def publish(channel, exchange, routing_key, message) do
		:amqp_channel.call channel, :'basic.publish'[exchange: exchange, routing_key: routing_key], :amqp_msg[payload: message]
	end
	def ack(channel, tag) do
		:amqp_channel.call channel, :'basic.ack'[delivery_tag: tag]
	end
	def get_messages(channel, queue), do: get_messages(channel, queue, true)
	def get_messages_ack(channel, queue), do: get_messages(channel, queue, false)
	def get_messages(channel, queue, true) do
		case :amqp_channel.call channel, :'basic.get'[queue: queue, no_ack: true] do
			{:'basic.get_ok'[], content} -> get_payload(content)
			:'basic.get_empty'[] -> nil
		end
	end
	def get_messages(channel, queue, false) do
		case :amqp_channel.call channel, :'basic.get'[queue: queue, no_ack: false] do
			{:'basic.get_ok'[delivery_tag: tag], content} -> [tag: tag, content: get_payload(content)]
			:'basic.get_empty'[] -> nil
		end
	end

	def declare_queue(channel) do
		:'queue.declare_ok'[queue: queue] = :amqp_channel.call channel, :'queue.declare'[auto_delete: true]
		queue
	end
	def declare_queue(channel, queue) do
		:'queue.declare_ok'[queue: queue] = :amqp_channel.call channel, :'queue.declare'[queue: queue]
		queue
	end
	def bind_queue(channel, queue, exchange, key \\ "") do
		:'queue.bind_ok'[] = :amqp_channel.call channel, :'queue.bind'[queue: queue, exchange: exchange, routing_key: key]		
	end

	def parse_message({:'basic.deliver'[delivery_tag: tag], :amqp_msg[payload: payload]}), do: {tag, payload}
	def parse_message(:'basic.cancel_ok'[]), do: nil
	def parse_message(:'basic.consume_ok'[]), do: nil

	def subscribe(channel, queue), do: subscribe(channel, queue, self)
	def subscribe(channel, queue, pid) do
		sub = :'basic.consume'[queue: queue]
		:'basic.consume_ok'[consumer_tag: consumer_tag] = :amqp_channel.subscribe channel, sub, pid
		consumer_tag
	end
	def unsubscribe(channel, consumer_tag) do
		:amqp_channel.call channel, :'basic.cancel'[consumer_tag: consumer_tag]
	end
end

