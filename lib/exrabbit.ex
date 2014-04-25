require Record

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

	defrecord :amqp_params_network, Record.extract(:amqp_params_network, from_lib: "amqp_client/include/amqp_client.hrl")
	defrecord :'queue.declare', Record.extract(:'queue.declare', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'queue.declare_ok', Record.extract(:'queue.declare_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'queue.bind', Record.extract(:'queue.bind', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'queue.bind_ok', Record.extract(:'queue.bind_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.get', Record.extract(:'basic.get', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.get_ok', Record.extract(:'basic.get_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.get_empty', Record.extract(:'basic.get_empty', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.ack', Record.extract(:'basic.ack', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.consume', Record.extract(:'basic.consume', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.consume_ok', Record.extract(:'basic.consume_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.publish', Record.extract(:'basic.publish', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.cancel', Record.extract(:'basic.cancel', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.cancel_ok', Record.extract(:'basic.cancel_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.deliver', Record.extract(:'basic.deliver', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.declare', Record.extract(:'exchange.declare', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.declare_ok', Record.extract(:'exchange.declare_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.delete', Record.extract(:'exchange.delete', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.delete_ok', Record.extract(:'exchange.delete_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.bind', Record.extract(:'exchange.bind', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.bind_ok', Record.extract(:'exchange.bind_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.unbind', Record.extract(:'exchange.unbind', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'exchange.unbind_ok', Record.extract(:'exchange.unbind_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.qos', Record.extract(:'basic.qos', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.qos_ok', Record.extract(:'basic.qos_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'basic.nack', Record.extract(:'basic.nack', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'confirm.select', Record.extract(:'confirm.select', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	defrecord :'confirm.select_ok', Record.extract(:'confirm.select_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
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


	defp wait_confirmation(root, channel) do
		receive do
			:'basic.ack'[delivery_tag: tag] ->
				send(root, {:message_confirmed, tag})
			:'basic.nack'[delivery_tag: tag] ->
				send(root, {:message_lost, tag})
			after 15000 ->
				send(root, {:confirmation_timeout})
		end
		:amqp_channel.unregister_confirm_handler channel
	end

	def rpc(channel, exchange, routing_key, message, reply_to) do
		:amqp_channel.call channel, :'basic.publish'[exchange: exchange, routing_key: routing_key], :amqp_msg[props: :'P_basic'[reply_to: reply_to], payload: message]
	end

	def rpc(channel, exchange, routing_key, message, reply_to, uuid) do
		:amqp_channel.call channel, :'basic.publish'[exchange: exchange, routing_key: routing_key], :amqp_msg[props: :'P_basic'[reply_to: reply_to, headers: [{"uuid", :longstr, uuid}]  ], payload: message]
	end


	def publish(channel, exchange, routing_key, message) do
		publish(channel, exchange, routing_key, message, :no_confirmation)
	end

	def publish(channel, exchange, routing_key, message, :no_confirmation) do
		:amqp_channel.call channel, :'basic.publish'[exchange: exchange, routing_key: routing_key], :amqp_msg[payload: message]
	end

	def publish(channel, exchange, routing_key, message, :wait_confirmation) do
		root = self
		:'confirm.select_ok'[] = :amqp_channel.call channel, :'confirm.select'[]
		:ok = :amqp_channel.register_confirm_handler channel, spawn fn -> 
			wait_confirmation(root, channel) 
		end
		:ok = publish(channel, exchange, routing_key, message, :no_confirmation)
		receive do
			{:message_confirmed, _data} -> :ok
			{:message_lost, _data} -> {:error, :lost}
			{:confirmation_timeout} -> {:error, :timeout}
		end
	end

	def ack(channel, tag) do
		:amqp_channel.call channel, :'basic.ack'[delivery_tag: tag]
	end
	def nack(channel, tag) do
		:amqp_channel.call channel, :'basic.nack'[delivery_tag: tag]
	end

	def get_messages_ack(channel, queue), do: get_messages(channel, queue, false)
	def get_messages(channel, queue), do: get_messages(channel, queue, true)
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

	def declare_queue_exclusive(channel) do
		:'queue.declare_ok'[queue: queue] = :amqp_channel.call channel, :'queue.declare'[exclusive: true]
		queue
	end

	def declare_queue(channel, queue) do
		:'queue.declare_ok'[queue: queue] = :amqp_channel.call channel, :'queue.declare'[queue: queue]
		queue
	end
	
	def declare_queue(channel, queue, autodelete) do
		:'queue.declare_ok'[queue: queue] = :amqp_channel.call channel, :'queue.declare'[queue: queue, auto_delete: autodelete]
		queue
	end
	
	def declare_queue(channel, queue, autodelete, durable) do
		:'queue.declare_ok'[queue: queue] = :amqp_channel.call channel, :'queue.declare'[queue: queue, auto_delete: autodelete, durable: durable]
		queue
	end
	

	def declare_exchange(channel, exchange) do
		:'exchange.declare_ok'[] = :amqp_channel.call channel, :'exchange.declare'[exchange: exchange, type: "fanout", auto_delete: true]
		exchange
	end
	
	def declare_exchange(channel, exchange, type, autodelete) do
		:'exchange.declare_ok'[] = :amqp_channel.call channel, :'exchange.declare'[exchange: exchange, type: type, auto_delete: autodelete]
		exchange
	end

	def declare_exchange(channel, exchange, type, autodelete, durable) do
		:'exchange.declare_ok'[] = :amqp_channel.call channel, :'exchange.declare'[exchange: exchange, type: type, auto_delete: autodelete, durable: durable]
		exchange
	end
	
	def set_qos(channel, prefetch_count \\ 1) do
		:'basic.qos_ok'[] = :amqp_channel.call channel, :'basic.qos'[prefetch_count: prefetch_count]
		prefetch_count
	end

	def bind_queue(channel, queue, exchange, key \\ "") do
		:'queue.bind_ok'[] = :amqp_channel.call channel, :'queue.bind'[queue: queue, exchange: exchange, routing_key: key]		
	end
  
  	def parse_message({:'basic.deliver'[delivery_tag: tag], :amqp_msg[props: :'P_basic'[reply_to: nil], payload: payload]}), do: {tag, payload}
	def parse_message({:'basic.deliver'[delivery_tag: tag], :amqp_msg[props: :'P_basic'[reply_to: reply_to], payload: payload]}), do: {tag, payload, reply_to}
	def parse_message({:'basic.deliver'[delivery_tag: tag], :amqp_msg[payload: payload]}), do: {:message,tag, payload}
	def parse_message(:'basic.cancel_ok'[]), do: nil
	def parse_message(:'basic.consume_ok'[]), do: nil
	def parse_message(:'exchange.declare_ok'[]), do: nil
	def parse_message(:'exchange.delete_ok'[]), do: nil
	def parse_message(:'exchange.bind_ok'[]), do: nil
	def parse_message(:'exchange.unbind_ok'[]), do: nil
	def parse_message(:'basic.qos_ok'[]), do: nil
		
	def parse_message(:'basic.cancel'[]) do
		{:error, :queue_killed}
	end
	
	def parse_message(_) do
		{:error, :unknown_message}
	end

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

