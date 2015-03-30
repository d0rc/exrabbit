require Record

defmodule Exrabbit do
  use Application

  def start(_type, _args) do
    Exrabbit.Supervisor.start_link
  end
end

defmodule Exrabbit.Framing do
	Record.defrecord :pbasic, :'P_basic', Record.extract(:'P_basic', from_lib: "rabbit_common/include/rabbit_framing.hrl")
end

defmodule Exrabbit.Defs do
	require Exrabbit.Framing

	Record.defrecord :amqp_params_network, Record.extract(:amqp_params_network, from_lib: "amqp_client/include/amqp_client.hrl")
	Record.defrecord :queue_declare, :'queue.declare', Record.extract(:'queue.declare', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :queue_declare_ok, :'queue.declare_ok', Record.extract(:'queue.declare_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :queue_bind, :'queue.bind', Record.extract(:'queue.bind', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :queue_bind_ok, :'queue.bind_ok', Record.extract(:'queue.bind_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_get, :'basic.get', Record.extract(:'basic.get', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_get_ok, :'basic.get_ok', Record.extract(:'basic.get_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_get_empty, :'basic.get_empty', Record.extract(:'basic.get_empty', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_ack, :'basic.ack', Record.extract(:'basic.ack', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_consume, :'basic.consume', Record.extract(:'basic.consume', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_consume_ok, :'basic.consume_ok', Record.extract(:'basic.consume_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_publish, :'basic.publish', Record.extract(:'basic.publish', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_cancel, :'basic.cancel', Record.extract(:'basic.cancel', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_cancel_ok, :'basic.cancel_ok', Record.extract(:'basic.cancel_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_deliver, :'basic.deliver', Record.extract(:'basic.deliver', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_declare, :'exchange.declare', Record.extract(:'exchange.declare', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_declare_ok, :'exchange.declare_ok', Record.extract(:'exchange.declare_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_delete, :'exchange.delete', Record.extract(:'exchange.delete', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_delete_ok, :'exchange.delete_ok', Record.extract(:'exchange.delete_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_bind, :'exchange.bind', Record.extract(:'exchange.bind', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_bind_ok, :'exchange.bind_ok', Record.extract(:'exchange.bind_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_unbind, :'exchange.unbind', Record.extract(:'exchange.unbind', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :exchange_unbind_ok, :'exchange.unbind_ok', Record.extract(:'exchange.unbind_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_qos, :'basic.qos', Record.extract(:'basic.qos', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_qos_ok, :'basic.qos_ok', Record.extract(:'basic.qos_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :basic_nack, :'basic.nack', Record.extract(:'basic.nack', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :confirm_select, :'confirm.select', Record.extract(:'confirm.select', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :confirm_select_ok, :'confirm.select_ok', Record.extract(:'confirm.select_ok', from_lib: "rabbit_common/include/rabbit_framing.hrl")
	Record.defrecord :amqp_msg, [props: Exrabbit.Framing.pbasic(), payload: ""]

end

defmodule Exrabbit.Utils do
	import Exrabbit.Defs
	import Exrabbit.Framing

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
		
		params = amqp_params_network(
			username: connection_settings[:username],
			password: connection_settings[:password],
			host: connection_settings[:host],
			virtual_host: connection_settings[:virtual_host],
			heartbeat: connection_settings[:heartbeat]
		)
		:amqp_connection.start(params)
	end

	defp get_payload(amqp_msg(payload: payload)), do: payload

	def channel_open(connection), do: channel(connection)
	def channel(connection) when is_pid(connection) do
		{:ok, channel} = :amqp_connection.open_channel(connection)
		channel
	end
	def disconnect(connection), do: :amqp_connection.close(connection)
	def channel_close(channel), do: :amqp_channel.close(channel)


	defp wait_confirmation(root, channel) do
		receive do
			basic_ack(delivery_tag: tag) ->
				send(root, {:message_confirmed, tag})
			basic_nack(delivery_tag: tag) ->
				send(root, {:message_lost, tag})
			after 15000 ->
				send(root, {:confirmation_timeout})
		end
		:amqp_channel.unregister_confirm_handler channel
	end

	def rpc(channel, exchange, routing_key, message, reply_to) do
		:amqp_channel.call channel, basic_publish(exchange: exchange, routing_key: routing_key), amqp_msg(props: pbasic(reply_to: reply_to), payload: message)
	end

	def rpc(channel, exchange, routing_key, message, reply_to, uuid) do
		:amqp_channel.call channel, basic_publish(exchange: exchange, routing_key: routing_key), amqp_msg(props: pbasic(reply_to: reply_to, headers: ({"uuid", :longstr, uuid})  ), payload: message)
	end


	def publish(channel, exchange, routing_key, message) do
		publish(channel, exchange, routing_key, message, :no_confirmation)
	end

	def publish(channel, exchange, routing_key, message, :no_confirmation) do
		:amqp_channel.call channel, basic_publish(exchange: exchange, routing_key: routing_key), amqp_msg(payload: message)
	end

	def publish(channel, exchange, routing_key, message, :wait_confirmation) do
		root = self
		confirm_select_ok() = :amqp_channel.call channel, confirm_select()
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
		:amqp_channel.call channel, basic_ack(delivery_tag: tag)
	end
	def nack(channel, tag) do
		:amqp_channel.call channel, basic_nack(delivery_tag: tag)
	end

	def get_messages_ack(channel, queue), do: get_messages(channel, queue, false)
	def get_messages(channel, queue), do: get_messages(channel, queue, true)
	def get_messages(channel, queue, true) do
		case :amqp_channel.call channel, basic_get(queue: queue, no_ack: true) do
			{basic_get_ok(), content} -> get_payload(content)
			basic_get_empty() -> nil
		end
	end

	def get_messages(channel, queue, false) do
		case :amqp_channel.call channel, basic_get(queue: queue, no_ack: false) do
			{basic_get_ok(delivery_tag: tag), content} -> [tag: tag, content: get_payload(content)]
			basic_get_empty() -> nil
		end
	end

	def declare_queue(channel) do
		queue_declare_ok(queue: queue) = :amqp_channel.call channel, queue_declare(auto_delete: true)
		queue
	end

	def declare_queue_exclusive(channel) do
		queue_declare_ok(queue: queue) = :amqp_channel.call channel, queue_declare(exclusive: true)
		queue
	end

	def declare_queue(channel, queue) do
		queue_declare_ok(queue: queue) = :amqp_channel.call channel, queue_declare(queue: queue)
		queue
	end
	
	def declare_queue(channel, queue, autodelete) do
		queue_declare_ok(queue: queue) = :amqp_channel.call channel, queue_declare(queue: queue, auto_delete: autodelete)
		queue
	end
	
	def declare_queue(channel, queue, autodelete, durable) do
		queue_declare_ok(queue: queue) = :amqp_channel.call channel, queue_declare(queue: queue, auto_delete: autodelete, durable: durable)
		queue
	end

	def declare_exchange(channel, exchange) do
		exchange_declare_ok() = :amqp_channel.call channel, exchange.declare(exchange: exchange, type: "fanout", auto_delete: true)
		exchange
	end
	
	def declare_exchange(channel, exchange, type, autodelete) do
		exchange_declare_ok() = :amqp_channel.call channel, exchange_declare(exchange: exchange, type: type, auto_delete: autodelete)
		exchange
	end

	def declare_exchange(channel, exchange, type, autodelete, durable) do
		exchange_declare_ok() = :amqp_channel.call channel, exchange_declare(exchange: exchange, type: type, auto_delete: autodelete, durable: durable)
		exchange
	end
	
	def set_qos(channel, prefetch_count \\ 1) do
		basic_qos_ok() = :amqp_channel.call channel, basic_qos(prefetch_count: prefetch_count)
		prefetch_count
	end

	def bind_queue(channel, queue, exchange, key \\ "") do
		queue_bind_ok() = :amqp_channel.call channel, queue_bind(queue: queue, exchange: exchange, routing_key: key)	
	end
  
  	def parse_message({basic_deliver(delivery_tag: tag), amqp_msg(props: pbasic(reply_to: nil), payload: payload)}), do: {tag, payload}
	def parse_message({basic_deliver(delivery_tag: tag), amqp_msg(props: pbasic(reply_to: reply_to), payload: payload)}), do: {tag, payload, reply_to}
	def parse_message({basic_deliver(delivery_tag: tag), amqp_msg(payload: payload)}), do: {:message,tag, payload}
	def parse_message(basic_cancel_ok()), do: nil
	def parse_message(basic_consume_ok()), do: nil
	def parse_message(exchange_declare_ok()), do: nil
	def parse_message(exchange_delete_ok()), do: nil
	def parse_message(exchange_bind_ok()), do: nil
	def parse_message(exchange_unbind_ok()), do: nil
	def parse_message(basic_qos_ok()), do: nil
		
	def parse_message(basic_cancel()) do
		{:error, :queue_killed}
	end
	
	def parse_message(_) do
		{:error, :unknown_message}
	end

	def subscribe(channel, opts = %{queue: queue, noack: noack}) do
		sub = basic_consume(queue: queue, no_ack: noack)
		basic_consume_ok(consumer_tag: consumer_tag) = :amqp_channel.subscribe channel, sub, self
		consumer_tag
	end
	def subscribe(channel, queue), do: subscribe(channel, queue, self)

	def subscribe(channel, queue, pid) do
		sub = basic_consume(queue: queue)
		basic_consume_ok(consumer_tag: consumer_tag) = :amqp_channel.subscribe channel, sub, pid
		consumer_tag
	end

	def unsubscribe(channel, consumer_tag) do
		:amqp_channel.call channel, basic_cancel(consumer_tag: consumer_tag)
	end
			
end

