defmodule Exrabbit.DSL do
	defmacro amqp_worker(name, opts, code) do
		quote do
			defmodule unquote(name) do
				import Exrabbit.Utils
				import Exrabbit.DSL
				def start_link() do
					:gen_server.start_link __MODULE__, [], []
				end
				def init(_) do
					IO.puts "Connecting: #{inspect unquote(opts) || :application.get_all_env(unquote(opts[:config_name]))}"
					config = case unquote(opts)[:config_name] do
						nil -> unquote(opts)
						config_name  -> :application.get_all_env(config_name)
					end
					amqp = connect(config)
					channel = channel(amqp)
					cond do
						queue = config[:queue] -> subscribe(channel, queue)
						exchange = config[:exchange] ->
							queue = declare_queue(channel)
							bind_queue(channel, queue, exchange)
							subscribe(channel, queue)
						true ->
							raise MissConfiguration
					end
			        amqp_monitor = :erlang.monitor :process, amqp
			        channel_monitor = :erlang.monitor :process, channel
					{:ok, 
						%{
							connection: amqp, 
							channel: channel, 
							amqp_monitor: amqp_monitor, 
							channel_monitor: channel
						}
					}
				end
				defp maybe_call_connection_established(state) do
					case Kernel.function_exported?(__MODULE__, :on_open, 1) do
						true -> :erlang.apply(__MODULE__, :on_open, [state])
						false -> :ok
					end
				end
				defp maybe_call_listener(tag, msg, state, reply_to \\ nil) do
					case Jazz.decode(msg) do
						{:ok, data} -> handle_data(data)
						_           -> handle_data(msg)
					end
				end
				def handle_info(message = {:'DOWN', monitor_ref, type, object, info}, state = %{
						amqp_monitor: amqp_monitor,
						channel_monitor: channel_monitor,
						channel: channel,
						amqp: amqp
					}) do
			        case monitor_ref do
						^amqp_monitor ->
				            Exrabbit.Utils.channel_close channel
						^channel_monitor ->
				            Exrabbit.Utils.disconnect amqp
			        end
			        raise "#{inspect __MODULE__}: somebody died, we should do it too..."
				end
				def handle_info(msg, state) do
					res = case parse_message(msg) do
						nil -> 
							maybe_call_connection_established(state)
						{tag, data} -> 
							{:ok, tag, maybe_call_listener(tag, data, state)}
						{tag, data, reply_to} ->
							{:ok, tag, maybe_call_listener(tag, data, state, reply_to)}
					end
					case res do
						{:ok, tag, :ok} -> 
							ack state[:channel], tag
						{:ok, tag, _}   -> 
							nack state[:channel], tag
						_ -> :ok
					end
					{:noreply, state}
				end
				unquote(code)
			end
		end
	end
	@doc """
		in case `on` returns :ok - message is acked, it's nacked otherwise
	"""
	defmacro on(match, code) do
		case match do
			{:when, _, [arg, when_code]} -> 
				quote do
					def handle_data(unquote(arg)) when unquote(when_code), unquote(code)
				end
			_ ->
				quote do
					def handle_data(unquote(match)), unquote(code)
				end
		end
	end
end
