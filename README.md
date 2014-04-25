# Exrabbit

Edited to handle more option in exchanges / queues. Major work done by d0rc


Easy way to get a queue/exchange worker:


```elixir
import Exrabbit.Ractor

amqp_worker TestQ, queue: "testQ" do
  on_json json do
    IO.puts "JSON: #{inspect json}"
  end
  on_binary <<"hello">> do
    IO.puts "Hello-hello from MQ"
  end
  on_binary text do
    IO.puts "Some random binary: #{inspect text}"
  end
end
```


Checking if message was published:


```elixir
publish(channel, exchange, routing_key, message, :wait_confirmation)
```


Workflow to send message:


```elixir
amqp = Exrabbit.Utils.connect
channel = Exrabbit.Utils.channel amqp
Exrabbit.Utils.publish channel, "testExchange", "", "hello, world"
```


To get messages, almost the same, but functions are


```elixir
Exrabbit.Utils.get_messages channel, "testQueue"
case Exrabbit.Utils.get_messages_ack channel, "testQueue" do
	nil -> IO.puts "No messages waiting"
	[tag: tag, content: message] -> 
		IO.puts "Got message #{message}"
		Exrabbit.Utils.ack tag # acking message
end
```


Please consult: http://www.rabbitmq.com/erlang-client-user-guide.html#returns to find out how to write gen_server consuming messages.


```elixir
defmodule Consumer do
  use GenServer.Behaviour
  import Exrabbit.Utils
  require Lager

  def start_link, do: :gen_server.start_link(Consumer, [], [])

  def init(_opts) do
    amqp = connect
    channel = channel amqp
    subscribe channel, "testQ"
    {:ok, [connection: amqp, channel: channel]}
  end

  def handle_info(request, state) do
    case parse_message(request) do
      nil -> Lager.info "Got nil message"
      {tag, payload} ->
        Lager.info "Got message with tag #{tag} and payload #{payload}"
        ack state[:channel], tag
    end
    { :noreply, state}
  end
end
```


Or same, using behaviours:


```elixir
defmodule Test do 
  use Exrabbit.Subscriber 

  def handle_message(msg, _state) do 
    case parse_message(msg) do 
      nil -> 
        IO.puts "Nil"
      {tag,json} -> 
        IO.puts "Msg: #{json}"
        ack _state[:channel], tag 
      {tag,json,reply_to} -> 
        IO.puts "For RPC messaging: #{json}"
        publish(_state[:channel], "", reply_to, "#{json}") # Return ECHO
        ack _state[:channel], tag 
    end  
  end 
end

:gen_server.start Test, [queue: "testQ"], []   
```




