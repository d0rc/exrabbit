defmodule Exrabbit.Mixfile do
  use Mix.Project

  def project do
    [ app: :exrabbit,
      version: "0.0.4",
      deps: deps ]
  end

  def application do
    [
      mod: { Exrabbit, [] },
      applications: [:amqp_client, :jazz]
    ]
  end

  defp deps do
    [
      { :amqp_client, github: "d0rc/amqp_client" },
      { :jazz, github: "meh/jazz"}
    ]
  end
end
