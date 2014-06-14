defmodule Exrabbit.Mixfile do
  use Mix.Project

  def project do
    [ app: :exrabbit,
      version: "0.0.5",
      deps: deps ]
  end

  def application do
    [
      mod: { Exrabbit, [] },
      applications: [:amqp_client, :jazz, :sweetconfig, :rabbit_common]
    ]
  end

  defp deps do
    [
      { :amqp_client, github: "d0rc/amqp_client" },
      { :jazz, github: "d0rc/jazz", branch: "v0.14.1"},
      { :sweetconfig, github: "d0rc/sweetconfig"}
    ]
  end
end
