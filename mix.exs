defmodule Citus.MixProject do
  use Mix.Project

  def project do
    [
      app: :citus,
      version: "0.1.0",
      elixir: "~> 1.7",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.0"},
      {:phoenix_ecto, "~> 4.0"},
      {:postgrex, ">= 0.0.0"},
      {:poison, "~> 3.1", override: true},
      {:remix, "~> 0.0.1", only: :dev}
    ]
  end
end
