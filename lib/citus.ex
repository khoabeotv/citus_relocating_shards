defmodule Citus do
  use DynamicSupervisor

  def start_link(), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  def init(_), do: DynamicSupervisor.init(strategy: :one_for_one)

  def connect() do
    if DynamicSupervisor.count_children(__MODULE__)[:active] == 0 do
      DynamicSupervisor.start_child(__MODULE__, {Citus.Repo, []})
      DynamicSupervisor.start_child(__MODULE__, {Citus.Worker, []})
    end
  end
end
