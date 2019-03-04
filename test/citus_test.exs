defmodule CitusTest do
  use ExUnit.Case
  doctest Citus

  test "greets the world" do
    assert Citus.hello() == :world
  end
end
