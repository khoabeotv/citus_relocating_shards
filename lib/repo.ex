defmodule Citus.Repo do
  use Ecto.Repo, otp_app: :citus, adapter: Ecto.Adapters.Postgres
end

defmodule Citus.SubRepo do
  use Ecto.SubRepo, otp_app: :citus, adapter: Ecto.Adapters.Postgres
end