defmodule Citus do
  use Agent

  alias Citus.Repo

  @db_name Application.get_env(:citus, :db_name)
  @repo Application.get_env(:citus, :repo)

  def start_link(),
    do: Agent.start_link(fn -> %{total: 0, current: 0, success_shards: [], errors: [], rollback: false, shard_group: nil, relocating: false} end, name: __MODULE__)

  def set_state(state), do: Agent.update(__MODULE__, &Map.merge(&1, state))
  def get_state, do: Agent.get(__MODULE__, & &1)

  def relocating_shards() do
    unless get_state().relocating do
      shard_group =
        raw_query("""
          --- Calculate total size of the cluster, later we could adapt this if the individual was a certain percentage larger than the average would be
          WITH total AS (
          SELECT sum(result::bigint) as size, count(nodename) as node_count FROM (
            SELECT nodename, nodeport, result
            FROM run_command_on_workers($cmd$SELECT pg_database_size('#{@db_name}');$cmd$)
          ) a ),

          --- Get the size of each node
          ind_size AS (
            SELECT nodename, result::bigint as node_size
            FROM run_command_on_workers($cmd$SELECT pg_database_size('#{@db_name}')::bigint;$cmd$)
          ),

          --- Save our most crowded node
          most_crowded_node AS (
            SELECT nodename
            FROM ind_size, total
            WHERE ind_size.node_size::bigint > ((cast(total.size as bigint) / cast(node_count as bigint))) ORDER BY ind_size.node_size::bigint DESC LIMIT 1
          ),

          --- Save our least crowded node
          least_crowded_node AS (
            SELECT nodename
            FROM ind_size, total
            WHERE ind_size.node_size::bigint < ((cast(total.size as bigint) / cast(node_count as bigint))) ORDER BY ind_size.node_size::bigint LIMIT 1
          ),

          --- Calculate the shard sizes, which includes data related by colocation groups
          shard_sizes AS(
            SELECT shardid, result::bigint size FROM
            (SELECT (run_command_on_shards(logicalrelid::text,$cmd$SELECT pg_total_relation_size('%s')$cmd$)).*
            FROM pg_dist_partition pp where pp.partmethod='h')a
          ),
          colocated_shard_sizes AS(
            SELECT colocationid, nodename, shard_group, logicalrel_group, group_size
            FROM
            (
              SELECT colocationid, nodename, array_agg(ps.shardid) shard_group, array_agg(pp.logicalrelid::text) logicalrel_group, sum(size) group_size
              from shard_sizes ss, pg_dist_shard ps, pg_dist_shard_placement psp, pg_dist_partition pp
              WHERE (ss.shardid=ps.shardid AND pp.logicalrelid=ps.logicalrelid AND psp.shardid=ps.shardid AND pp.partmethod='h')
              GROUP BY shardmaxvalue, shardminvalue, nodename, colocationid
            )a
          ),

          --- Get a shard with appropriate size to balance the cluster
          optimal_move AS (
            SELECT src.nodename source_node, src.shard_group, src.logicalrel_group, abs((size/node_count)::bigint-(node_size+group_size)) optimal_size, least_crowded_node.nodename dest_node, group_size
            FROM (
              SELECT mc.nodename, group_size, shard_group, logicalrel_group from colocated_shard_sizes cs, most_crowded_node mc WHERE cs.nodename=mc.nodename
            )src,
            ind_size, least_crowded_node, total
            WHERE ind_size.nodename=least_crowded_node.nodename ORDER BY optimal_size LIMIT 1
          )

          --- Pull out our information so we can then move our shard
          SELECT source_node, dest_node, logicalrel_group, shard_group from optimal_move;
        """, [], [timeout: 50000])
        |> Map.get(:rows)
        |> List.first

      total = length(List.last(shard_group))
      set_state(%{total: total, current: 0, shard_group: shard_group, success_shards: [], errors: [], rollback: false, relocating: true})
      [source_node, dest_node, logicalrel_group, shard_group] = shard_group
      @repo.transaction(fn ->
        move_shard_group(source_node, dest_node, logicalrel_group, shard_group)
      end)
      |> case do
        {:ok, _} ->
          check_wal_status(source_node, dest_node, logicalrel_group, shard_group)
        {:error, error} ->
          state = get_state()
          errors = state.errors ++ [error]
          set_state(%{errors: errors})
          rollback_shard_group()
          puts_notice(text: "MOVE_SHARD_GROUP_ERROR: #{source_node} - #{dest_node} \n #{inspect(error)}", failed: true)
      end
    end
  end

  def rollback_shard_group() do
    set_state(%{rollback: true})
    [source_node, dest_node, logicalrel_group, shard_group] = get_state().shard_group
    rollback_shard_group(source_node, dest_node, logicalrel_group, shard_group)
  end

  def rollback_shard_group(_, _, [], []), do: :ok

  def rollback_shard_group(source_node, dest_node, [logicalrelid|logicalrel_tail], [shardid|shard_tail]) do
    table_name = "#{logicalrelid}_#{shardid}"

    drop_pub(source_node, "pub_#{table_name}")
    drop_sub(dest_node, "sub_#{table_name}")
    run_command_on_worker(dest_node, "DROP TABLE IF EXISTS #{table_name}")
    update_metadata(source_node, shardid)

    rollback_shard_group(source_node, dest_node, logicalrel_tail, shard_tail)
  end

  def drop_source_table(_, [], []), do: :ok

  def drop_source_table(node, [logicalrelid|logicalrel_tail], [shardid|shard_tail]) do
    run_command_on_worker(node, "DROP TABLE IF EXISTS #{logicalrelid}_#{shardid}")
    drop_source_table(node, logicalrel_tail, shard_tail)
  end

  def drop_source_table() do
    state = get_state()
    if state.current == state.total do
      [source_node, _, logicalrel_group, shard_group] = state.shard_group
      drop_source_table(source_node, logicalrel_group, shard_group)
      set_state(%{relocating: false})
    end
  end

  def update_metadata(node, shardid) do
    raw_query("""
      UPDATE pg_dist_placement p
      SET groupid = t.groupid
      FROM (
        SELECT groupid FROM pg_dist_node
        WHERE nodename = '#{node}'
      ) t
      WHERE p.shardid = '#{shardid}';
    """)
  end

  def check_wal_status(_, _, [], []), do: :ok

  def check_wal_status(source_node, dest_node, [logicalrelid|logicalrel_tail], [shardid|shard_tail]) do
    spawn(fn -> check_wal_status(source_node, dest_node, logicalrelid, shardid) end)
    check_wal_status(source_node, dest_node, logicalrel_tail, shard_tail)
  end

  def check_wal_status(source_node, dest_node, logicalrelid, shardid) do
    unless get_state().rollback do
      if @repo.in_transaction?, do: Process.sleep(200), else: Process.sleep(10000)
      table_name = "#{logicalrelid}_#{shardid}"
      source_count = table_count(source_node, table_name)
      dest_count = table_count(dest_node, table_name)

      IO.inspect "#{table_name}: #{dest_count}/#{source_count}"

      cond do
        source_count - dest_count < 100 && wal_is_active?(source_node, table_name) ->
          update = fn ->
            Process.sleep(5000)
            update_metadata(dest_node, shardid)
          end
          if @repo.in_transaction?, do: update.(), else: @repo.transaction(fn ->
            raw_query("LOCK TABLE #{logicalrelid} IN ROW EXCLUSIVE MODE")
            update.()
          end)
          drop_pub(source_node, "pub_#{table_name}")
          drop_sub(dest_node, "sub_#{table_name}")

          state = get_state()
          current = state.current + 1
          success_shards = state.success_shards ++ [table_name]
          set_state(%{current: current, success_shards: success_shards})

          drop_source_table()
          IO.inspect("#{current}/#{state.total}", label: "PROGRESS")

        source_count - dest_count < 500 && !@repo.in_transaction? && wal_is_active?(source_node, table_name) ->
          try do
            @repo.transaction(fn ->
              raw_query("LOCK TABLE #{logicalrelid} IN ROW EXCLUSIVE MODE")
               check_wal_status(source_node, dest_node, logicalrelid, shardid)
            end)
          rescue
            _ ->
              check_wal_status(source_node, dest_node, logicalrelid, shardid)
          end

        true -> check_wal_status(source_node, dest_node, logicalrelid, shardid)
      end
    end
  end

  def wal_is_active?(node, table_name) do
    case run_command_on_worker(node, "SELECT pid FROM pg_stat_replication WHERE application_name = 'sub_#{table_name}'") do
      {:ok, ""} -> false
      {:ok, _} -> true
      _ -> false
    end
  end

  def table_count(node, table_name) do
    case run_command_on_worker(node, "SELECT reltuples::bigint AS estimate FROM pg_class where relname='#{table_name}'") do
      {:ok, data} ->
        count = String.to_integer(data)
        cond do
          count < 1000000 -> table_rel_count(node, table_name)
          true -> count
        end

      _ -> 0
    end
  end

  def table_rel_count(node, table_name) do
    case run_command_on_worker(node, "SELECT count(1) FROM #{table_name}") do
      {:ok, data} -> String.to_integer(data)
      _           -> 0
    end
  end

  def move_shard_group(_, _, [], []), do: :ok

  def move_shard_group(source_node, dest_node, [logicalrelid|logicalrel_tail], [shardid|shard_tail]) do
    table_name = "#{logicalrelid}_#{shardid}"

    with {:ok, _} <- create_pub(source_node, table_name) |> IO.inspect(label: "CREATE PUB #{table_name}"),
         {:ok, _} <- clone_table(dest_node, logicalrelid, shardid) |> IO.inspect(label: "CREATE TABLE #{table_name}"),
         {:ok, _} <- create_sub(dest_node, source_node, table_name) |> IO.inspect(label: "CREATE SUB #{table_name}")
    do
      move_shard_group(source_node, dest_node, logicalrel_tail, shard_tail)
    else
      {:error, error} -> @repo.rollback(error)
    end
  end

  def create_pub(node, table_name) do
    create_pub = "CREATE PUBLICATION pub_#{table_name} FOR TABLE #{table_name}"
    case run_command_on_worker(node, create_pub) do
      {:error, error} ->
        if String.contains?(error, "already exists") do
          run_command_on_worker(node, "DROP PUBLICATION pub_#{table_name}")
          create_pub(node, table_name)
        else
          {:error, error}
        end

      res -> res
    end
  end

  def create_sub(node, source_node, table_name) do
    create_sub = "CREATE SUBSCRIPTION sub_#{table_name} connection 'host=#{source_node} port=5432 user=adcake dbname=#{@db_name}' PUBLICATION pub_#{table_name}"
    case run_command_on_worker(node, create_sub) do
      {:error, error} ->
        if String.contains?(error, "already exists") do
          run_command_on_worker(node, "DROP SUBSCRIPTION sub_#{table_name}")
          create_sub(node, source_node, table_name)
        else
          {:error, error}
        end

      res -> res
    end
  end

  def drop_pub(node, pub_name), do: run_command_on_worker(node, "DROP PUBLICATION #{pub_name}")

  def drop_sub(node, sub_name), do: run_command_on_worker(node, "DROP SUBSCRIPTION #{sub_name}")

  def clone_table(node, source_table_name, shardid) do
    table_name = "#{source_table_name}_#{shardid}"

    schema =
      raw_query("""
        SELECT c.column_name, c.udt_name, c.is_nullable, c.column_default, tc.constraint_type FROM information_schema.columns c
        LEFT JOIN information_schema.key_column_usage cu ON cu.table_name = c.table_name AND cu.column_name = c.column_name
        LEFT JOIN information_schema.table_constraints tc ON tc.table_name = c.table_name AND cu.constraint_name = tc.constraint_name
        WHERE c.table_name = '#{source_table_name}'
      """)
      |> Map.get(:rows)

    primary_keys =
      schema
      |> Enum.filter(fn row -> List.last(row) end)
      |> Enum.map(fn [name|_] -> name end)
      |> Enum.join(", ")

    primary_keys = "CONSTRAINT #{source_table_name}_pkey_#{shardid} PRIMARY KEY (#{primary_keys})"

    columns =
      schema
      |> Enum.map(fn [name, type, nullable, default, _] ->
        column = "#{name} #{type}"
        column = if nullable == "NO", do: "#{column} NOT NULL", else: column
        if default && !String.contains?(default, "nextval"), do: "#{column} DEFAULT #{default}", else: column
      end)
      |> Enum.join(", ")

    command = ["CREATE TABLE IF NOT EXISTS public.#{table_name}(#{columns}, #{primary_keys})"]

    indexes =
      raw_query("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '#{source_table_name}'")
      |> Map.get(:rows)
      |> Enum.filter(fn [indexname, _] -> !String.contains?(indexname, "pkey") end)
      |> Enum.map(fn [indexname, indexdef] ->
        String.replace(indexdef, indexname, "#{indexname}_#{shardid}")
        |> String.replace("public.#{source_table_name}", "public.#{table_name}")
        |> String.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
        |> String.replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS")
      end)

    command = command ++ indexes

    run_command_on_worker(node, command)
  end

  def run_command_on_worker(node, command, parallel \\ true)

  def run_command_on_worker(node, command, parallel) when is_list(command),
    do: run_command_on_worker(node, command, parallel, [])

  def run_command_on_worker(node, command, parallel) do
    res =
      raw_query("SELECT * FROM master_run_on_worker(ARRAY['#{node}'], ARRAY[5432], ARRAY[$cmd$#{command}$cmd$], #{parallel})")
      |> Map.get(:rows)
      |> List.first

    case Enum.at(res, 2) do
      true -> {:ok, List.last(res)}
      _    -> {:error, List.last(res)}
    end
  end

  def run_command_on_worker(_, [], _, res), do: {:ok, res}

  def run_command_on_worker(node, [command|tail], parallel, res) do
    case run_command_on_worker(node, command, parallel) do
      {:ok, data} ->
        res = res ++ [data]
        run_command_on_worker(node, tail, parallel, res)

      error -> error
    end
  end

  def shards_size() do
    raw_query("SELECT * FROM run_command_on_workers($$SELECT pg_size_pretty(pg_database_size('#{@db_name}'));$$)")
    |> Map.get(:rows)
  end

  def raw_query(query, params \\ [], opts \\ []), do: Ecto.Adapters.SQL.query!(Repo, query, params, opts)

  def puts_notice(options \\ []) do
    defaults = [text: "", failed: false]
    options = Keyword.merge(defaults, options) |> Enum.into(%{})
    %{text: text, failed: failed} = options

    if failed do
      IO.puts ANSI.red <> text <> ANSI.reset
    else
      IO.puts ANSI.green <> text <> ANSI.reset
    end
  end
end
