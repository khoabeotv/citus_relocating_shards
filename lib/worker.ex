defmodule Citus.Worker do
  use Agent

  alias Citus.{Repo, SubRepo}

  @db_name System.get_env("POSTGRES_DB")
  @user System.get_env("POSTGRES_USER")
  @sub_conf Application.get_env :citus, SubRepo

  def test_lock do
    Repo.transaction(fn ->
      raw_query("SELECT lock_shard_metadata(3, ARRAY[102652])")
      Process.sleep(20000)
    end)
  end

  def setup_subrepo() do
    ["pg_dist_partition", "pg_dist_shard", "pg_dist_placement", "pg_dist_node", "pg_dist_colocation"]
    |> Enum.each(fn table_name ->
      create_pub = "CREATE PUBLICATION pub_#{table_name} FOR TABLE #{table_name}"
      Ecto.Adapters.SQL.query!(Repo, create_pub, []) |> IO.inspect(label: "CREATE_PUB")

      create_sub =
        "CREATE SUBSCRIPTION sub_#{table_name} connection 'host=#{@sub_conf[:hostname]} port=5432 user=#{
          @user
        } dbname=#{@db_name}' PUBLICATION pub_#{table_name}"
      Ecto.Adapters.SQL.query!(Repo, create_pub, []) |> IO.inspect(label: "CREATE_SUB")
    end)
  end

  def start_link(_),
    do:
      Agent.start_link(
        fn ->
          %{
            total: 0,
            current: 0,
            shard_group: [],
            success_shards: [],
            catchup_shards: [],
            error: nil,
            rollback: false,
            relocating: false,
            min_diff: 0,
            success_groups: [],
            group_count_down: 0
          }
        end,
        name: __MODULE__
      )

  def set_state(state), do: Agent.update(__MODULE__, &Map.merge(&1, state))
  def get_state, do: Agent.get(__MODULE__, & &1)

  def shard_group_query(source_node, dest_node) when is_nil(source_node) or is_nil(dest_node) do
    """
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
    """
  end

  def shard_group_query(source_node, dest_node) do
    """
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
        SELECT src.nodename source_node, src.shard_group, src.logicalrel_group, abs((size/node_count)::bigint-(node_size+group_size)) optimal_size, ind_size.nodename dest_node, group_size
        FROM (
          SELECT cs.nodename, group_size, shard_group, logicalrel_group from colocated_shard_sizes cs WHERE cs.nodename='#{
      source_node
    }'
        )src,
        ind_size, total
        WHERE ind_size.nodename='#{dest_node}' ORDER BY optimal_size LIMIT 1
      )

      --- Pull out our information so we can then move our shard
      SELECT source_node, dest_node, logicalrel_group, shard_group from optimal_move;
    """
  end

  def get_shard_group(source_node, dest_node) do
    try do
      shard_group_query(source_node, dest_node)
      |> raw_query([], timeout: 150_000)
      |> Map.get(:rows)
      |> List.first()
    rescue
      _ -> get_shard_group(source_node, dest_node)
    end
  end

  def relocating_shards(source_node \\ nil, dest_node \\ nil, count \\ 1) do
    state = get_state()
    unless state.relocating do
      set_state(%{
        success_shards: [],
        catchup_shards: [],
        error: nil,
        rollback: false,
        relocating: true,
        min_diff: 0,
        success_groups: (if state.group_count_down == 0, do: [], else: state.success_groups),
        group_count_down: count,
        current: 0,
        total: 0,
        shard_group: []
      })

      shard_group = get_shard_group(source_node, dest_node)
      total = length(List.last(shard_group))

      set_state(%{
        total: total,
        shard_group: shard_group
      })

      [source_node, dest_node, logicalrel_group, shard_group] = shard_group

      move_shard_group(source_node, dest_node, logicalrel_group, shard_group)
      |> case do
        :ok ->
          check_wal_status(source_node, dest_node, logicalrel_group, shard_group)

        {:error, error} ->
          set_state(%{error: error})
          rollback_shard_group()

          puts_notice(
            text: "MOVE_SHARD_GROUP_ERROR: #{source_node} - #{dest_node} \n #{inspect(error)}",
            failed: true
          )
      end
    end
  end

  def rollback_shard_group() do
    set_state(%{rollback: true})
    [source_node, dest_node, logicalrel_group, shard_group] = get_state().shard_group
    rollback_shard_group(source_node, dest_node, logicalrel_group, shard_group)
  end

  def rollback_shard_group(_, _, [], []) do
    set_state(%{relocating: false})
  end

  def rollback_shard_group(source_node, dest_node, [logicalrelid | logicalrel_tail], [
        shardid | shard_tail
      ]) do
    table_name = "#{logicalrelid}_#{shardid}"

    drop_sub(dest_node, "sub_#{table_name}")
    drop_pub(source_node, "pub_#{table_name}")
    drop_table(table_name, dest_node)
    update_metadata(source_node, shardid)

    rollback_shard_group(source_node, dest_node, logicalrel_tail, shard_tail)
  end

  def drop_table(table_name, node) do
    case run_command_on_worker(node, "DROP TABLE IF EXISTS #{table_name}") do
      {:ok, _} -> :ok
      _        -> drop_table(table_name, node)
    end
  rescue
    _ -> drop_table(table_name, node)
  end

  def drop_source_table(_, [], []), do: IO.puts("drop_source_table => success")

  def drop_source_table(node, [logicalrelid | logicalrel_tail], [shardid | shard_tail]) do
    drop_table("#{logicalrelid}_#{shardid}", node)
    drop_source_table(node, logicalrel_tail, shard_tail)
  end

  def drop_source_table(continue \\ true) do
    state = get_state()

    if state.current == state.total && state.relocating do
      [source_node, dest_node, logicalrel_group, shard_group] = state.shard_group
      drop_source_table(source_node, logicalrel_group, shard_group)

      group_count_down = state.group_count_down - 1

      set_state(%{
        relocating: false,
        success_groups: state.success_groups ++ [state.shard_group],
        group_count_down: group_count_down
      })

      if continue && group_count_down != 0,
        do: relocating_shards(source_node, dest_node, group_count_down)
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

  def check_wal_status(source_node, dest_node, [logicalrelid | logicalrel_tail], [
        shardid | shard_tail
      ]) do
    spawn(fn -> check_wal_status(source_node, dest_node, logicalrelid, shardid) end)
    check_wal_status(source_node, dest_node, logicalrel_tail, shard_tail)
  end

  def check_wal_status(source_node, dest_node, logicalrelid, shardid) do
    unless get_state().rollback do
      Process.sleep(10000)
      table_name = "#{logicalrelid}_#{shardid}"

      if length(get_state().catchup_shards) == get_state().total do
        Repo.transaction(
          fn ->
            raw_query("SELECT lock_shard_metadata(3, ARRAY[#{shardid}])")
            Process.sleep(10000)
            update_metadata(dest_node, shardid)
          end,
          timeout: :infinity
        )
        |> case do
          {:ok, _} ->
            drop_sub(dest_node, "sub_#{table_name}")
            drop_pub(source_node, "pub_#{table_name}")

            state = get_state()
            success_shards = (state.success_shards ++ [table_name]) |> Enum.uniq()
            current = length(success_shards)
            set_state(%{current: current, success_shards: success_shards})

            IO.inspect("#{current}/#{state.total}", label: "PROGRESS")
            # drop_source_table()

          _ ->
            check_wal_status(source_node, dest_node, logicalrelid, shardid)
        end
      else
        is_active = wal_is_active?(source_node, table_name)
        is_catchup = wal_is_catchup?(source_node, table_name)

        with {:ok, source_count} <- table_count(source_node, table_name),
             {:ok, dest_count} <- table_count(dest_node, table_name) do
          count_match = source_count - dest_count <= get_state().min_diff

          info = "#{table_name}: Streaming => #{is_active} | Catchup => #{is_catchup} | #{dest_count}/#{source_count}"
          info =
            unless is_catchup do
              source_size = table_size(source_node, table_name)
              dest_size = table_size(dest_node, table_name)
              "#{info} | Progress => #{dest_size}/#{source_size}"
            else
              info
            end

          IO.puts(info)

          cond do
            is_active && is_catchup && count_match &&
            table_name not in get_state().catchup_shards ->
              catchup_shards = get_state().catchup_shards ++ [table_name] |> Enum.uniq()
              set_state(%{catchup_shards: catchup_shards})
              check_wal_status(source_node, dest_node, logicalrelid, shardid)

            is_active && is_catchup &&
            table_name not in get_state().catchup_shards ->
              analyze_table(dest_node, table_name)
              check_wal_status(source_node, dest_node, logicalrelid, shardid)

            true ->
              check_wal_status(source_node, dest_node, logicalrelid, shardid)
          end
        else
          _ -> check_wal_status(source_node, dest_node, logicalrelid, shardid)
        end
      end
    end
  end

  def table_size(node, table_name) do
    {_, res} = run_command_on_worker(node, "SELECT pg_size_pretty(pg_total_relation_size('#{table_name}'));")
    res
  end

  def analyze_table(node, table_name) do
    raw_query(
      "SELECT * FROM master_run_on_worker(ARRAY['#{node}'], ARRAY[5432], ARRAY[$cmd$ANALYZE #{table_name}$cmd$], true)",
      [],
      timeout: :infinity
    )
    |> IO.inspect
  end

  def wal_is_catchup?(node, table_name, count \\ 0) do
    case run_command_on_worker(
           node,
           "SELECT pid FROM pg_stat_replication WHERE application_name ilike 'sub_#{table_name}%' AND state = 'startup'"
         ) do
      {:ok, ""} ->
        if count == 5 do
          true
        else
          :timer.sleep(3000)
          wal_is_catchup?(node, table_name, count + 1)
        end
      {:ok, _} -> false
      _ -> false
    end
  end

  def wal_is_active?(node, table_name) do
    case run_command_on_worker(
           node,
           "SELECT pid FROM pg_stat_replication WHERE application_name = 'sub_#{table_name}' AND state = 'streaming'"
         ) do
      {:ok, ""} -> false
      {:ok, _} -> true
      _ -> false
    end
  end

  def table_count(node, table_name) do
    try do
      case run_command_on_worker(
             node,
             "SELECT reltuples::bigint AS estimate FROM pg_class where relname='#{table_name}'"
           ) do
        {:ok, data} ->
          count = String.to_integer(data)
          {:ok, count}
          # cond do
          #   count < 1_000_000 && !String.contains?(table_name, "messages") -> table_rel_count(node, table_name)
          #   true -> {:ok, count}
          # end

        _ ->
          :error
      end
    rescue
      _ -> :error
    end
  end

  def table_rel_count(node, table_name) do
    case run_command_on_worker(node, "SELECT count(1) FROM #{table_name}") do
      {:ok, data} -> {:ok, String.to_integer(data)}
      _ -> :error
    end
  end

  def move_shard_group(_, _, [], []), do: :ok

  def move_shard_group(source_node, dest_node, [logicalrelid | logicalrel_tail], [
        shardid | shard_tail
      ]) do
    table_name = "#{logicalrelid}_#{shardid}"

    with {:ok, _} <-
           clone_table(source_node, dest_node, logicalrelid, shardid)
           |> IO.inspect(label: "CREATE TABLE #{table_name}"),
         {:ok, _} <-
           create_pub(source_node, table_name) |> IO.inspect(label: "CREATE PUB #{table_name}"),
         # {:ok, _} <-
         #   create_slot(source_node, table_name) |> IO.inspect(label: "CREATE SLOT #{table_name}"),
         {:ok, _} <-
           create_sub(dest_node, source_node, table_name)
           |> IO.inspect(label: "CREATE SUB #{table_name}") do
      move_shard_group(source_node, dest_node, logicalrel_tail, shard_tail)
    else
      {:error, error} -> {:error, error}
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

      res ->
        res
    end
  end

  def create_slot(node, table_name) do
    create_slot = "SELECT * FROM pg_create_logical_replication_slot('sub_#{table_name}', 'pgoutput');"

    case run_command_on_worker(node, create_slot) do
      {:error, error} ->
        if String.contains?(error, "already exists") do
          run_command_on_worker(node, "SELECT pg_drop_replication_slot('sub_#{table_name}');")
          create_slot(node, table_name)
        else
          {:error, error}
        end

      res ->
        res
    end
  end

  def create_sub(node, source_node, table_name) do
    create_sub =
      "CREATE SUBSCRIPTION sub_#{table_name} connection 'host=#{source_node} port=5432 user=#{
        @user
      } dbname=#{@db_name}' PUBLICATION pub_#{table_name}" # WITH (create_slot = false)

    case run_command_on_worker(node, create_sub) do
      {:error, error} ->
        if String.contains?(error, "already exists") do
          run_command_on_worker(node, "DROP SUBSCRIPTION sub_#{table_name}")
          create_sub(node, source_node, table_name)
        else
          {:error, error}
        end

      res ->
        res
    end
  end

  def drop_pub(node, pub_name) do
    case run_command_on_worker(node, "DROP PUBLICATION #{pub_name}") do
      {:error, error} ->
        if String.contains?(error, "does not exist") do
          :ok
        else
          drop_pub(node, pub_name)
        end

      {:ok, _} -> :ok
    end
  rescue
    _ ->
      drop_pub(node, pub_name)
  end

  def drop_sub(node, sub_name) do
    case run_command_on_worker(node, "DROP SUBSCRIPTION #{sub_name}") do
      {:error, error} ->
        if String.contains?(error, "does not exist") do
          :ok
        else
          drop_sub(node, sub_name)
        end

      {:ok, _} -> :ok
    end
  rescue
    _ ->
      drop_sub(node, sub_name)
  end

  def clone_table(source_node, node, source_table_name, shardid) do
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
      |> Enum.map(fn [name | _] -> name end)
      |> Enum.join(", ")

    primary_keys = "CONSTRAINT #{source_table_name}_pkey_#{shardid} PRIMARY KEY (#{primary_keys})"

    columns =
      schema
      |> Enum.map(fn [name, type, nullable, default, _] ->
        column = "\"#{name}\" #{type}"
        column = if nullable == "NO", do: "#{column} NOT NULL", else: column

        if default && !String.contains?(default, "nextval"),
          do: "#{column} DEFAULT #{default}",
          else: column
      end)
      |> Enum.join(", ")

    command = ["CREATE TABLE IF NOT EXISTS public.#{table_name}(#{columns}, #{primary_keys})"]

    indexes =
      run_command_on_worker(source_node, "SELECT array_agg(indexdef) FROM pg_indexes WHERE tablename = '#{table_name}'")
      |> elem(1)
      |> String.replace(~r/.$/, "]")
      |> String.replace(~r/^./, "[")
      |> Poison.decode!
      |> Enum.filter(fn indexdef -> !String.contains?(indexdef, "pkey") end)
      |> Enum.map(fn indexdef ->
        indexdef
        |> String.replace("CREATE INDEX", "CREATE INDEX IF NOT EXISTS")
        |> String.replace("CREATE UNIQUE INDEX", "CREATE UNIQUE INDEX IF NOT EXISTS")
      end)

    command = command ++ indexes

    triggers =
      raw_query("""
        WITH triggers AS (
          SELECT
            trigger_name,
            array_agg(event_manipulation::text) event_manipulation,
            array_agg(action_condition::text) action_condition,
            array_agg(action_statement::text) action_statement,
            array_agg(action_orientation::text) action_orientation,
            array_agg(action_timing::text) action_timing
          FROM
            information_schema.triggers
          WHERE
            event_object_table = '#{source_table_name}'
          GROUP BY
            "trigger_name"
        )
        SELECT
          "trigger_name",
          event_manipulation,
          action_condition [1],
          action_statement [1],
          action_orientation [1],
          action_timing [1]
        FROM
          triggers
      """)
      |> Map.get(:rows)
      |> Enum.map(fn [
                       trigger_name,
                       event_manipulation,
                       action_condition,
                       action_statement,
                       action_orientation,
                       action_timing
                     ] ->
        """
          CREATE TRIGGER #{trigger_name}
          #{action_timing} #{Enum.join(event_manipulation, " OR ")}
          ON public.#{table_name}
          FOR EACH #{action_orientation}
          #{if action_condition, do: "WHEN #{action_condition}", else: nil}
          #{action_statement};
        """
      end)

    command = command ++ triggers

    ret = run_command_on_worker(node, command)
    Process.sleep(5000)
    ret
  end

  def run_command_on_worker(node, command, parallel \\ true)

  def run_command_on_worker(node, command, parallel) when is_list(command),
    do: run_command_on_worker(node, command, parallel, [])

  def run_command_on_worker(node, command, parallel) do
    res =
      raw_query(
        "SELECT * FROM master_run_on_worker(ARRAY['#{node}'], ARRAY[5432], ARRAY[$cmd$#{command}$cmd$], #{
          parallel
        })"
      )
      |> Map.get(:rows)
      |> List.first()

    case Enum.at(res, 2) do
      true -> {:ok, List.last(res)}
      _ -> {:error, List.last(res)}
    end
  end

  def run_command_on_worker(_, [], _, res), do: {:ok, res}

  def run_command_on_worker(node, [command | tail], parallel, res) do
    case run_command_on_worker(node, command, parallel) do
      {:ok, data} ->
        res = res ++ [data]
        run_command_on_worker(node, tail, parallel, res)

      error ->
        error
    end
  end

  def shards_size() do
    raw_query(
      "SELECT * FROM run_command_on_workers($$SELECT pg_size_pretty(pg_database_size('#{@db_name}'));$$)"
    )
    |> Map.get(:rows)
  end

  def get_metadata() do
    raw_query(
      "SELECT * FROM pg_dist_placement WHERE shardid IN (#{
        Enum.join(List.last(get_state().shard_group), ",")
      })"
    )
  end

  def raw_query(query, params \\ [], opts \\ []),
    do: Ecto.Adapters.SQL.query!(Repo, query, params, opts)

  def puts_notice(options \\ []) do
    defaults = [text: "", failed: false]
    options = Keyword.merge(defaults, options) |> Enum.into(%{})
    %{text: text, failed: failed} = options

    if failed do
      IO.puts(IO.ANSI.red() <> text <> IO.ANSI.reset())
    else
      IO.puts(IO.ANSI.green() <> text <> IO.ANSI.reset())
    end
  end
end
