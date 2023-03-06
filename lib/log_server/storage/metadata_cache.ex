defmodule LogServer.Storage.MetadataCache do
  alias LogServer.Tools
  use GenServer
  @metadata_cache :metadata_cache
  @hour 60 * 60

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def get(key) do
    :ets.lookup(@metadata_cache, key)
  end

  def set(key, value) do
    expired_at =
      Tools.naive_utc_now
      |> NaiveDateTime.add(3 * @hour)

    :ets.insert(@metadata_cache, {key, value, expired_at})
  end

  def invalidate(key),
    do: :ets.delete(@metadata_cache, key)

  def purging_expired_cache() do
    GenServer.cast(__MODULE__, :purging_expired_cache)
  end

  @impl true
  def init([]) do
    if :ets.whereis(@metadata_cache) == :undefined do
      pid =
        spawn fn ->
          :ets.new(@metadata_cache, [:named_table, :public])
          Process.sleep(:infinity)
        end

      :persistent_term.put(:pid_metadata_cache, pid)
    end
    :timer.apply_interval(@min, __MODULE__, :purging_expired_cache, [])
    {:ok, nil}
  end

  def handle_cast(:purging_expired_cache, state) do
    now = Tools.naive_utc_now
    :ets.fun2ms(fn({key, _value, expired_at}) when now > expired_at ->
      true
    end)

    [
      {{:"$1", :"$2", :"$3"},
       [{:>, {:const, now}, :"$3"}], [true]}
    ]
  end
end
