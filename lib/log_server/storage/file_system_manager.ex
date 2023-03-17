defmodule LogServer.Storage.FileSystemManager do
  @moduledoc false
  use GenServer
  alias LogServer.Tools

  @file_system_ttl_table :file_system_ttl
  @file_system_ttl (
    if System.get_env("DEV"),
      do: "file_system_ttl",
      else: "./data/file_system_ttl"
  )
  @min 60_000

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def set_ttl(dest_path, ttl) do
    expired_at =
      NaiveDateTime.utc_now()
      |> NaiveDateTime.add(ttl)
      |> Tools.ecto_datetime_to_unix

    :dets.insert(@file_system_ttl_table, {dest_path, expired_at})
  end

  def purging_expired_cache() do
    GenServer.cast(__MODULE__, :purging_expired_cache)
  end

  @impl true
  def init([]) do
    :dets.open_file(
      @file_system_ttl_table,
      file: String.to_charlist(@file_system_ttl)
    )
    :timer.apply_interval(@min, __MODULE__, :purging_expired_cache, [])
    {:ok, nil}
  end

  @impl true
  def handle_cast(:purging_expired_cache, state) do
    now = Tools.naive_utc_now() |> Tools.ecto_datetime_to_unix
    # :ets.fun2ms(fn {path, ttl} when ttl <= now ->
    #   path
    # end)
    match_spec =
      [
        {
          {:"$1", :"$2"},
          [{:"=<", :"$2", {:const, now}}],
          [:"$1"]
        }
      ]

    match_spec_delete =
      [
        {
          {:"$1", :"$2"},
          [{:"=<", :"$2", {:const, now}}],
          [true]
        }
      ]

    @file_system_ttl_table
    |> :dets.select(match_spec)
    |> Enum.map(fn path ->
      File.rm_rf!(path)
      path
    end)
    |> cleaning_empty_folder_after_delete_files()

    :dets.select_delete(@file_system_ttl_table, match_spec_delete)
    {:noreply, state}
  end

  defp cleaning_empty_folder_after_delete_files([]), do: :ok

  defp cleaning_empty_folder_after_delete_files(
    [path_deleted | _] = paths_deleted
  ) do
    # khi đường dẫn đã bị xoá split ra chỉ còn 2 phần tử thì đã delete đến
    # thư mục ngay dưới thư mục root("cache") nên cần phải dừng lại chứ ko được
    # xoá thư mục root
    if length(Tools.split_storage_path(path_deleted) |> IO.inspect(label: "omg 123")) == 2 do
      :ok
    else
      paths_deleted
      |> Enum.reduce([], fn path_deleted, acc ->
        parent_folder_path =
          path_deleted
          |> Tools.split_storage_path()
          |> List.delete_at(-1)
          |> Tools.join_storage_path()

        if Enum.any?(acc, & &1 == parent_folder_path),
          do: acc,
          else: acc ++ [parent_folder_path]

      end)
      |> Enum.reduce([], fn parent_folder_path, acc ->
        if File.ls!(parent_folder_path) == [] do
          File.rm_rf!(parent_folder_path)
          acc ++ [parent_folder_path]
        else
          acc
        end
      end)
      |> cleaning_empty_folder_after_delete_files()
    end
  end
end
