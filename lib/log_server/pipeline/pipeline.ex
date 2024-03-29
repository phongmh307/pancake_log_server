defmodule LogServer.Pipeline do
  @moduledoc """
  Documentation for Pipeline

  """
  alias LogServer.Pipeline.{Extractor, Extractor.Client, Transformer, Loader}
  alias LogServer.Tools
  @shard_interval 600
  @storage_folder (
    if System.get_env("DEV"),
      do: "buffer_area",
      else: "./data/buffer_area"
  )

  def open() do
    clients = Client.get_all()
    shard_time = generate_shard_time()
    projects = Enum.uniq_by(clients, & &1.project)
    Enum.each(projects, & setup_buffer_area(&1.project, shard_time))

    clients
    |> Enum.group_by(& &1.project)
    |> Enum.each(fn {project, clients_in_project} ->
      clients_in_project
      |> Extractor.extract(shard_time)
      |> Transformer.transform()
      |> Loader.upload()
    end)

    # Enum.each(projects, & clear_buffer_area(&1.project, shard_time))
  end

  # Thống nhất file name này với Log Client
  # ở Log Client là phần đầu hàm LogCake.current_path
  defp generate_shard_time do
    shard_id =
      if System.get_env("DEV"),
        do: to_shard_time(System.os_time(:second)),
        else: to_shard_time(System.os_time(:second) - @shard_interval)

    "#{Integer.to_string(shard_id)}_#{to_next_shard_time(shard_id)}"
  end

  defp clear_buffer_area(project, shard_time) do
    Tools.join_storage_path([@storage_folder, project, shard_time])
    |> File.rm_rf!
  end

  defp setup_buffer_area(project, shard_time) do
    clear_buffer_area(project, shard_time)
    File.mkdir_p!(Tools.join_storage_path([@storage_folder, project, shard_time, "raw_file"]) |> IO.inspect(label: "oi gioi oi"))
    File.mkdir_p!(Tools.join_storage_path([@storage_folder, project, shard_time, "metadata_file"]) |> IO.inspect(label: "oi gioi oi"))
    File.mkdir_p!(Tools.join_storage_path([@storage_folder, project, shard_time, "body_file"]) |> IO.inspect(label: "oi gioi oi"))
  end

  def to_shard_time(time) do
    div(time, @shard_interval) * @shard_interval
  end

  def to_next_shard_time(time) do
    to_shard_time(time + @shard_interval)
  end
end
