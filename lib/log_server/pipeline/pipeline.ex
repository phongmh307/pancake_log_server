defmodule LogServer.Pipeline do
  @moduledoc false
  alias LogServer.Pipeline.{Extractor, Extractor.Client, Transformer, Loader}
  @shard_interval 600
  @storage_folder (
    if System.get_env("DEV"),
      do: "buffer_area",
      else: "../data/buffer_area"
  )

  def open() do
    clients = Client.get_all()
    shard_time = generate_shard_time()
    projects = Enum.uniq_by(clients, & &1.project)
    Enum.each(projects, & setup_buffer_area(&1.project, shard_time))

    clients
    |> Extractor.extract(shard_time)
    |> Transformer.transform()
    |> Loader.upload()

    Enum.each(projects, & clear_buffer_area(&1.project, shard_time))
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
    File.rm_rf!(Path.join([@storage_folder, project, shard_time]))
  end

  defp setup_buffer_area(project, shard_time) do
    clear_buffer_area(project, shard_time)
    File.mkdir_p!(Path.join([@storage_folder, project, shard_time, "raw_file"]))
    File.mkdir_p!(Path.join([@storage_folder, project, shard_time, "metadata_file"]))
    File.mkdir_p!(Path.join([@storage_folder, project, shard_time, "body_file"]))
  end

  def to_shard_time(time) do
    div(time, @shard_interval) * @shard_interval
  end

  def to_next_shard_time(time) do
    to_shard_time(time + @shard_interval)
  end
end
