defmodule LogServer.Storage.S3 do
  @moduledoc false
  alias LogServer.Storage.{FileSystemManager, MetadataCache}
  alias LogServer.Tools
  @bucket if System.get_env("DEV"), do: "pancake_log_server.dev", else: "pancake_log_server.prod"
  @hour 60 * 60
  def upload(storage_path) do
    metadata_file_paths = list_files_in_path(Tools.join_storage_path([storage_path, "metadata_file"]))
    body_file_paths = list_files_in_path(Tools.join_storage_path([storage_path, "body_file"]))
    metadata_file_paths ++ body_file_paths
    |> Task.async_stream(fn path ->
      [_buffer_area | path_identity] = Tools.split_storage_path(path)
      path_identity =
        path_identity
        |> Tools.join_storage_path
        |> format_path_by_env(System.get_env("DEV"))

      path
      |> ExAws.S3.Upload.stream_file()
      |> ExAws.S3.upload(@bucket, path_identity)
      |> ExAws.request()

      MetadataCache.invalidate(path_identity)
    end)
    |> Stream.run()
  end

  # làm 1 lần thôi
  def create_bucket() do
    @bucket
    |> ExAws.S3.put_bucket("")
    |> ExAws.request()
  end

  defp list_files_in_path(path) do
    path
    |> File.ls!()
    |> Enum.map(& Tools.join_storage_path([path, &1]))
  end

  def download(storage_path, :file, dest_path: dest_path) do
    storage_path = format_path_by_env(storage_path, System.get_env("DEV"))
    |> IO.inspect(label: "storage_path123")
    @bucket
    |> ExAws.S3.download_file(storage_path, dest_path)
    |> ExAws.request()
    |> IO.inspect(label: "result download")
    |> case do
      {:ok, _} ->
        FileSystemManager.set_ttl(dest_path, 3 * @hour)
        {:ok, dest_path}
      error -> error
    end
  end

  def download(storage_path, :memory, bytes_range_fetches: {begin_byte, end_byte}) do
    storage_path = format_path_by_env(storage_path, System.get_env("DEV"))
    %{body: body} =
      @bucket
      |> ExAws.S3.get_object(
        storage_path,
        range: "bytes=#{begin_byte}-#{end_byte}"
      )
      |> ExAws.request!()

    {:ok, body}
  end

  defp format_path_by_env(path, env) do
    if env == "dev" do
      path
    else
      [first, second | _rest] = parts = Path.split(path)
      if (first == "." and second == "data") or String.contains?(first, "./data"),
        do: parts |> Enum.drop(2) |> Path.join(),
        else: path
        # 1678173600_1678174200
    end
  end
end
