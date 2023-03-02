defmodule LogServer.Storage.S3 do
  @moduledoc false
  alias LogServer.Storage.FileSystemManager
  @bucket if System.get_env("DEV"), do: "pancake_log_server.dev", else: "pancake_log_server.prod"
  @hour 60 * 60
  def upload(storage_path) do
    metadata_file_paths = list_files_in_path(Path.join(storage_path, "metadata_file"))
    body_file_paths = list_files_in_path(Path.join(storage_path, "body_file"))
    metadata_file_paths ++ body_file_paths
    |> Task.async_stream(fn path ->
      [_buffer_area | path_identity] = Path.split(path)
      path
      |> ExAws.S3.Upload.stream_file()
      |> ExAws.S3.upload(@bucket, Path.join(path_identity))
      |> ExAws.request()
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
    |> Enum.map(& Path.join(path, &1))
  end

  def download(storage_path, dest_path) do
    @bucket
    |> ExAws.S3.download_file(storage_path, dest_path)
    |> ExAws.request()
    |> case do
      {:ok, _} ->
        FileSystemManager.set_ttl(dest_path, 3 * @hour)
        {:ok, dest_path}
      error -> error
    end
  end
end
