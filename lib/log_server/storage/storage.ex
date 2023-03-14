defmodule LogServer.Storage do
  @moduledoc false
  alias LogServer.Tools
  alias LogServer.Storage.S3
  alias LogServer.Storage.MetadataCache
  @cache_folder(
    if System.get_env("DEV"),
      do: "cache",
      else: "../data/cache"
  )

  def upload(storage_path) do
    S3.upload(storage_path)
  end

  #output {:ok, path}
  def download(storage_path, opts \\ []) do
    dest_path = Keyword.get(opts, :dest_path, :file)
    params = Keyword.delete(opts, :dest_path)
    do_download(storage_path, dest_path, params)
  end

  defp do_download(storage_path, :file, _params) do
    [project, time_shard, type_file, key_shard] = Tools.split_storage_path(storage_path)
    dest_path =
      [@cache_folder]
      |> Kernel.++([project, time_shard, type_file, key_shard])
      |> Tools.join_storage_path()

    if File.exists?(dest_path) do
      {:ok, dest_path}
    else
      [@cache_folder]
      |> Kernel.++([project, time_shard, type_file])
      |> Tools.join_storage_path()
      |> File.mkdir_p!()

      S3.download(storage_path, :file, dest_path: dest_path)
    end
  end

  defp do_download(storage_path, :memory, bytes_range_fetches: bytes_range_fetches) do
    S3.download(storage_path, :memory, bytes_range_fetches: bytes_range_fetches)
  end
end
