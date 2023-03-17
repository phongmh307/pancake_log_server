defmodule LogServer.Pipeline.Extractor do
  @moduledoc """
  Log sẽ được shard theo thời gian(cụ thể là theo shard_interval), dùng làm file_name ở các client.
  Khi server phát lệnh thu gom log sẽ thu gom tất cả các log trong shard này sau đó thực hiên công việc
  hậu kì trước khi upload lên s3
  """
  require Logger
  alias LogServer.Pipeline.Extractor.Client
  alias LogServer.Tools
  @storage_folder (
    if System.get_env("DEV"),
      do: "buffer_area",
      else: "./data/buffer_area"
  )

  # Quét log ở các server shard theo thời gian nên các server sẽ cùng
  # generate ra 1 file_name giống nhau(tham khảo hàm generate_shard_time)
  # nên lúc quét log indentity chỉ cần là file_name
  # @spec extract([{binary(), non_neg_integer()}]) :: [binary()]
  def extract(clients, shard_time) do
    Logger.debug("#{__MODULE__}: Begin extract, shard_time: #{shard_time}")
    # TODO: return path
    Task.async_stream(
      clients,
      fn client ->
        collect_log(client, shard_time)
      end,
      timeout: 60_000,
      on_timeout: :kill_task
    )
    |> Enum.reduce([], fn {:ok, result}, acc ->
      case result do
        {:ok, path} -> acc ++ [path]
        {:error, _} -> acc
      end
    end)
  end

  # Tạo các thư mục có nhiệm vụ lưu trữ các file raw cũng như các file
  # đã được transfer về đúng cấu trúc trước khi upload lên storage
  defp setup_buffer_area(shard_time) do
    File.rm_rf!(Tools.join_storage_path([@storage_folder, shard_time, "raw_file"]))
    File.rm_rf!(Tools.join_storage_path([@storage_folder, shard_time, "metadata_file"]))
    File.rm_rf!(Tools.join_storage_path([@storage_folder, shard_time, "body_file"]))
    File.mkdir_p!(Tools.join_storage_path([@storage_folder, shard_time, "raw_file"]))
    File.mkdir_p!(Tools.join_storage_path([@storage_folder, shard_time, "metadata_file"]))
    File.mkdir_p!(Tools.join_storage_path([@storage_folder, shard_time, "body_file"]))
  end

  def collect_log(%Client{
    host: client_host,
    port: client_port,
    project: client_project
  }, log_indentity) do
    local_path = Tools.join_storage_path([@storage_folder, client_project, log_indentity, "raw_file", "#{client_host}:#{client_port}"])
    stream = File.stream!(local_path)
    try do
      "http://#{client_host}:#{client_port}/#{log_indentity}"
      |> HTTPStream.get()
      |> Stream.into(stream)
      |> Stream.run()

      {:ok, local_path}
    catch
      %HTTPStream.Adapter.HTTPoison.Error{status_code: status_code} ->
        IO.inspect("**************************************")
        IO.inspect("Host #{client_host} from project #{client_project} throw status_code #{status_code}")
        IO.inspect("**************************************")
        {:error, status_code}
    end
  end
end
