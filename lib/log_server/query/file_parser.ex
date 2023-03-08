defmodule LogServer.Query.FileParser do
  @moduledoc false
  alias LogServer.{CustomInteger, Tools}
  alias LogServer.Pipeline.{Transformer}

  @buffer_reader_size 100 * 1024 # 100KB
  def parse(path, :metadata_file) do
    [_cache, project, time_shard, _metadata_file, key_shard] = Tools.split_storage_path(path)
    body_path = Tools.join_storage_path([project, time_shard, "body_file", key_shard])
    path
    |> File.open!([{:read_ahead, @buffer_reader_size}])
    |> do_parse(body_path, :metadata)
  end

  defp do_parse(io_device, body_path, :metadata, acc \\ []) do
    with  body_length when body_length != :done <-
            (
              case IO.binread(io_device, 4) do
                :eof -> :done
                binaries -> Transformer.binaries_to_decimal(binaries)
              end
            ),
          metadata_length <-
            (
              case IO.binread(io_device, 2) do
                :eof ->
                  raise RuntimeError, message: "#{__MODULE__}: got eof at metadata_length"
                binaries -> Transformer.binaries_to_decimal(binaries)
              end
            ),
          body_offset <-
            (
              case CustomInteger.decode(io_device) do
                nil ->
                  raise RuntimeError, message: "#{__MODULE__}: got eof at body_offset"
                result -> result
              end
            ),
          metadata_content <-
            (
              case IO.binread(io_device, metadata_length) do
                :eof ->
                  raise RuntimeError, message: "#{__MODULE__}: got eof at metadata_content"
                binaries -> Transformer.parse_content_metadata_binaries(binaries)
              end
            )
    do
      do_parse(
        io_device,
        body_path,
        :metadata,
        acc ++ [
          %{
            body_path: body_path,
            body_position: {body_offset, body_length},
            metadata: metadata_content,
          }
        ]
      )
    else
      :done -> acc
    end
  end
end
