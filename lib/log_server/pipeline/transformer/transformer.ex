defmodule LogServer.Pipeline.Transformer do
  @moduledoc """
  Documentation for `Transformer`.
  raw record log structure:
  |   4 bytes   |     2 bytes      |  <65KB   | <4GB |
  | body_length | metadata_length  | metadata | body |
  <-      mark_position_bytes     -><- content_bytes->
  """

  defmodule FileCursorPositionManager do
    use GenServer

    def start_link(path: path) do
      GenServer.start_link(__MODULE__, [], name: generate_process_name(path))
    end

    def prepare_write(path, bytes) do
      GenServer.call(generate_process_name(path), {:prepare_write, bytes})
    end

    def get_current_position(path) do
      GenServer.call(generate_process_name(path), :get_current_position)
    end

    def generate_process_name(path) do
      Module.concat(__MODULE__, path)
    end

    def init(_opts) do
      {:ok, 0}
    end

    def handle_call({:prepare_write, bytes}, _from, current_position) do
      {:reply, current_position, current_position + byte_size(bytes)}
    end

    def handle_call(:get_current_position, _from, current_position) do
      {:reply, current_position, current_position}
    end
  end

  alias LogServer.CustomInteger
  alias LogServer.Pipeline.Transformer.RawFileParser
  alias LogServer.Tools

  @buffer_reader_size 100 * 1024 # 100KB
  @buffer_writer_size 100 * 1024 # 100KB
  @total_key_shard_number 30
  @default_key_shard "-1"

  def transform(source_paths) when length(source_paths) != 0 do
    storage_path = find_storage_path(source_paths)
    @total_key_shard_number
    |> generate_key_shards()
    |> Enum.each(fn key_shard ->
      # Tạo ra các process maintain vị trí con trỏ để các lệnh ghi có thông tin
      # vị trí cần ghi. Cụ thể ở đây tạo ra các process maintain con trỏ cho các file
      # body vì file body cần trả về vị trí các byte vừa ghi vào để phục vụ sau này query.
      # Các process maintain vị trí con trỏ sẽ exit cùng process transform nay`
      FileCursorPositionManager.start_link(
        path: generate_body_path(storage_path, key_shard)
      )
    end)

    source_paths
    |> Task.async_stream(fn source_path ->
      do_transform(source_path, storage_path)
    end, timeout: :infinity)
    |> Stream.run()

    clear_empty_files_after_transform(storage_path)
    storage_path
  end

  def transform(_), do: nil

  defp find_storage_path(paths) do
    path_uniq_by_time_shard =
      Enum.uniq_by(paths, fn path ->
        [_buffer_area, project, time_shard, _raw_file, _file_name] = Tools.split_storage_path(path) |> IO.inspect(label: "test123")
        {project, time_shard}
      end)

    case path_uniq_by_time_shard do
      [path] ->
        [buffer_area, project, time_shard, _raw_file, _file_name] = Tools.split_storage_path(path)
        Tools.join_storage_path([buffer_area, project, time_shard])
      _ -> raise ArgumentError, "expected paths from one project, one time_shard"
    end
  end

  # fd
  defp do_transform(source_path, storage_path) do
    {:ok, source_fd} = File.open(source_path, [{:read_ahead, @buffer_reader_size}])
    metadata_devices =
      @total_key_shard_number
      |> generate_key_shards()
      |> Enum.map(fn key_shard ->
        {
          String.to_atom("metadata_#{key_shard}"),
          File.open!(
            generate_metadata_path(storage_path, key_shard),
            [{:delayed_write, @buffer_writer_size, 10 * 1000}, :append]
          )
        }
      end)
      |> Enum.into(%{})

    body_devices =
      @total_key_shard_number
      |> generate_key_shards()
      |> Enum.map(fn key_shard ->
        {
          String.to_atom("body_#{key_shard}"),
          File.open!(
            generate_body_path(storage_path, key_shard),
            [{:delayed_write, @buffer_writer_size, 10 * 1000}, :write]
          )
        }
      end)
      |> Enum.into(%{})

    source_fd
    # build đầu ra của file vào 1 stream để emit ra từng phần tử của record
    |> RawFileParser.into_stream()
    |> Stream.chunk_every(4)
    |> Stream.map(fn ([
      body_length: body_length,
      metadata_length: metadata_length,
      metadata: metadata,
      body: body
    ]) = params ->
      key_shard =
        metadata
        |> parse_content_metadata_binaries()
        |> key_shard()

      # write to body file
      # Body file không cần cấu trúc gì cả chỉ cần ghi vào và có trách nhiêm trả về
      # vị trí vừa ghi(tức là offset và độ dài byte vừa ghi vào file) để metadata lưu
      # lại phục vụ query
      body_device = Map.get(body_devices, String.to_atom("body_#{key_shard}"))
      body_write_offset =
        generate_body_path(storage_path, key_shard)
        |> FileCursorPositionManager.prepare_write(body)

      :file.pwrite(body_device, body_write_offset, body)

      # write to metadata file
      # metadata log structure:
      # Total Bytes:     4 bytes      |       2 bytes       |  Dynamic Bytes(explain in LogServer.CustomInteger module) |    <65KB
      # Name:          body_length    |   metadata_length   |                       body_offset                         |   metadata
      # Job:        cùng nhiệm vụ với | xác định vị trí các | Cùng với body_length xác định vị trí trong file body ứng  | Lưu nội dung
      #                body_offset    |   bytes metadata    |                   với log metadata này                    | của metadata
      metadata_device = Map.get(metadata_devices, String.to_atom("metadata_#{key_shard}"))
      body_offset_encoded = CustomInteger.encode(body_write_offset)
      finished_metadata_log = body_length <> metadata_length <> body_offset_encoded <> metadata
      IO.binwrite(metadata_device, finished_metadata_log)
    end)
    |> Stream.run()
  end

  defp clear_empty_files_after_transform(storage_path) do
    metadata_devices =
      @total_key_shard_number
      |> generate_key_shards()
      |> Enum.each(fn key_shard ->
        metadata_path = generate_metadata_path(storage_path, key_shard)
        if File.read!(metadata_path) == "",
          do: File.rm_rf!(metadata_path)
      end)

    body_devices =
      @total_key_shard_number
      |> generate_key_shards()
      |> Enum.map(fn key_shard ->
        body_path = generate_body_path(storage_path, key_shard)
        if File.read!(body_path) == "",
          do: File.rm_rf!(body_path)
      end)
  end

  def binaries_to_decimal(binaries) do
    mark_bits = byte_size(binaries) * 8
    <<mark_length::size(mark_bits)>> = binaries
    mark_length
  end

  defp find_new_part_of_raw_log(current_part) do
    case current_part do
      :body_length -> :metadata_length
      :metadata_length -> :metadata
      :metadata -> :body
      :body -> :body_length
    end
  end

  # bytes -> map
  def parse_content_metadata_binaries(
    <<byte_value>> <> tail,
    {kv_list, current_key, buffer} \\ {[], nil, []}
  ) do
    action =
      cond do
        byte_value == 0 -> :end_key_value
        byte_value == 1 -> :end_key
        tail == "" -> :end_metadata
        true -> :continue
      end

    {kv_list, current_key, buffer} =
      case action do
        :continue -> {kv_list, current_key, buffer ++ [byte_value]}
        :end_key ->
          key = List.to_string(buffer)
          {kv_list, key, []}
        :end_key_value ->
          value = List.to_string(buffer)
          {kv_list ++ [{current_key, value}], nil, []}
        :end_metadata ->
          value = List.to_string(buffer ++ [byte_value])
          {kv_list ++ [{current_key, value}], nil, []}
      end

    if tail == "",
      do: Map.new(kv_list),
      else: parse_content_metadata_binaries(tail, {kv_list, current_key, buffer})
  end

  def key_shard(%{"page_id" => key_shard}) when key_shard != nil,
    do: do_key_shard(key_shard)

  def key_shard(_),
    do: @default_key_shard

  defp do_key_shard(key_shard) do
    key_shard
    |> :erlang.phash2(@total_key_shard_number)
    |> to_string()
  end

  defp generate_body_path(storage_path, key_shard) do
    Tools.join_storage_path([storage_path, "body_file", to_string(key_shard)])
  end

  defp generate_metadata_path(storage_path, key_shard),
    do: Tools.join_storage_path([storage_path, "metadata_file", to_string(key_shard)])

  defp generate_key_shards(total_shard) do
    0..(total_shard - 1)
    |> Enum.to_list()
    |> List.insert_at(-1, @default_key_shard)
  end
end
