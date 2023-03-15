defmodule LogServer.Query.Executor do
  @moduledoc false
  alias ElixirSense.Core.Metadata
  alias LogServer.Pipeline.Transformer
  alias LogServer.Query.{Step, FileParser}
  alias LogServer.Pipeline.{Loader}
  alias LogServer.{TaskManager, Storage, Tools}
  alias LogServer.Storage.MetadataCache
  require Logger

  @timestamp_field "timestamp"

  def execute_plan(query_plan, current_step \\ 0) do
    current_step_execute = Enum.fetch!(query_plan, current_step)
    step_return_value = execute_step(current_step_execute)
    if length(query_plan) == (current_step + 1) do
      step_return_value
    else
      query_plan
      |> filling_plan_by_step_info(current_step_execute, step_return_value)
      |> execute_plan(current_step + 1)
    end
  end

  # plan -> filled_plan
  defp filling_plan_by_step_info(
    query_plan,
    %Step{action: did_action} = did_step,
    step_return_value
  ) do
    Enum.map(query_plan, fn step = %Step{action: action, params: params} ->
      cond do
        did_action == :load_metadata_storage and action == :parse_metadata ->
          filled_params = %{params | metadata_dest_paths: step_return_value}
          %{step | params: filled_params}
        did_action == :parse_metadata and action == :query_metadata ->
          filled_params = %{params | metadata_content: step_return_value}
          %{step | params: filled_params}
        did_action == :query_metadata and action == :query_body_and_rebuild_raw_log ->
          filled_params = %{params | metadata_passed: step_return_value}
          %{step | params: filled_params}
        true -> step
      end
    end)
  end

  defp execute_step(%Step{
    action: :load_metadata_storage,
    params: %{metadata_storage_paths: metadata_storage_paths}
  }) do
    Task.async_stream(metadata_storage_paths, fn storage_path ->
      # Bắt buộc sử dụng TaskManager cho hàm load storage này vì bên trong hàm có thể xảy ra race-condition
      TaskManager.do_task({Storage, :download, [storage_path]})
      # Storage.download(storage_path)
    end, timeout: 20_000)
    |> Enum.reduce([], fn {:ok, result}, acc ->
      case result do
        {:ok, metadata_dest_path} -> acc ++ [metadata_dest_path]
        {:error, _} -> acc
      end
    end)
  end

  defp execute_step(%Step{
    action: :parse_metadata,
    params: %{metadata_dest_paths: metadata_dest_paths}
  }) do
    Task.async_stream(metadata_dest_paths, fn metadata_dest_path ->
      [_data_folder | metadata_path] = Tools.split_storage_path(metadata_dest_path)
      {
        metadata_path,
        FileParser.parse(metadata_dest_path, :metadata_file)
      }
    end, timeout: 10000)
    |> Enum.reduce([], fn {:ok, {metadata_path, metadata_content}}, acc ->
      MetadataCache.set(
        Tools.join_storage_path(metadata_path),
        metadata_content
      )
      acc ++ metadata_content
    end)
  end

  defp execute_step(%Step{
    action: :query_metadata,
    params: %{
      query_params: query_params,
      metadata_content: metadata_content
    }
  }) do
    Enum.filter(metadata_content, fn %{metadata: metadata} ->
      Enum.all?(query_params, fn {key, value} ->
        metadata[to_string(key)] == value
      end)
    end)
  end

  defp execute_step(%Step{
    action: :load_body_storage,
    params: %{body_storage_paths: body_storage_paths}
  }) do
    # Logger.debug("#{LogServer.Query}: Middle query, total body shard scan: #{length(body_storage_paths)}")
    Task.async_stream(body_storage_paths, fn storage_path ->
      # Bắt buộc sử dụng TaskManager cho hàm load storage này vì bên trong hàm có thể xảy ra race-condition
      TaskManager.do_task({Storage, :download, [storage_path]})
      # Storage.download(storage_path)
    end)
    |> Enum.reduce([], fn {:ok, result}, acc ->
      case result do
        {:ok, metadata_dest_path} -> acc ++ [metadata_dest_path]
        {:error, _} -> acc
      end
    end)
  end

  @cache_folder(
    if System.get_env("DEV"),
      do: "cache",
      else: "../data/cache"
  )
  defp execute_step(%Step{
    action: :query_body_and_rebuild_raw_log,
    params: %{
      metadata_passed: metadata_passed
    }
  }) do
    {log_content, _} =
      metadata_passed
      |> Task.async_stream(fn %{
        body_path: body_path,
        body_position: {body_offset, body_length},
        metadata: metadata
      } ->
        case Storage.download(
          body_path,
          dest_path: :memory,
          bytes_range_fetches: {body_offset, body_offset + body_length}
        ) do
          {:ok, body} ->
            {:ok, %{
              metadata: metadata,
              body: body
            }}
          error -> error
        end
      end)
      |> Enum.reduce({"", 1}, fn {:ok, result}, {log_content, log_count} = acc ->
        case result do
          {:ok, %{metadata: metadata, body: body}} ->
            metadata_layout =
              metadata
              |> Map.delete("timestamp")
              |> Enum.map_join(", ", fn {k, v} -> "#{k}=#{v}" end)
            {
              log_content
              <> "#{metadata[@timestamp_field]} UTC [#{log_count}] "
              <> "METADATA LOG: #{metadata_layout} || "
              <> "BODY LOG: #{body}\n",
              log_count + 1
            }
          {:error, _} -> acc
        end
      end)

    log_content
  end
end
