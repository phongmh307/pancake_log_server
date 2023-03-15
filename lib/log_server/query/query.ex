defmodule LogServer.Query do
  @moduledoc false
  defmodule Step do
    defstruct [
      :action,
      :params
    ]
  end

  # page_id, project, time
  defstruct [
    :project,
    :time_range,
    :page_id,
    query_params: [] # a: 1, b: 2
  ]

  alias LogServer.Pipeline.{Transformer, Loader}
  alias LogServer.{Pipeline, Tools}
  alias LogServer.Query.Executor
  alias LogServer.Storage.MetadataCache
  require Logger

  def query(query_struct) do
    query_struct
    |> build_query_plan()
    |> Executor.execute_plan()
  end

  def build_query_plan(%__MODULE__{
    project: project, #"local"
    time_range: {begin_time, end_time}, #{1676609400, 1676610000}
    page_id: page_id, #"123"
    query_params: query_params
  }) when page_id != nil do
    begin_time_shard = Pipeline.to_shard_time(begin_time)
    end_time_shard = Pipeline.to_shard_time(end_time)
    # loading là trạng thái các step chưa đầy đủ dữ liệu để thực thi
    metadata_storage_paths =
      find_storage_paths(
        project: project,
        time_shard: [begin_time_shard, end_time_shard],
        key_shard: Transformer.key_shard(%{"page_id" => page_id})
      )

    Logger.debug("#{__MODULE__}: Begin query, total metadata shard scan: #{length(metadata_storage_paths)}")
    [
      %Step{
        action: :load_metadata_storage,
        params: %{metadata_storage_paths: metadata_storage_paths}
      },
      %Step{
        action: :parse_metadata,
        params: %{metadata_dest_paths: :loading}
      },
      %Step{
        action: :query_metadata,
        params: %{
          query_params: Keyword.put(query_params, :page_id, page_id),
          metadata_content: :loading
        }
      },
      %Step{
        action: :query_body_and_rebuild_raw_log,
        params: %{
          metadata_passed: :loading,
          body_dest_paths: :loading
        }
      }
    ]
  end

  # time_range -> trả về các shard chứa time_range
  def to_time_shards_load(begin_time, end_time, acc \\ []) do
    current_shard_time = Integer.to_string(begin_time)
    next_shard_time = Pipeline.to_next_shard_time(begin_time)
    current_shard_load = "#{current_shard_time}_#{next_shard_time}"
    new_acc = acc ++ [current_shard_load]
    if begin_time >= end_time,
      do: new_acc,
      else: to_time_shards_load(next_shard_time, end_time, new_acc)
  end

  def find_storage_paths(params) do
    with  {true, path} <-
            (
              project = params[:project]
              {!is_nil(project), project}
            ),
          {true, paths} <-
            (
              time_shards_load = apply(__MODULE__, :to_time_shards_load, params[:time_shard])
              {length(time_shards_load) != 0, Enum.map(time_shards_load, & Tools.join_storage_path([path, &1]))}
            ),
          paths <- Enum.map(paths, & Tools.join_storage_path([&1, "metadata_file", params[:key_shard]]))
    do
      paths
    else
      {_, paths} when is_list(paths) -> paths
      {_, path} -> [path]
    end
  end
end
