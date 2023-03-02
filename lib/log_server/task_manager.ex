defmodule LogServer.TaskManager do
  @moduledoc """
  Module để phục vụ các heavy get hoặc hàm chứa heavy get
  Logic: giả sử có nhiều heavy get cùng target 1 resource thì khi gọi các heavy get qua
  module này thì chỉ có 1 request thực sự thực hiện heavy get sau đó sẽ gửi kết quả đến
  các request khác. Các request khác chỉ cần ngồi chờ kết quả.
  """
  use GenServer
  # api
  @task :task
  @task_pids :task_pids
  @default_timeout 50_000

  def do_task({module, function, args} = task, opts \\ []) do
    task_key = Keyword.get(opts, :task_key, task)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    :ets.insert(@task_pids, {task_key, self()})
    maybe_create_task_worker(task, task_key)

    receive do
      {:task_result, result} ->
        result
    after
      timeout ->
        GenServer.cast(__MODULE__, {:report_abnormal_task, task_key})
        apply(module, function, args)
    end
  end

  defp maybe_create_task_worker({module, function, args}, task_key) do
    if :ets.insert_new(@task, {task_key}) do
      spawn(fn ->
        apply(module, function, args)
        |> cleaning_and_dispatch(task_key)
      end)
    end
  end

  defp cleaning_and_dispatch(result, task_key) do
    release_task(task_key)

    @task_pids
    |> :ets.lookup(task_key)
    |> Enum.each(fn {_ets_key, subscriber_pid} ->
      send(subscriber_pid, {:task_result, result})
    end)

    :ets.delete(@task_pids, task_key)
  end

  defp release_task(task_key),
    do: :ets.delete(@task, task_key)

  # server
  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_) do
    :ets.new(@task, [:named_table, :public])
    :ets.new(@task_pids, [:named_table, :public, :duplicate_bag])
    :timer.apply_interval(10_000, GenServer, :cast, [__MODULE__, {:release_lock_abnormal_tasks, nil}])
    {:ok, %{}}
  end

  def handle_cast({:report_abnormal_task, task_key}, state) do
    new_state = Map.update(state, task_key, 1, fn report_count -> report_count + 1 end)
    {:noreply, new_state}
  end

  @abnormal_threshold 3
  def handle_cast({:release_lock_abnormal_tasks, _}, state) do
    Enum.each(state, fn {abnormal_task_key, report_count} ->
      if is_number(report_count) && report_count >= @abnormal_threshold,
        do: release_task(abnormal_task_key)

    end)
    {:noreply, %{}}
  end
end
