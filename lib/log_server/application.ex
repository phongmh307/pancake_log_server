defmodule LogServer.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      %{
        id: LogServer.Router,
        start: {
          LogServer.Router,
          :start_server,
          [[port: 2023]]
        }
      },
      LogServer.Scheduler,
      LogServer.TaskManager,
      LogServer.Storage.FileSystemManager,
      LogServer.Pipeline.Extractor.Client
    ]
    opts = [strategy: :one_for_one, name: LogServer.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
