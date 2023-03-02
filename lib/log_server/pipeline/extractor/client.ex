defmodule LogServer.Pipeline.Extractor.Client do
  @moduledoc false
  defstruct [
    :host,
    :port,
    :project
  ]

  @clients_dets_table :clients
  @clients_saved_file (
    if System.get_env("DEV"),
      do: "clients",
      else: "../data/clients"
  )

  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []}
    }
  end

  def start_link do
    :dets.open_file(
      @clients_dets_table,
      file: String.to_charlist(@clients_saved_file)
    )
    :ignore
  end

  def get_all() do
    :dets.match(@clients_dets_table, :"$1")
    |> Enum.map(fn [{
      %{
        host: host,
        port: port
      },
      project
    }] ->
      %__MODULE__{
        host: host,
        port: port,
        project: project
      }
    end)
  end

  def insert(%__MODULE__{
    host: host,
    port: port,
    project: project
  }) do
    :dets.insert(@clients_dets_table, {%{
      host: host,
      port: port
    }, project})
  end
end
