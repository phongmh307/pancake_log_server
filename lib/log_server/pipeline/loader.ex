defmodule LogServer.Pipeline.Loader do
  @moduledoc false
  alias LogServer.Storage
  @bucket if System.get_env("DEV"), do: "pancake_log_server.dev", else: "cc"
  def upload(storage_path) when is_binary(storage_path) do
    Storage.upload(storage_path)
  end

  def upload(_), do: nil
end
