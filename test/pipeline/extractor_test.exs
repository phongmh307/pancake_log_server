defmodule LogCake.Pipeline.ExtractorTest do
  alias LogServer.Pipeline
  use ExUnit.Case, async: true

  setup do
    Application.started_applications |> IO.inspect(label: "started apps")
    # Application.ensure_started(:pancake_log)
    storage_path = Application.get_env(:pancake_log, :storage_path)
    File.rm_rf!("#{storage_path}/**")

    Enum.each(1..100, fn _ ->
      metadata = [partition_key: Faker.Lorem.characters(100) |> to_string(), random: :rand.uniform(1_000_000)]
      IO.inspect(metadata, label: "metadata")

      LogCake.log(
        Faker.Lorem.paragraph(2),
        metadata
      )
    end)
  end

  test "Extract log file from client" do
    client_port = Application.get_env(:pancake_log, :adapter_port)
    client_spec = [{"localhost", client_port}]
    paths = Pipeline.Extractor.extract(client_spec)
    assert Enum.all?(paths, &File.exists?/1)
    assert length(client_spec) == length(paths)
  end
end
