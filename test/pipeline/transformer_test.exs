defmodule LogCake.Pipeline.TransformerTest do
  alias LogServer.Pipeline.Transformer
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

    paths = Pipeline.Extractor.extract(client_spec)
    %{paths: paths}
  end

  test "Extract log file from client", %{paths: paths} do
    Transformer.transform(paths)
  end
end
