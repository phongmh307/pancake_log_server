defmodule LogServer.MixProject do
  use Mix.Project

  def project do
    [
      app: :log_server,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :http_stream],
      mod: {LogServer.Application, []}
    ]
  end

  defp deps do
    [
      {:plug, "~> 1.10"},
      {:plug_cowboy, "~> 1.0"},
      {:plug_crypto, "~> 1.1.2"},
      {:httpoison, "~> 1.7.0"},
      {:poison, "~> 5.0"},
      {:quantum, "~> 3.0"},
      {:http_stream,
        git: (
          if System.get_env("DEV"),
            do: "git@github.com:taonhanvat/http_stream.git",
            else: "https://github.com/taonhanvat/http_stream.git"
        ),
        ref: "34a90018afaaeb8afd2e1a23e704245f895ecc11"
      },
      {:ex_aws_s3, "~> 2.4.0"},
      {:sweet_xml, "~> 0.7.3"},
      {:pancake_log,
        git: (
          if System.get_env("DEV"),
            do: "git@github.com:Goose97/pancake-log.git",
            else: "https://github.com/Goose97/pancake-log.git"
        ),
        ref: "90ccf7a4cd13d65329a3d0b0df1acc911c59e157"
      },
      {:faker, "~> 0.17", only: :test},
      {:ex_doc, "~> 0.27", only: :dev, runtime: false},
    ]
  end
end
