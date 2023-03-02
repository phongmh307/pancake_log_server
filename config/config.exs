import Config

config :http_stream,
  adapter: HTTPStream.Adapter.HTTPoison

config :log_server, LogServer.Scheduler,
  jobs: [
    [
      name: "opening_pipeline",
      schedule: "*/10 * * * *",
      task: {
        LogServer.Pipeline,
        :open,
        []
      }
    ]
  ]

config :ex_aws,
  region: "vn-hn-cmc",
  s3: [host: "statics.pancake.vn", region: "vn-hn-cmc", scheme: "https://"],
  json_codec: Jason,
  access_key_id: [
    {:system, "AWS_ACCESS_KEY_ID"},
    :instance_role
  ],
  secret_access_key: [
    {:system, "AWS_SECRET_ACCESS_KEY"},
    :instance_role
  ]

config :ex_aws, :s3,
  scheme: "https://",
  host: "statics.pancake.vn",
  region: "vn-hn-cmc"

import_config "#{Mix.env}.exs"
