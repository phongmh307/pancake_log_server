defmodule LogServer.Router do
  alias LogServer.{Query, Tools}
  alias LogServer.Pipeline.Extractor.Client

  def start_server(port: port),
    do: Plug.Cowboy.http(__MODULE__, [], port: port)

  def init(_),
    do: :ignore

  use Plug.Router
  plug(Plug.Parsers, parsers: [:urlencoded])
  plug(:match)
  plug(:dispatch)

  @hour 60 * 60
  @time_range_thresold 60 * 60 * 24 * 30 # 10day
  get "/query" do
    %Plug.Conn{params: params} = conn
    parse_time = fn time ->
      try do
        time
        |> NaiveDateTime.from_iso8601!()
        |> Tools.ecto_datetime_to_unix()
      rescue
        _ ->
          {time, _} = Integer.parse(time)
          time
      end
    end

    query_struct =
      if project = params["project"],
        do: %Query{project: project},
        else: send_resp(conn, 400, "require param project")

    %Query{time_range: {begin_query, end_query}} = query_struct =
      cond do
        params["begin"] && params["end"] ->
          begin_query = parse_time.(params["begin"])
          end_query = parse_time.(params["end"])
          %{query_struct | time_range: {begin_query, end_query}}
        begin_query = params["begin"] ->
          begin_query = parse_time.(begin_query)
          end_query = begin_query + @hour
          %{query_struct | time_range: {begin_query, end_query}}
        end_query = params["end"] ->
          end_query = parse_time.(end_query)
          begin_query = end_query - @hour
          %{query_struct | time_range: {begin_query, end_query}}
        true ->
          begin_query = System.os_time(:second) - @hour
          end_query = System.os_time(:second)
          %{query_struct | time_range: {begin_query, end_query}}
      end

    query_struct =
      if page_id = params["page_id"],
        do: %{query_struct | page_id: page_id},
        else: query_struct

    query_struct =
      if (params = Map.drop(params, ["project", "begin", "end", "page_id"])) != %{},
        do: %{query_struct | query_params: Enum.map(params, fn({key, value}) -> {String.to_atom(key), value} end)},
        else: query_struct

    result =
      if end_query - begin_query > @time_range_thresold,
        do: "query time is out of range (for now maximum is 30 day)",
        else: Query.query(query_struct)

    send_resp(conn, 200, result)
  end

  patch "/client" do
    params = conn.params
    with {_, host} when is_binary(host) <- {:host, params["host"]},
         {_, port} when is_binary(port) <- {:port, params["port"]},
         {_, project} when is_binary(project) <- {:project, params["project"]} do
      %Client{
        host: host,
        port: port,
        project: project
      }
      |> Client.insert()
      send_resp(conn, 200, Poison.encode!(%{host: host, port: port, project: project}))
    else
      {:host, _} ->
        send_resp(conn, 400, "required host (ip server)")
      {:port, _} ->
        send_resp(conn, 400, "required port (port running Log Client)")
      {:project, _} ->
        send_resp(conn, 400, "required project (your server belong to project?)")
    end
  end
end
