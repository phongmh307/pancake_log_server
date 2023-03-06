defmodule LogServer.Tools do
  @epoch 62_167_219_200
  def to_atom_keys_map(%Date{} = date), do: date
  def to_atom_keys_map(%DateTime{} = datetime), do: datetime
  def to_atom_keys_map(%NaiveDateTime{} = datetime), do: datetime
  def to_atom_keys_map(string_map) when is_map(string_map), do: for {k, v} <- string_map, into: %{}, do: { (if is_atom(k), do: k, else: String.to_atom(k)), to_atom_keys_map(v)}
  def to_atom_keys_map(list) when is_list(list), do: Enum.map(list, fn elem -> to_atom_keys_map(elem)  end)
  def to_atom_keys_map(not_is_map), do: not_is_map

  def naive_utc_now(unit \\ :second) when unit in [:second, :millisecond, :microsecond] do
    NaiveDateTime.utc_now
    |> NaiveDateTime.truncate(unit)
  end

  def ecto_datetime_to_unix(nil), do: nil
  def ecto_datetime_to_unix(datetime) do
    # epoch = :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

    datetime
    |> NaiveDateTime.to_erl
    |> :calendar.datetime_to_gregorian_seconds
    |> Kernel.-(@epoch)
  end

  def naive_utc_now_second do
    NaiveDateTime.utc_now
    |> NaiveDateTime.truncate(:second)
  end

  def split_storage_path(path) do
    if System.get_env("DEV"),
      do: Path.split(path),
      else: Path.split(path) |> Enum.drop(2)
  end
end
