defmodule LogServer.CustomInteger do
  @moduledoc false
  # Phục vụ lưu integer offset đọc file có thể dài ra vô hạn nên sử dụng cách lưu bit tương tự
  # utf8 để cân bằng giữa khả năng parse và lượng byte cần lưu trữ

  def encode(integer) do
    # integer ->        binary     -> maybe_add_some_bits(các bit phục vụ đánh dấu độ dài)
    # Bước thứ 3(maybe_add_some_bits) để phục vụ sau này parse, khi parse sẽ đọc đến khi nào gặp bit có giá trị
    # là 0 sẽ dừng lại, sau đó lấy số bit 1 đằng trước số 0 để suy ra tổng độ dài của số để có thể đọc đầy đủ cả
    # giá trị ban đầu lên (0 số 1 => 1 byte, 1 số 1 => 2 byte, ...). Số bit đánh dấu max là 6 bit(tức tổng độ
    # dài lưu trữ 1 integer sẽ là 6 byte).
    # example:
    # 127     ->          01111111 -> 01111111
    # 129     ->          10000001 -> 10000000 10000001
    # 2554    -> 00001001 11111010 -> 10001001 11111010

    cond do
      integer < 2 ** 7  -> <<integer::8>>
      integer < 2 ** 14 -> <<0b10::2, integer::14>>
      integer < 2 ** 21 -> <<0b110::3, integer::21>>
      integer < 2 ** 28 -> <<0b1110::4, integer::28>>
      integer < 2 ** 35 -> <<0b11110::5, integer::35>>
      integer < 2 ** 42 -> <<0b111110::6, integer::42>>
      true -> raise RuntimeError, message: "out of range 0..#{2 ** 42 - 1}"
    end
  end

  # [100, 50]
  def decode(bytes, :full) when is_binary(bytes) do
    case bytes do
      <<0::1, value::7>> -> value
      <<0b10::2, value::14>> -> value
      <<0b110::3, value::21>> -> value
      <<0b1110::4, value::28>> -> value
      <<0b11110::5, value::35>> -> value
      <<0b111110::6, value::42>> -> value
      _ -> nil
    end
  end

  def decode(byte, :mark_length_byte) when is_binary(byte) do
    case byte do
      <<0::1, _::7>> -> 1
      <<0b10::2, _::6>> -> 2
      <<0b110::3, _::5>> -> 3
      <<0b1110::4, _::4>> -> 4
      <<0b11110::5, _::3>> -> 5
      <<0b111110::6, _::2>> -> 6
      _ -> nil
    end
  end

  def decode(device) do
    mark_length_byte = IO.binread(device, 1)
    case decode(mark_length_byte, :mark_length_byte) do
      nil -> nil
      1 -> decode(mark_length_byte, :full)
      length ->
        content_binary =
          mark_length_byte
          |> Kernel.<>(IO.binread(device, length - 1))
          |> decode(:full)
    end
  end
end
