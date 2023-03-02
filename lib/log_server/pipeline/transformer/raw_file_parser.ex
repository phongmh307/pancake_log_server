defmodule LogServer.Pipeline.Transformer.RawFileParser do
  @moduledoc """
  Documentation for `Transformer`.
  raw record log structure:
  |   4 bytes   |     2 bytes      |  <65KB   | <4GB |
  | body_length | metadata_length  | metadata | body |
  <-      mark_position_bytes     -><- content_bytes->
  """

  alias LogServer.Pipeline.Transformer
  defstruct [
    :raw_fd,
    :next_part,
    state: %{}
  ]

  def into_stream(raw_fd) do
    Stream.resource(
      fn -> %__MODULE__{
        raw_fd: raw_fd,
        next_part: find_next_part(:init)
      } end,
      &handle_chunk/1,
      &handle_close/1
    )
  end

  defp handle_chunk(
    %__MODULE__{
      raw_fd: raw_fd,
      next_part: :body_length,
      state: state
    } = acc
  ) do
    content_bytes =
      case IO.binread(raw_fd, 4) do
        :eof -> {:halt, nil}
        content_bytes ->
          {
            [{:body_length, content_bytes}],
            %{
              acc |
              next_part: find_next_part(:body_length),
              state: Map.put(state, :body_length, Transformer.binaries_to_decimal(content_bytes))
            }
          }
      end
  end

  defp handle_chunk(
    %__MODULE__{
      raw_fd: raw_fd,
      next_part: :metadata_length,
      state: state
    } = acc
  ) do
    content_bytes = IO.binread(raw_fd, 2)
    {
      [{:metadata_length, content_bytes}],
      %{
        acc |
        next_part: find_next_part(:metadata_length),
        state: Map.put(
          state,
          :metadata_length,
          Transformer.binaries_to_decimal(content_bytes)
        )
      }
    }
  end

  defp handle_chunk(
    %__MODULE__{
      raw_fd: raw_fd,
      next_part: :metadata,
      state: %{metadata_length: metadata_length}
    } = acc
  ) do
    content_bytes = IO.binread(raw_fd, metadata_length)
    {
      [{:metadata, content_bytes}],
      %{acc | next_part: find_next_part(:metadata)}
    }
  end

  defp handle_chunk(
    %__MODULE__{
      raw_fd: raw_fd,
      next_part: :body,
      state: %{body_length: body_length}
    } = acc
  ) do
    content_bytes = IO.binread(raw_fd, body_length)
    {
      [{:body, content_bytes}],
      %{acc | next_part: find_next_part(:body)}
    }
  end

  def handle_close(_) do
    nil
  end

  defp find_next_part(current_part) do
    case current_part do
      :init -> :body_length
      :body_length -> :metadata_length
      :metadata_length -> :metadata
      :metadata -> :body
      :body -> :body_length
    end
  end
end
