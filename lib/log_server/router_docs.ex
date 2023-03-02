defmodule OneWa.RouterDocs do
  @moduledoc """
  API document
  """

  @doc """
  API để query logs

  url: /query

  method: GET

  ## Require Params:
    * `project`: Tên project(là tên project mà bạn đã đăng kí client ở API put client)

  ## Optional Params:
    * `page_id`(string): Đây là key để đánh index nên khuyến khích sử dụng khi query(vẫn có thể query
    không có page_id). Nếu cần key index mới phù hợp với project thì liên hệ dev.
    * `begin`, `end`(unix_timstamp): Đây là 2 param dùng để đánh dấu khoảng thời gian muốn query.
    Nếu không có cả 2 thì sẽ query trong 1 tiếng gần nhất. Nếu chỉ có `begin`
    thì query từ begin -> begin + 1 hour. Tương tự với chỉ có `end`
    * Còn lại là các free params đi kèm để lọc các bản ghi log

  """
  def query(url), do: doc!([url])

  @doc """
  API để đăng kí thêm client sản sinh log

  url: /client

  method: PATCH

  ## Require Params:
    * `host`: Ip client
    * `port`: Port config chạy project log client(LogCake)
    * `project`: Tên project của con server đang muốn log
  """
  def patch_client(url), do: doc!([url])

  defp doc!(_) do
    raise "document"
  end
end
