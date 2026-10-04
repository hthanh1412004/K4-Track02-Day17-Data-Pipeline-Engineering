# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Nguyễn Hữu Thành / 2A202602807

**Repo:** https://github.com/hthanh1412004/K4-Track02-Day17-Data-Pipeline-Engineering.git

**Commit bài nộp:** `65c46922ce14e7eeedcef9656d3064ba478e6f56`

**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex được dùng để đọc yêu cầu, gợi ý hướng tìm lỗi, hỗ trợ sửa code, chạy kiểm thử và rà soát báo cáo. Các thay đổi được tôi review lại bằng test, checksum và dbt parity trước khi đưa vào bài nộp.

**Nguồn tham khảo khác:** Tài liệu và mã nguồn có sẵn trong repository.

## 1. Ba lỗi

|                 | Lỗi Silver                                                                                                                           | Lỗi late data                                                                                                          | Lỗi xoá CDC                                                                                                                           |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng** | `silver_tickets` có 24 dòng nhưng chỉ có 12 `ticket_id`. Riêng T-91 xuất hiện cả ba trạng thái cũ và mới.                            | Checksum của bảng feature không khớp full recompute. u05 ngày 12/08 chỉ có 2 event, trong khi kết quả đúng là 5 event. | T-97 đã bị xoá ở nguồn nhưng vẫn còn dữ liệu cá nhân trong Silver, snapshot mới nhất và RAG chunks.                                   |
| **Nguyên nhân** | Code chỉ loại bản ghi trùng trong từng batch rồi `INSERT`, nên các trạng thái của cùng ticket ở nhiều ngày vẫn bị cộng dồn.          | `LOOKBACK_DAYS` đang bằng 0. Event xảy ra ngày 12 nhưng tới ngày 15 không làm bảng feature ngày 12 được tính lại.      | Bản ghi delete có `after = null`, trong khi staging chỉ đọc `ticket_id` từ `after`, nên delete bị lọc mất.                            |
| **Cách sửa**    | Đổi sang `MERGE` theo `ticket_id`. Chỉ cập nhật khi `_lsn` mới lớn hơn `_lsn` đang có để replay ngày cũ không ghi đè trạng thái mới. | Đo lateness từ Bronze được P99 bằng 3 ngày, nên đặt lookback bằng 3 và tính lại feature theo ngày xảy ra event.        | Lấy khoá lần lượt từ `after`, `before` hoặc Kafka key. CDC delete được giữ lại thành tombstone; Kafka tombstone rỗng vẫn được bỏ qua. |
| **Khái niệm**   | Upsert theo khoá, idempotency và thứ tự CDC bằng LSN.                                                                                | Event time, ingest time, lookback và overwrite partition.                                                              | Debezium delete, Kafka tombstone và truyền trạng thái xoá xuống Gold.                                                                 |

## 2. Các con số

- P50/P95/P99/max lateness: `0.00 / 2.90 / 3.00 / 3` ngày → `LOOKBACK_DAYS = 3`.
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f` cho C0 = C1 = C2 = C3.
- dbt: **PASS=19**; parity: **PARITY** cho `silver_tickets` và `gold_feature_daily`.

## 3. Lựa chọn công cụ / kỹ thuật

- `silver_tickets` lưu trạng thái hiện tại của mỗi ticket nên phù hợp với `MERGE` theo khoá. `gold_feature_daily` là số liệu tổng hợp theo ngày nên khi có event đến muộn cần xoá và tính lại cả partition.
- Tombstone được giữ trong Silver để nhớ rằng ticket đã bị xoá. Nhờ `_lsn`, việc chạy lại batch cũ cũng không thể làm ticket xuất hiện trở lại.
- Snapshot training được dựng từ Bronze theo dữ liệu đã có ở từng ngày. Snapshot cũ được giữ nguyên để có thể kiểm tra lại kết quả trước đây; snapshot mới sẽ phản ánh bản ghi delete.
- Dữ liệu của bài nhỏ và chạy local nên DuckDB gọn hơn Spark. dbt được dùng như một cách triển khai thứ hai để kiểm tra lại logic `MERGE` và microbatch.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot bất biến có ích cho việc tái hiện kết quả, nhưng yêu cầu xoá dữ liệu cá nhân vẫn phải được ưu tiên. Khi có yêu cầu xoá, cần có một danh sách xoá dùng chung để dọn dữ liệu khỏi bảng đang dùng, RAG, cache và các snapshot có chứa PII. Snapshot cũ có thể được dựng lại sau khi ẩn danh hoặc bị giới hạn thời gian lưu. Log kiểm toán chỉ cần giữ mã bản ghi, thời gian và kết quả xử lý, không cần giữ nội dung đã xoá. Trong phạm vi bài lab, T-97 mới chỉ được loại khỏi snapshot mới nhất và RAG nên chưa thể xem là quy trình xoá hoàn chỉnh cho production.
2. Chốt đầu tiên nên đặt ở bước Bronze → Silver: email, số điện thoại, tên và địa chỉ được nhận diện trước khi dữ liệu đi tiếp. Regex phù hợp với email và số điện thoại, còn tên hoặc địa chỉ tiếng Việt cần thêm NER/DLP. Trước khi ghi Gold hoặc tạo embedding nên quét thêm một lần; dòng còn PII sẽ đi vào quarantine thay vì được publish. Chất lượng chốt được đo trên tập dữ liệu tiếng Việt đã gán nhãn bằng precision, recall, F1 và số dòng bị lọt PII. Bronze vẫn cần mã hoá, phân quyền chặt và có thời hạn lưu riêng.

## 5. Output thực tế

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.64s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
fresh build             8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
[OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
[OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

## 6. Bằng chứng bonus

- **B1 (+5):** cache LLM theo `SHA-256(input) + model + prompt_version`, kiểm schema và chuyển output sai sang `llm_label_quarantine` trong `pipeline/llm_label.py`.
- **B2 (+5):** phương án brainstorm tại [`bonus/DESIGN.md`](../bonus/DESIGN.md) — 1.511 từ, 5 quyết định có trade-off, phương án bị loại và sơ đồ kiến trúc.

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
