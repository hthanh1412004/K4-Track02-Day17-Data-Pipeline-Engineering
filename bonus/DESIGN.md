# Bonus B2 — Pipeline chống gian lận thanh toán thời gian gần thực

## Bài toán và ràng buộc

Tôi chọn thiết kế pipeline feature cho hệ thống phát hiện gian lận thanh toán của một ví điện tử tại Việt Nam. Người dùng trực tiếp là dịch vụ chấm điểm rủi ro: trước khi cho phép giao dịch, dịch vụ cần các feature như số giao dịch thất bại trong 10 phút, tổng tiền trong 24 giờ, số thiết bị mới trong 30 ngày và khoảng cách địa lý so với giao dịch gần nhất. Người dùng thứ hai là nhóm data science, cần đúng cùng định nghĩa feature để huấn luyện và đánh giá model.

Dữ liệu đến từ CDC tài khoản/KYC, event giao dịch, đăng nhập, thay đổi thiết bị, kết quả chargeback và danh sách chặn. Chúng có JSON khác phiên bản, event trùng do retry, event đến muộn khi thiết bị mất mạng, clock sai, CDC delete và cả cập nhật ngược trạng thái giao dịch. Dữ liệu chứa PII và thông tin tài chính nên khả năng truy vết, quyền xoá và kiểm soát truy cập là điều kiện thiết kế chứ không phải việc làm sau. Mục tiêu phục vụ là p99 dưới 150 ms; feature online mới dưới 30 giây; feature offline chấp nhận hoàn thiện sau 24 giờ. Hệ thống ban đầu xử lý khoảng 2.000 event/giây nhưng phải có đường tăng lên 20.000 event/giây.

## Sơ đồ kiến trúc

```text
App events ──> Kafka ──> Flink validation/dedup ──┬─> Online feature store ──> Risk API
Postgres CDC ───┘                │                │                              │
                                ├─> Quarantine + alert                         ├─> decision
                                │                │                              │
                                └─> Object storage Bronze ─> Silver/Iceberg ─> Gold PIT features
                                                          │                    │
Chargeback labels ────────────────────────────────────────┘                    ├─> training
                                                                               └─> drift/eval

Deletion ledger ──> Silver/Gold/online store/cache purge ──> audit receipt
Data catalog + contracts + lineage + RBAC bao quanh toàn bộ luồng
```

## 1. Batch hay streaming?

**Quyết định:** dùng streaming cho các feature ảnh hưởng quyết định giao dịch, nhưng dùng batch hằng ngày để đối soát, backfill và tạo tập train. Đây là kiến trúc kết hợp có hai đường xử lý nhưng dùng chung thư viện định nghĩa feature và cùng contract.

**Đánh đổi:** chỉ batch đơn giản, rẻ và dễ vận hành nhưng feature trễ vài giờ khiến model bỏ lỡ burst gian lận. Chỉ streaming giảm độ trễ nhưng việc sửa lịch sử, xử lý chargeback đến sau nhiều tuần và tái tạo training set rất khó. Hai đường làm tăng chi phí và nguy cơ lệch train/serve; tôi chấp nhận chi phí đó vì độ trễ là yêu cầu kinh doanh, đồng thời giảm parity drift bằng feature specification có version, test vàng và reconciliation hằng ngày. Khi online và offline lệch quá ngưỡng, hệ thống giữ model hiện tại và cảnh báo thay vì tự động phát hành phiên bản mới.

## 2. Event time, late data và failure semantics

**Quyết định:** mọi event có `event_id`, `event_time`, `ingest_time`, producer version và khoá nghiệp vụ. Streaming dedup theo `event_id`, dùng watermark được đo từ phân phối lateness theo từng nguồn; event sau watermark vẫn được lưu vào Bronze và đi qua một luồng correction. Gold batch overwrite các partition bị ảnh hưởng. Các sink dùng upsert theo khoá `(entity_id, feature_name, window_end)` và version tăng đơn điệu, nên replay không cộng lặp số liệu.

**Đánh đổi:** watermark dài tăng độ đúng nhưng giữ state lớn và trì hoãn kết quả; watermark ngắn rẻ hơn nhưng tạo nhiều correction. Tôi chọn 15 phút cho app online dựa trên p99 quan sát, không áp một con số cho CDC hay chargeback. Với thiết bị offline, correction có thể sửa feature phục vụ các giao dịch sau nhưng không được âm thầm viết lại quyết định đã ban hành. Quyết định cũ là ledger bất biến gồm model version, feature vector và rule version; đây là side effect không đảo ngược. Backfill ghi vào namespace/version mới, so checksum và metric rồi mới đổi alias, thay vì ghi đè trực tiếp production.

## 3. Hợp đồng dữ liệu và chất lượng

**Quyết định:** kiểm schema ở cửa ingest và kiểm semantic trước Silver. Contract bắt buộc ID, timestamp hợp lệ, currency ISO, amount không âm, trạng thái thuộc enum và quan hệ chuyển trạng thái hợp lệ. PII được token hoá xác định để join nhưng không lộ giá trị. Record sai vào quarantine kèm reason code, raw object pointer và schema version; không bị bỏ im lặng. Alert dựa trên tỷ lệ quarantine theo producer và loại lỗi, không chỉ số lượng tuyệt đối.

**Đánh đổi:** fail toàn batch bảo vệ chất lượng nhưng một producer drift có thể làm dừng hệ thống chống gian lận; cho qua tất cả giữ availability nhưng đầu độc feature. Tôi chọn xử lý từng record: lỗi cục bộ vào quarantine, còn vi phạm hệ thống như schema registry không đọc được hoặc tỷ lệ lỗi vượt 5% sẽ mở circuit breaker cho nguồn đó. On-call data platform nhận cảnh báo; owner của producer nhận mẫu lỗi đã khử PII. SLO gồm freshness, completeness, duplicate rate, late rate và reconciliation delta. Feature quan trọng có expectation riêng và dashboard theo version.

## 4. Train/serve parity và chống leakage

**Quyết định:** training set được point-in-time join tại `decision_time`, chỉ dùng dữ liệu có `event_time <= decision_time` và, với nhãn vận hành, phải xét cả `available_time`. Mỗi dòng train lưu feature-definition version và cutoff. Cùng logic cửa sổ được biên dịch cho Flink online và SQL offline; một tập fixture chạy qua cả hai engine và so checksum.

**Đánh đổi:** dùng bảng Gold mới nhất để join đơn giản và nhanh nhưng rò rỉ chargeback, KYC update hoặc trạng thái tài khoản xảy ra sau quyết định. Point-in-time join cần history/SCD2, tốn storage và compute, song đây là chi phí bắt buộc vì leakage tạo metric offline đẹp giả. Chargeback chỉ trở thành nhãn sau maturity window 30 ngày; giao dịch chưa đủ tuổi không bị coi là “không gian lận”. Tôi giữ một eval set theo thời gian, tách merchant/user group để giảm contamination, và không đưa trực tiếp quyết định của model cũ vào label của model mới.

## 5. Scale, chi phí và bối cảnh Việt Nam

**Quyết định:** Bronze dùng object storage partition theo ngày/giờ và compaction sang file cỡ 256–512 MB; streaming state partition theo `account_id` và đặt TTL theo cửa sổ feature dài nhất. Online store chỉ giữ feature cần phục vụ; lịch sử đầy đủ nằm ở lakehouse. Các tác vụ OCR/địa chỉ hoặc enrichment đắt tiền dùng cache theo hash đầu vào + model/version. Autoscaling dựa trên consumer lag nhưng có quota để tránh runaway cost.

**Đánh đổi:** giữ mọi feature online cho truy vấn linh hoạt làm RAM tăng nhanh; tính tại request giảm storage nhưng phá ngân sách 150 ms. Tôi materialize một tập feature đã được model sử dụng và xóa version cũ theo retention. Bottleneck đầu tiên ở 10× có thể là hot key của merchant lớn và state store, không phải object storage; cần salting có kiểm soát rồi aggregate hai tầng. Với Việt Nam, chuẩn hoá Unicode NFC nhưng giữ raw hash để audit, xử lý số điện thoại `+84/0`, múi giờ UTC trong kho và hiển thị Asia/Ho_Chi_Minh. Dữ liệu định danh được mã hoá, tách quyền, log truy cập và thực thi retention/deletion ledger theo chính sách bảo vệ dữ liệu cá nhân.

## Phương án bị loại

Tôi loại phương án “mỗi giao dịch gọi trực tiếp warehouse để tính toàn bộ feature mới nhất”. Nó hấp dẫn vì chỉ có một nguồn logic và tránh online store, nhưng không đạt p99 150 ms khi tải tăng, tạo truy vấn scan khó dự đoán, dễ dùng trạng thái tương lai khi backtest và biến sự cố warehouse thành sự cố thanh toán. Tôi cũng không chọn Lambda với hai codebase độc lập hoàn toàn; độ lệch logic sẽ lớn. Thiết kế được chọn vẫn có streaming và batch, nhưng buộc chúng chia sẻ feature specification, fixture parity và reconciliation. Nếu quy mô ban đầu nhỏ hơn dự kiến, tôi sẽ bắt đầu bằng Kafka + một stream processor và bảng lakehouse managed, chưa tự xây feature platform phức tạp; các interface/version ở trên cho phép thay backend khi tải thật chứng minh nhu cầu.

## Tiêu chí chấp nhận

Thiết kế đạt yêu cầu khi p99 Risk API dưới 150 ms, 99% feature online mới dưới 30 giây, duplicate không làm đổi checksum, backfill version mới khớp full recompute, sai lệch online/offline dưới ngưỡng định trước, và mọi feature của một quyết định có thể tái dựng từ ledger. Một diễn tập xoá phải chứng minh dữ liệu của subject biến mất khỏi Silver, Gold, online store và cache nhưng vẫn còn audit receipt không chứa PII. Những tiêu chí này biến các quyết định trên thành contract đo được thay vì chỉ là sơ đồ mong muốn.
