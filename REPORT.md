# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602985` |
| GitHub | `<điền: tên tài khoản>` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Năm khác biệt tôi thấy giữa `raw_telemetry_a` (3021 dòng) và `raw_telemetry_b` (3234 dòng):
1. **Tên cột**: A dùng `ts_utc, speed_mps, heading_deg, lat/lon, alt_m, acc_x/acc_y, gas/brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude/longitude, altitude, ax/ay, throttle_pct/brake_pct, steering`.
2. **Đơn vị tốc độ**: A là m/s (dòng đầu 1.213), B là km/h (dòng đầu 55.11, khoảng 15,3 m/s).
3. **Kiểu thời gian**: A là chuỗi ISO UTC (`2025-10-07T08:00:00.014Z`), B là epoch mili giây (`1759933801509`).
4. **Thang bàn đạp**: A theo tỉ lệ 0–1 (`gas` 0.028), B theo phần trăm (`throttle_pct` 2.0).
5. **Đồng hồ**: `telemetry_clock_offset_ms` của B là 1500, của A là 0 (xem `bronze_video_files`).

Bronze chỉ ghi nguyên bản và chỉ thêm, nên mỗi nguồn một bảng và mọi cột để dạng chữ: không phải quyết định gộp hay ép kiểu khi chưa hiểu dữ liệu. Ép kiểu sớm thì ô trống hoặc giá trị lạ làm lần nạp lỗi hoặc bị đổi thầm lặng, và bản gốc mất. Giữ dạng chữ thì dòng nào cũng vào được, việc làm sạch để silver xử lý và khi luật đổi có thể chạy lại từ bronze. Chạy lại không nhân đôi: file được nhận diện bằng sha256 (không theo tên hay giờ), lần nạp thứ hai báo `already present`, và `history bronze.raw_telemetry_a` chỉ có một commit (`version 0`, `rows_added` 3021).

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Các staging model (`stg_telemetry_a/b.sql`) đưa B về schema của A: tên cột đổi theo A; tốc độ `spd_kmh / 3.6` ra m/s; bàn đạp `throttle_pct / 100` ra tỉ lệ 0–1; thời gian `(timestamp_ms - telemetry_clock_offset_ms)` rồi đổi sang timestamp UTC, với độ lệch 1500 ms lấy từ `bronze_video_files`.

Bằng chứng: thời điểm bắt đầu của B (`bat_dau`) là `2025-10-08 14:30:00.009`, khớp giờ bắt đầu video `14:30:00` (dòng thô đầu là `14:30:01.509`; `verify` xác nhận lệch dưới 0,5 s). Tốc độ tối đa sau chuẩn hoá là 71 km/h (A) và 100 km/h (B), hợp lý. Số dòng cân đối: 2968 + 53 = 3021 (A), 3181 + 53 = 3234 (B).

| drive | duplicate | speed_missing | speed_out_of_range | tổng |
|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

Dòng lỗi vào `silver.telemetry_rejects` (106 dòng) kèm `reject_reason` thay vì bị xoá, vì xoá thì không biết đã mất bao nhiêu, không đối soát được "sạch + loại = nhận vào", không điều tra được nguyên nhân (cảm biến hay nguồn), và nếu luật loại sai thì không còn dữ liệu để lấy lại. Cách ly giữ được dữ liệu sạch cho silver mà vẫn truy vết được.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Trong `schema.yml`, `max_gap_ms` (khoảng trống dữ liệu quá 1000 ms) đặt `severity: warn`, còn `in_range` (tốc độ 0–70 m/s) mặc định là `error`. Mất tín hiệu cảm biến là sự kiện có thật ngoài đời, không làm các dòng còn lại sai: tôi đếm được mỗi drive có 1 khoảng trống (lớn nhất 3073 ms ở A và 3124 ms ở B), nên chỉ cần báo để con người biết. Tốc độ vô lý là dữ liệu hỏng, đi tiếp sẽ làm hỏng mọi thứ phía sau (join, tập huấn luyện), nên phải dừng pipeline.

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 thì luật làm sạch không còn loại các dòng tốc độ vô lý, chúng lọt vào silver. `dbt test` chuyển từ `PASS=6 WARN=1 ERROR=0` sang `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** (đúng 3 dòng A + 3 dòng B từng nằm trong quarantine). Khôi phục luật thì về lại `PASS=6 WARN=1 ERROR=0`.

Vậy cổng chất lượng là lớp bảo vệ độc lập: kể cả khi bước làm sạch bị cấu hình sai, test vẫn chặn dữ liệu xấu trước khi các task phía sau chạy. Nó chỉ hiệu quả khi test kiểm tra ngưỡng riêng, không dùng cùng tham số với luật làm sạch.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
