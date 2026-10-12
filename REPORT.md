# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602985` |
| GitHub | `<điền: tên tài khoản>` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `43aa8251b022`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `715 / 153 / 152`
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

Tôi chạy lại partition `trip-00003` (60 khung) hai lần. Cách của pipeline (Delta `overwrite` theo predicate `trip_id`): trước 1246 dòng, sau hai lần chạy lại vẫn 1246 dòng, dấu vân tay `cb6118dbb0b8` không đổi (`unchanged: true`). Cách sai (`append`, chạy hai lần): `append_variant_rows_after_two_runs` = 120 dòng cho một trip chỉ có 60 khung, tức nhân đôi (2x), mỗi lần chạy lại lại thêm dữ liệu trùng.

`overwrite` theo partition an toàn hơn xoá rồi ghi vì nó là **một commit nguyên tử** trong Delta log: người đọc thấy hoặc bảng cũ hoặc bảng mới, không có trạng thái nửa chừng. Nếu giữa chừng lỗi (như task sập ở bước 5), commit chưa được ghi nên bảng giữ nguyên dữ liệu cũ. Xoá rồi ghi là hai bước riêng: sập ở giữa thì trip đó biến mất khỏi bảng và người đọc thấy khoảng trống. Kết quả của overwrite chỉ phụ thuộc vào đầu vào, không phụ thuộc số lần chạy, nên chạy lại bao nhiêu lần cũng được.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 60 (10,0%) | 8 (1,2%) |
| 2 | 149 (24,8%) | 52 (8,1%) |
| 6 | 363 (60,3%) | 244 (37,9%) |

(tổng khung: A 602, B 644; trích từ `threshold.json`, các ngưỡng 1, 4, 8 cũng có trong file.)

**Đen và tối:** khung đen gần như không có thông tin (mất tín hiệu camera): A có 6, B có 4, đều bị loại. Khung tối (đường hầm, bóng râm, thiếu sáng) vẫn còn nội dung nhìn thấy được, nên chỉ bị gắn cờ và **giữ lại**: A có 4, B có 61. Tôi đồng ý, vì xe thật có lái trong chỗ tối và mô hình cần thấy điều kiện đó, còn cờ cho phép lọc lại sau này nếu cần; khung đen thì không dạy được gì.

**Chọn ngưỡng 2** (đang dùng). Ngưỡng 0 chỉ bắt khung giống hệt từng bit nên bỏ sót các khung gần như trùng, vốn khác vài bit do nén video và nhiễu (B chỉ 8 khung). Ngưỡng 6 loại 60,3% khung của A và 37,9% của B: nhiều khả năng đã loại cả khung khác nhau thật. Ngưỡng 2 loại 24,8% (A) và 8,1% (B), đủ bắt cảnh đứng yên mà vẫn giữ phần lớn dữ liệu.

**Phụ thuộc tốc độ xe:** xe chạy chậm hoặc dừng thì hai khung cách nhau 0,5 giây gần như y hệt, nên có nhiều khung trùng; xe chạy nhanh thì cảnh đổi giữa hai khung nên ít trùng. Vì vậy cùng một ngưỡng loại nhiều hơn ở A (xe chậm, cộng camera đứng hình 6 giây) so với B. Ngưỡng cố định có thể loại hơi quá tay lúc chạy chậm và quá nhẹ lúc chạy nhanh; muốn chính xác hơn có thể điều chỉnh ngưỡng theo tốc độ.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

`leakage.json` (1020 khung đáng dùng, 26 trip): chia **theo khung hình** (ngẫu nhiên 70/15/15) làm **26/26 trip** bị chia cắt, và cả 26 trip đều có khung ở cả train lẫn test. Chia **theo trip** (gold của pipeline): **0 trip** bị chia cắt.

Các khung liền nhau trong một trip gần như giống hệt (cùng cảnh, cùng ánh sáng, chỉ cách nhau 0,5 giây). Nếu một nửa nằm ở train và nửa kia ở test thì mô hình đã "thấy đề thi" lúc huấn luyện: nó chỉ cần nhớ cảnh chứ không cần khái quát hoá. Điểm trên test khi đó cao giả tạo, và khi gặp một chuyến xe mới ngoài thực tế thì kết quả tệ hơn nhiều. Chia theo trip buộc test là những cảnh mô hình chưa từng gặp.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` của tôi là `43aa8251b022` (12 ký tự). Nó là băm của nội dung dataset, không phải ngày giờ hay số đếm, nên cùng nội dung thì cùng version và nội dung đổi thì version đổi. Thử trực tiếp: chạy `scripts/lab.sh step 4` rồi chạy lại `scripts/lab.sh cli export-dataset` cho cùng `dataset_version` `43aa8251b022` (`manifest_sha256` `378d4f54...`), dù hai lần chạy cách nhau vài phút và `created_at` khác nhau. Nếu dùng ngày giờ thì mỗi lần chạy lại sẽ ra version mới, và không thể dùng version để biết hai dataset có giống nhau không.

MSSV được trộn vào cách xếp trip để chia train/val/test (manifest ghi `split: per trip, stratified by drive, md5 order`), nên mỗi MSSV cho một cách chia khác, tức nội dung dataset khác và băm khác. Tôi chưa thử đổi MSSV để quan sát; phần này dựa trên mô tả của README và manifest.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Phần `lineage` trong `manifest.json` của dataset `43aa8251b022` ghi: dataset được tạo từ các bảng Delta ở những **phiên bản** sau: `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.telemetry` v0, `silver.frames` v27, `silver.frames_flagged` v0, `silver.frame_telemetry` v0; và từ hai video có sha256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa` (drive_a), `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc` (drive_b), khớp với kết quả `shasum -a 256` trên file gốc.

Manifest còn ghi `rules`: bỏ khung đen, mờ, gần trùng và mất đồng bộ (`is_black, is_blurry, is_near_dup, not sync_ok`), chia theo trip phân tầng theo drive; kèm 1020 khung (train 715, val 153, test 152). Ba tháng sau, để tái tạo đúng dataset này cần: sha256 của video đầu vào, phiên bản từng bảng (nhờ time travel của Delta mở lại đúng v27 của `silver.frames`), luật làm sạch và chia, và MSSV dùng cho cách chia; cuối cùng so `dataset_version` và `manifest_sha256` để chắc chắn trùng. Số `silver.frames` v27 cao hơn số trip (26) vì mỗi lần ghi partition là một commit, kể cả các lần chạy lại của thí nghiệm idempotent (đây là suy luận của tôi từ số commit, chưa kiểm tra bằng `history`).

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
