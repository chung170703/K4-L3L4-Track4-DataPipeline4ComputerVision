# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Ngô Đức Chung` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602985` |
| GitHub | `chung170703` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04868` |

> Cách tính trip: MSSV `2A202602985` có chữ `A` nên công thức của GUIDE (`int(MSSV) % 26`) không chạy được; tôi lấy phần số `202602985` rồi `% 26` làm chỉ số trong `list-trips`, ra `trip-04868`.

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

Thí nghiệm `break-gate` (`break_gate.json`): nới `max_speed_mps` từ 70 lên 1000 thì luật làm sạch không còn loại các dòng tốc độ vô lý, chúng lọt vào silver. `dbt test` chuyển từ `PASS=6 WARN=1 ERROR=0 SKIP=0` sang `PASS=5 WARN=1 ERROR=1 SKIP=0`: test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng** (đúng 3 dòng A + 3 dòng B từng nằm trong quarantine). Khôi phục luật thì về lại `PASS=6 WARN=1 ERROR=0 SKIP=0`.

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

`dataset_version` của tôi là `43aa8251b022` (12 ký tự). Theo mã `pipeline/publish.py`, nó là SHA-256 (lấy 12 ký tự đầu) của danh sách các dòng `frame_id|split` của mọi khung trong gold, sắp theo `frame_id`; không có ngày giờ hay số đếm nào trong đó, nên cùng danh sách khung và cùng split thì cùng version, còn chỉ cần một khung đổi split hoặc thêm bớt một khung thì version đổi. Thử trực tiếp: chạy `scripts/lab.sh step 4` rồi chạy lại `scripts/lab.sh cli export-dataset` cho cùng `dataset_version` `43aa8251b022` (`manifest_sha256` `378d4f54...`), dù hai lần chạy cách nhau vài phút và `created_at` khác nhau. Nếu dùng ngày giờ thì mỗi lần chạy lại sẽ ra version mới, và không thể dùng version để biết hai dataset có giống nhau không.

MSSV được trộn vào cách xếp trip ở `dbt/models/gold/gold_dataset_frames.sql`: trong mỗi drive, các trip được xếp theo `md5(trip_id || LAB_STUDENT_ID)`, rồi `round(n × 0.70)` trip đầu vào train, tới `round(n × 0.85)` vào val, còn lại vào test (với 13 trip mỗi drive thì 9 / 2 / 2). Cùng dữ liệu nhưng khác MSSV thì thứ tự md5 khác, trip nào vào split nào khác, nên một số dòng `frame_id|split` đổi và băm đổi. Tôi đã thử trực tiếp: đổi tạm `LAB_STUDENT_ID` từ `2A202602985` sang `2A202602986` (chỉ khác chữ số cuối), chạy lại `step 4` với cùng dữ liệu. Kết quả (`report/evidence/mssv_version.json`):

| MSSV | `dataset_version` | khung (test / train / val) | trip (test / train / val) |
|---|---|---|---|
| `2A202602985` (của tôi) | `43aa8251b022` | 152 / 715 / 153 | 4 / 18 / 4 |
| `2A202602986` (thử) | `cd76f86ca4a3` | 168 / 711 / 141 | 4 / 18 / 4 |

Tổng 1020 khung và số trip mỗi split không đổi (chia phân tầng theo drive), nhưng trip nào vào split nào đã đổi, nên số khung mỗi split và `dataset_version` khác. Chỉ đổi một chữ số của MSSV đã đủ làm đổi băm, đúng với việc version là băm nội dung. Sau đó tôi đặt lại MSSV của mình và chạy `step all --reset`: `dataset_version` về lại `43aa8251b022` và `verify` 27/27.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Phần `lineage` trong `manifest.json` của dataset `43aa8251b022` ghi: dataset được tạo từ các bảng Delta ở những **phiên bản** sau: `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.telemetry` v0, `silver.frames` v25, `silver.frames_flagged` v0, `silver.frame_telemetry` v0; và từ hai video có sha256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa` (drive_a), `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc` (drive_b), khớp với kết quả `shasum -a 256` trên file gốc.

Manifest còn ghi `rules`: bỏ khung đen, mờ, gần trùng và mất đồng bộ (`is_black, is_blurry, is_near_dup, not sync_ok`), chia theo trip phân tầng theo drive; kèm 1020 khung (train 715, val 153, test 152). Ba tháng sau, để tái tạo đúng dataset này cần: sha256 của video đầu vào, phiên bản từng bảng (nhờ time travel của Delta mở lại đúng v25 của `silver.frames`), luật làm sạch và chia, và MSSV dùng cho cách chia; cuối cùng so `dataset_version` và `manifest_sha256` để chắc chắn trùng. Số `silver.frames` v25 tương ứng 26 phiên bản (v0 đến v25) cho 26 trip: mỗi trip được ghi bằng một commit `overwrite` theo partition. `history` xác nhận phiên bản mới nhất là 25, thao tác đều là `WRITE` (CLI chỉ in 8 commit gần nhất nên 26 là suy ra từ số phiên bản, chưa đếm trực tiếp). Con số này lấy sau lần chạy `step all --reset` cuối; nếu có thêm lần chạy lại một trip (như thí nghiệm idempotent) thì số tăng lên.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Tôi đặt `LAB_FAIL_ONCE=trip-04868`, chạy `day09_reset` rồi `day09_video_pipeline` (run `2026-10-12 14:23:54 UTC`). Kết quả: **1 task đỏ**, là `silver_frames` với `map_index=13` (đúng trip của tôi; log ghi `RuntimeError: simulated crash while extracting trip-04868; run it again`), **25** task `silver_frames` còn lại xanh, và **7 task `upstream_failed`** (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`); 3 task bronze và `trip_commands` vẫn xanh. Cổng chất lượng đã chặn đúng: dữ liệu thiếu một trip không được đưa tiếp sang gold.

Tôi chỉ **clear task đỏ đó** (Clear task, không bật downstream). Chỉ `map_index=13` có `attempt=2`; 25 task kia chỉ có `attempt=1`, nên đúng **một** task được chạy lại, rồi 7 task phía sau tự chạy tiếp. Run chuyển xanh với 37/37 task `success`.

Dữ liệu **không nhân đôi**: `silver.frames` vẫn 1246 dòng với 1246 `frame_id` khác nhau, trip-04868 có 60 dòng. Lý do là mỗi trip được ghi bằng Delta `overwrite` theo partition, nên chạy lại ra đúng kết quả như lần đầu (giống thí nghiệm idempotent ở Câu 4). `scripts/lab.sh verify` vẫn **27/27** và `dataset_version` vẫn `43aa8251b022`, không đổi. Bằng chứng: `report/screenshots/fail_task_log.png` (log task đỏ thấy trip-04868) và `report/screenshots/fail_graph_green.png` (Graph xanh sau khi clear).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Số đo hiện tại: 2 drive (26 trip, 1246 khung hình) chạy hết DAG trong khoảng 1 phút, lake chiếm 79,1 MiB với 2346 object. Nếu 500 xe, mỗi xe tương đương một drive, thì khối lượng gấp khoảng 250 lần (ước lượng thô, giả định tuyến tính, và thực tế mỗi xe chạy lâu hơn nhiều so với đoạn cắt 5 phút này). Pipeline này theo đúng ý slide 22 (ELT): video gốc nằm yên trong object storage (`bronze/video/<drive>/raw.mp4`), chỉ dòng metadata (khung hình, telemetry) đi qua các bảng và dbt. Vì vậy phần nặng nhất khi mở rộng là xử lý video và lưu trữ, không phải bảng metadata. Ba thành phần tôi cho là vỡ trước:

1. **Giải mã video (FFmpeg) chạy trên một container Airflow.** Slide 24 nói decode video bằng CPU là nút cổ chai của xử lý khung hình (nặng hơn nhiều so với bước embedding trên GPU). Mỗi task của lab chạy `ffmpeg ... -vf fps=2` trên đoạn video của một trip: bộ lọc chỉ giữ 2 khung/giây, nhưng ffmpeg vẫn phải decode mọi khung 10 fps của đoạn đó trước khi bỏ bớt; `silver_frames` tạo một task cho mỗi trip, nên 500 xe sẽ ra hàng nghìn task tranh nhau CPU của một máy, và scheduler cùng metadata DB của Airflow cũng phải gánh số task đó. Thay bằng cách slide 24 gợi ý để giảm tải: giữ FFmpeg nhưng chạy trên nhiều worker, dùng NVIDIA DALI để decode trên GPU, và Ray Data để chạy song song; về phía Airflow thì dùng KubernetesExecutor hoặc Celery.
2. **Delta trên một node MinIO, ghi `overwrite` theo từng trip.** Video gốc vào thẳng storage (slide 22) nên dung lượng object storage tăng rất nhanh, còn mỗi lần ghi bảng là một commit: `silver.frames` sẽ có hàng chục nghìn commit và rất nhiều file nhỏ, nhiều worker ghi cùng lúc dễ xung đột commit, và 2346 object hiện tại sẽ thành hàng trăm nghìn mỗi ngày. Thay bằng object storage phân tán (S3/GCS), phân vùng theo ngày và xe, gộp file định kỳ (compaction), và giới hạn số writer trên một partition.
3. **dbt + DuckDB chạy trong một tiến trình, rồi nạp vào Postgres cho Metabase.** Dù chỉ metadata đi qua bảng (slide 22), telemetry 10 Hz của 500 xe vẫn lên hàng trăm triệu dòng mỗi ngày: DuckDB một máy sẽ hết RAM ở phép nối ASOF theo event time và ở các test chất lượng quét toàn bảng, còn Postgres hàng dòng không hợp với truy vấn phân tích trên lượng đó. Slide 23 dạy chọn engine theo độ trễ nên tôi tách theo nhu cầu: **dbt** (SQL theo lịch) vẫn hợp để dựng bảng khung hình nhưng chạy trên kho cột hoặc engine phân tán (BigQuery, Snowflake, Trino) với mô hình incremental theo ngày; **Spark** (batch, micro-batch) khi cần tính lại cả ngày trên lượng telemetry lớn; **Flink** (per-event) nếu cần cảnh báo telemetry trong vài giây. Pipeline hiện tại chưa có phần per-event: mất tín hiệu cảm biến chỉ được phát hiện sau khi chạy batch (test `max_gap_ms` ở mức `warn`), với 500 xe thì cảnh báo kiểu đó quá chậm.

Slide 12 nêu bốn chỗ vòng lặp dữ liệu thường hỏng; khi lên 500 xe tôi thấy cả bốn đều thành rủi ro thật: (1) **không đo được**: hiện có 27 kiểm tra, bảng quarantine và `dataset_version`, nhưng với đội xe cần theo dõi các số đo đó theo ngày và theo xe; (2) **nhãn đến trễ**: bài lab chưa có nhãn, nhưng khi nhãn về trễ thì phải chạy lại (backfill) từng partition mà không nhân đôi dữ liệu, đúng nhờ tính idempotent đã thấy ở Câu 4 và Câu 9; (3) **leakage**: tôi đã đo ở Câu 6 rằng chia theo khung hình làm 26/26 trip bị chia cắt, nên khi nhiều xe thì vẫn phải chia theo trip (có thể thêm theo xe hoặc theo ngày); (4) **không ai sở hữu**: các dòng trong `telemetry_rejects` và khung bị loại sẽ chồng lên nếu không ai xử lý. Như slide kết luận, mỗi bước (nạp, silver, gold, phục vụ) cần một sản phẩm đầu ra và một người chịu trách nhiệm.

Một rủi ro khác khi mở rộng sang nhiều loại xe là slide 24 cảnh báo: fps không cố định làm lệch chỉ số khung giữa nhãn và ảnh. Pipeline này ghép khung với telemetry theo event time (ASOF) chứ không theo chỉ số khung, nên đã tránh được lỗi đó; đội xe nhiều loại camera thì càng nên giữ cách này.

Ghi chú: đây là suy luận từ kiến trúc, số đo của bài lab và nội dung slide 12, 22, 23, 24; tôi chưa thử nghiệm ở quy mô lớn.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Thẻ **"Tỷ lệ khung bị loại theo trip"** (trên database `warehouse`, đã thêm vào dashboard `Day 9: video data pipeline`):

```sql
select drive_id, trip_id, split, frames, frames_kept,
       frames - frames_kept as khung_bi_loai,
       round(100.0 * (frames - frames_kept) / frames, 1) as ty_le_loai_pct
from marts.mart_trip_summary
order by ty_le_loai_pct desc
```

Kết quả (26 trip): `trip-04622` (drive_a, train) bị loại nhiều nhất, 34/60 khung (56,7%); tiếp theo là `trip-03904` (50,0%, 11/22) và `trip-02086` (46,7%, 28/60). Sáu trip bị loại nhiều nhất đều thuộc drive_a; trip drive_b đầu tiên là `trip-04871`, đứng thứ bảy với 28,3%.

Điều này khớp với bước 3: drive_a có 149 khung `near_duplicate` (xe chạy chậm, camera đứng hình 6 giây) so với 52 ở drive_b, nên drive_a mất nhiều khung hơn. Tuy nhiên đây chỉ là suy luận từ số liệu theo drive; mart `mart_trip_summary` không tách lý do loại theo từng trip, nên tôi chưa khẳng định được nguyên nhân của riêng `trip-04622`. Một trip mất hơn nửa số khung cũng cho thấy khi dùng dataset này cần kiểm tra trip đó trước (ví dụ khung đen/mờ/trùng tập trung ở đâu).

## Điểm cộng

### Thí nghiệm riêng: độ nhạy của `sync_tol_ms`
`sync_tol_ms` (mặc định 150) là độ trễ tối đa cho phép giữa một khung hình và dòng telemetry gần nhất trước nó (nối ASOF theo event time); quá ngưỡng thì khung bị gắn `sync_lost` và bị loại khỏi gold. Tôi chạy `dbt run --select silver_frame_telemetry gold_dataset_frames --vars '{sync_tol_ms: X}'` với 5 giá trị (không sửa file), rồi chạy lại với 150 để khôi phục: `verify` vẫn 27/27 và `dataset_version` vẫn `43aa8251b022`. Số liệu ở `report/evidence/bonus_sync_tol.json`.

| `sync_tol_ms` | khung mất đồng bộ | khung trong gold | test / train / val |
|---|---|---|---|
| 20 | 600 | 539 | 79 / 369 / 91 |
| 50 | 600 | 539 | 79 / 369 / 91 |
| **150 (mặc định)** | 14 | 1020 | 152 / 715 / 153 |
| 500 | 13 | 1021 | 152 / 715 / 154 |
| 3500 | 2 | 1032 | 152 / 721 / 159 |

Tác động không đều mà có một "vách đá" giữa 50 ms và 150 ms: gold tụt từ 1020 xuống 539 khung (mất 47%). Phân bố độ trễ giải thích điều này: trong 1246 khung, 646 khung lệch dưới 20 ms, **0 khung** nằm trong 20–50 ms, 586 khung nằm trong 50–150 ms, chỉ 12 khung lệch trên 150 ms (11 khung trên 500 ms, khớp với các khoảng mất tín hiệu 3 giây đã thấy ở Câu 3), và 2 khung không có telemetry nào trước nó. Tức là dữ liệu có hai cụm độ trễ, và 150 ms được đặt ngay trên cụm thứ hai nên giữ được gần hết. Nới lên 500 ms chỉ thêm 1 khung, còn 3500 ms thêm 12 khung nhưng đó chính là các khung có telemetry cũ tới vài giây (tốc độ, góc lái của vài giây trước), nên giá trị mặc định 150 là hợp lý: đổi vài khung lấy nhãn sai không có lợi.

### Câu hỏi Metabase thứ hai: tỷ lệ khung bị loại theo drive và split
Thẻ này đã được thêm vào dashboard `Day 9: video data pipeline` (ảnh: `report/screenshots/metabase_dashboard_2.png`). Câu SQL chạy trên database `warehouse` (Postgres), nên cột tốc độ phải ép `::numeric` trước khi `round`:

```sql
select drive_id, split, count(*) as so_trip, sum(frames) as khung,
       sum(frames - frames_kept) as khung_bi_loai,
       round(100.0 * sum(frames - frames_kept) / sum(frames), 1) as ty_le_loai_pct,
       round(avg(avg_speed_kmh)::numeric, 1) as toc_do_tb_kmh
from marts.mart_trip_summary
group by drive_id, split
order by drive_id, ty_le_loai_pct desc
```
Kết quả: ở drive_a, tập **test** bị loại 32,7% số khung (33/101) và chạy rất chậm, trung bình chỉ 13,4 km/h; tập train bị loại 28,4% (tốc độ 37,3 km/h), val 18,8% (51,0 km/h). Ở drive_b, tỷ lệ loại thấp hơn hẳn (val 17,6%, train 8,4%, test 5,6%) và xe chạy nhanh hơn (56–72 km/h). Nhìn chung drive_a (xe chậm hơn) mất nhiều khung hơn drive_b, phù hợp với giải thích ở Câu 5 (xe chậm sinh nhiều khung gần trùng), nhưng quan hệ không đều từng split (drive_b test chạy 56,2 km/h mà chỉ mất 5,6%, thấp hơn val chạy 59,3 km/h với 17,6%), nên chỉ có thể nói là xu hướng. Điều đáng lo là tập test của drive_a vừa mất gần một phần ba số khung vừa lệch về tốc độ thấp, nên nó không đại diện cho điều kiện lái bình thường; khi đánh giá mô hình nên xem riêng từng drive thay vì chỉ một con số test gộp.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

**Bất ngờ nhất:** thí nghiệm `break-gate`. Chỉ cần nới một con số (tốc độ tối đa từ 70 lên 1000 m/s) là 6 dòng vô lý lọt vào silver, và chính test `in_range` bắt được đúng 6 dòng đó; không có test thì lỗi đi thẳng xuống gold mà không ai biết. Một bất ngờ nữa là `trip-04622` bị loại tới 56,7% số khung.

**Còn chưa rõ:** `mart_trip_summary` không tách lý do loại khung theo từng trip, nên tôi chỉ suy ra được nguyên nhân từ số liệu theo drive (drive_a nhiều khung trùng vì xe chạy chậm và camera đứng hình), chưa kiểm chứng cho riêng `trip-04622`. Việc đổi MSSV làm `dataset_version` đổi thì tôi đã tự thử và đọc mã (Câu 7); điều tôi chưa kiểm chứng là `dataset_version` chỉ băm `frame_id` và `split`, nên nếu chỉ ảnh của một khung bị sửa mà id và split giữ nguyên thì version sẽ không đổi.

**Lỗi pipeline chưa bắt được:** tốc độ nằm trong khoảng 0–70 m/s nhưng nhảy vọt giữa hai mẫu liên tiếp (gia tốc phi thực tế) vẫn lọt qua `in_range`; tương tự, nếu độ lệch đồng hồ khai báo trong `sources.json` sai thì pipeline vẫn trừ theo con số đó mà không phát hiện. Ngoài ra, khi làm bài tập sập tôi lỡ chạy `day09_reset` trong lúc pipeline đang chạy: lake bị xoá giữa chừng và run hỏng vì lý do khác (`bronze.trips_raw does not exist`), không phải lỗi giả lập; pipeline chưa có cơ chế chặn chạy reset khi đang có run.
