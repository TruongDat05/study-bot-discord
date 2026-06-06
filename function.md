# Chức năng bot

Tài liệu ngắn gọn về các chức năng chính và cách dùng bot học tập.

## Cách hoạt động nhanh

- Vào phòng học, bật Cam hoặc Stream trong 60 giây để được ở lại.
- Bot chỉ tính thời gian học và cộng coins khi bạn đang bật Cam hoặc Stream.
- Coins là tiền ảo trong server, không liên quan tiền thật.
- Thông báo tự động được gửi qua DM riêng; có thể bật/tắt bằng `/notify`.
- Hầu hết lệnh nên dùng bằng slash command `/...`.

## Theo dõi học tập

- `/studying`: xem ai đang học ngay lúc này.
- `/rank [member]`: xem ví, class, streak và thống kê nhanh.
- `/stats [member]`: xem thống kê học tập chi tiết.
- `/card [member]`: tạo ảnh profile card.
- `/leaderboard`: bảng xếp hạng học hôm nay.
- `/top_alltime`: bảng xếp hạng tổng thời gian học.
- `/setgoal <goal> [hours] [minutes]`: đặt mục tiêu học tập.
- `/remind <hour>`: đặt giờ nhắc học hằng ngày qua DM, nhập `-1` để tắt.

## Quest, huy hiệu, class

- `/quest`: xem nhiệm vụ hằng ngày.
- `/badges [member]`: xem huy hiệu đã đạt.
- `/roles`: xem danh sách vai trò theo money class.
- Bot tự cộng coins, cập nhật class và đồng bộ role khi bạn học đủ điều kiện.

## Pomodoro

- `/pomodoro start [work] [break] [rounds]`: bắt đầu Pomodoro cá nhân.
- `/pomodoro stop`: dừng phiên đang chạy.
- `/pomodoro status`: xem tiến độ phiên hiện tại.
- `/pomodoro preset [work] [break] [rounds]`: lưu cấu hình yêu thích.
- `/pomodoro stats`: xem lịch sử Pomodoro.
- `/pomodoro create <name> [work] [break] [rounds]`: tạo phòng Pomodoro nhóm.
- `/pomodoro join <name>`: tham gia phòng Pomodoro nhóm.
- `/pomodoro leave`: rời phòng nhóm.
- `/pomodoro list`: xem danh sách phòng nhóm.

## Economy coins

- `/balance [member]`: xem balance, total earned, class, debt, credit score.
- `/pay <member> <amount>`: chuyển coins ảo cho người khác.
- `/transactions [limit]`: xem lịch sử giao dịch.
- `/economy leaderboard`: top người có total earned cao nhất.
- `/economy adjust <member> <amount> <reason>`: admin điều chỉnh balance.

## Loan/vay coins

- `/loan borrow <amount>`: vay coins từ bot.
- `/loan repay <amount>`: trả nợ.
- `/loan status`: xem nợ, khoản vay, offer và credit score.
- `/loan offer <member> <amount> <interest_percent> <days>`: tạo lời mời cho vay.
- `/loan accept <loan_id>`: chấp nhận lời mời vay.
- `/loan cancel <loan_id>`: hủy offer đang pending của bạn.
- `/loan history`: xem lịch sử vay/trả gần đây.

## Bảng điều khiển phòng

- `/room_panel`: tạo nút `Bảng điều khiển` cho phòng học.
- Bấm `Bảng điều khiển` để mở dashboard riêng tư.
- Nhóm Room controls: khóa phòng, mở phòng, đổi tên, xóa phòng.
- Nhóm Study controls: Pomodoro start, stop, status.
- Nhóm Economy controls: balance, borrow, repay, lend, loan status.

## Phòng học tạm

- Khi vào kênh tạo phòng, bot tạo phòng học riêng cho bạn.
- Phòng tạm tự xóa khi không còn thành viên thật ở lại.
- Vẫn cần bật Cam hoặc Stream để được tính giờ và tránh bị kick.

## Thông báo riêng

- `/notify on`: bật thông báo DM riêng.
- `/notify off`: tắt thông báo DM tự động.
- `/notify status`: xem trạng thái thông báo.
- Thông báo quan trọng gồm: chào mừng, nhắc bật Cam/Stream, cảnh báo kick, cảm ơn bật Cam/Stream, tổng kết phiên học, lên class, milestone học tập, milestone economy, loan.
- Nếu tắt thông báo, bot vẫn trả lời khi bạn dùng lệnh hoặc bấm nút.

## Báo cáo tuần

- `/weekly preview`: xem trước báo cáo tuần.
- `/weekly on`: bật báo cáo tuần tự động.
- `/weekly off`: tắt báo cáo tuần tự động.
- `/weekly status`: xem trạng thái báo cáo.
- `/weekly leaderboard`: top học nhiều nhất tuần này.
- `/weekly compare`: so sánh tuần này với tuần trước.
- `/weekly send [target]`: admin gửi báo cáo tuần ngay.

## AI học tập

- `/ask <question>`: hỏi AI học tập.
- Có thể tag bot kèm câu hỏi trong server để nhận câu trả lời ngắn gọn.

## Dashboard và báo cáo server

- Web dashboard chạy theo cấu hình `DASHBOARD_PORT`.
- Bot có live message, báo cáo ngày và bảng tổng kết ngày.
- Dữ liệu chính lưu trong `study_data.json`.

## Lệnh admin

- `/syncroles`: đồng bộ role theo class.
- `/report`: gửi báo cáo ngày ngay.
- `/dailyboard [date]`: gửi bảng tổng kết ngày.
- `/updatelive`: cập nhật live message.
- `/backup`: backup dữ liệu ngay.
- `/economy adjust`: điều chỉnh balance và ghi transaction.
- `/weekly send`: gửi báo cáo tuần thủ công.

## Lệnh prefix cũ

- `!stats`: xem thống kê.
- `!leaderboard`, `!lb`, `!top`: xem bảng xếp hạng.
- `!quest`: xem nhiệm vụ.
- `!badges`: xem huy hiệu.
- `!rank`: xem rank.
- `!sync`: admin đồng bộ role.
- `!report`: admin gửi báo cáo.
