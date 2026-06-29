package protocol

// ---------------------------------------------------------------------------
// NATS 异步消息协议
// ---------------------------------------------------------------------------
//
// 连接：configs 中 infra.nats，默认 nats://127.0.0.1:4222
// 编码：JSON UTF-8，字段名 snake_case，与 api/proto 中对应 message 一致。
// 发布方负责 fire-and-forget；订阅方需幂等处理。
// ---------------------------------------------------------------------------

const (
	// SubjectMailSend 邮件发送。
	// 载荷: pb.MailSendRequest { role_id, title, content, items }
	// 订阅: Mail 服务
	SubjectMailSend = "mail.send"

	// SubjectRankUpdate 排行榜分数更新。
	// 载荷: pb.RankUpdateRequest { zone_id, role_id, score, board }
	// 订阅: Rank 服务（同时写单区榜、全服榜、legacy 榜）
	SubjectRankUpdate = "rank.update"

	// SubjectActivityEvent 活动进度增量。
	// 载荷: { role_id, activity_id, delta }
	// 订阅: Activity 服务
	SubjectActivityEvent = "activity.event"
)

// ActivityEvent NATS activity.event 消息体。
type ActivityEvent struct {
	RoleID     int64 `json:"role_id"`     // 角色 ID
	ActivityID int32 `json:"activity_id"` // 活动 ID，如 1001=七日登录
	Delta      int32 `json:"delta"`       // 进度增量
}
