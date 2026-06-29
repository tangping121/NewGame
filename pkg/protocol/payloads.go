// Package protocol 还定义 Gate↔Client TCP 帧内 JSON 载荷结构（见本文件）。
package protocol

// ---------------------------------------------------------------------------
// Gate TCP 协议总览
// ---------------------------------------------------------------------------
//
// 传输：TCP 长连接，大端序二进制帧。
// 帧格式：| uint16 len | uint16 cmd | uint16 act | payload(JSON UTF-8)... |
//   - len：cmd+act+payload 字节数（不含 len 自身 2 字节）
//   - payload：JSON 对象；错误时统一含 code、message
//
// 通用错误响应 JSON：{"code":1002,"message":"not logged in"}
// 业务码见 pkg/errors：0 成功，1001 参数，1002 未授权，1003 不存在，5000 内部错误
//
// ---------------------------------------------------------------------------
// CmdPing = 100
// ---------------------------------------------------------------------------

// PingRequest CmdPing 请求载荷（可为空 JSON {} 或空 body）。
type PingRequest struct{}

// PingResponse CmdPing 响应载荷。
type PingResponse struct {
	Pong bool `json:"pong"` // 固定 true
}

// ---------------------------------------------------------------------------
// CmdLogin = 1
// ---------------------------------------------------------------------------

// LoginGateRequest CmdLogin + ActLogin 请求（同 api/pb.EnterGateRequest）。
type LoginGateRequest struct {
	Token string `json:"token"` // Login HTTP 返回的会话 token
}

// LoginGateResponse CmdLogin + ActLogin 成功响应。
type LoginGateResponse struct {
	Code    int32  `json:"code"`    // 0=成功
	Message string `json:"message"` // 如 gate login ok
	ZoneID  int32  `json:"zone_id"` // 绑定区服
}

// ---------------------------------------------------------------------------
// CmdGame = 2（需先 CmdLogin 鉴权）
// ---------------------------------------------------------------------------

// SkillUpgradeRequest CmdGame + ActSkillUpgrade 请求。
type SkillUpgradeRequest struct {
	SkillID string `json:"skill_id"` // 技能 ID，空则默认 slash
}

// SkillUpgradeResponse CmdGame + ActSkillUpgrade 成功响应。
type SkillUpgradeResponse struct {
	SkillID string `json:"skill_id"`
	Level   int32  `json:"level"` // 升级后等级
}

// QuestAcceptRequest CmdGame + ActQuestAccept 请求。
type QuestAcceptRequest struct {
	QuestID string `json:"quest_id"` // 任务 ID，空则默认 main_1
}

// QuestAcceptResponse CmdGame + ActQuestAccept 响应。
type QuestAcceptResponse struct {
	Code    int32  `json:"code"`    // 0=成功；1001 业务失败
	Message string `json:"message"` // 失败原因
	QuestID string `json:"quest_id"`
}

// PlayerDataResponse CmdGame + ActPlayerData 响应（请求 body 可为空）。
type PlayerDataResponse struct {
	RoleID  int64            `json:"role_id"`
	Level   int32            `json:"level"`
	Gold    int64            `json:"gold"`
	Bag     map[string]int32 `json:"bag"`
	Skills  []map[string]any `json:"skills"` // skill_id + level 列表
	Quests  []map[string]any `json:"quests"`
	GuildID int64            `json:"guild_id"`
}

// SkillListResponse CmdGame + ActSkillList 响应。
type SkillListResponse struct {
	Skills []map[string]any `json:"skills"`
}

// QuestListResponse CmdGame + ActQuestList 响应。
type QuestListResponse struct {
	Quests []map[string]any `json:"quests"`
}

// ErrorResponse 通用 TCP/HTTP 业务错误 JSON。
type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
