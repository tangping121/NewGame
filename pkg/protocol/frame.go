// Package protocol 定义 Gate 与游戏客户端之间的大端序（Big-Endian）二进制帧协议。
package protocol

import (
	"encoding/binary"
	"fmt"
)

// Gate→Game 内部 HTTP 转发的请求头名（避免 JSON 套 JSON 双重编码：
// cmd/act/role 走 header，原始 payload 作为 HTTP body 直传）。
const (
	HeaderCmd  = "X-NG-Cmd"
	HeaderAct  = "X-NG-Act"
	HeaderRole = "X-NG-Role"
)

// HeaderSize 帧头固定长度（字节）：cmd(2) + act(2) + 不含 len 字段本身。
// 完整线上帧布局：| uint16 len | uint16 cmd | uint16 act | payload... |
// 其中 len = cmd + act + payload 的总字节数（即 HeaderSize-2 + len(payload)）。
const HeaderSize = 6

// Frame 解码后的单帧消息。
type Frame struct {
	Cmd  uint16 // 命令字，如 CmdLogin、CmdGame
	Act  uint16 // 动作字，同一 Cmd 下的子操作
	Body []byte // 业务载荷，通常为 JSON 字符串
}

// Encode 将 Frame 编码为可写入 TCP 的字节切片。
//
// 参数:
//   - f: 待编码帧；Body 可为 nil
//
// 返回: 完整帧字节，前缀 2 字节为 big-endian 长度
func Encode(f Frame) []byte {
	n := HeaderSize + len(f.Body)
	buf := make([]byte, 2+n)
	binary.BigEndian.PutUint16(buf[0:2], uint16(n))
	binary.BigEndian.PutUint16(buf[2:4], f.Cmd)
	binary.BigEndian.PutUint16(buf[4:6], f.Act)
	copy(buf[6:], f.Body)
	return buf
}

// Decode 从已去掉外层 len 的帧体解析 Frame。
//
// 参数:
//   - buf: 至少包含 cmd+act+payload；即 Encode 输出去掉前 2 字节 len 后的部分
//
// 返回: 解析后的 Frame；buf 过短时返回 error
func Decode(buf []byte) (Frame, error) {
	if len(buf) < HeaderSize {
		return Frame{}, fmt.Errorf("frame too short")
	}
	f := Frame{
		Cmd:  binary.BigEndian.Uint16(buf[0:2]),
		Act:  binary.BigEndian.Uint16(buf[2:4]),
		Body: buf[4:],
	}
	return f, nil
}

// 客户端命令字（Cmd）：Gate dispatch 按 Cmd 路由。
const (
	CmdLogin uint16 = 1   // 登录鉴权；payload 见 LoginGateRequest
	CmdGame  uint16 = 2   // 游戏逻辑；需 authed；payload/响应见 payloads.go
	CmdPush  uint16 = 3   // 服务端主动推送（Gate→Client）；由跨分片通知触发
	CmdPing  uint16 = 100 // 心跳；响应 PingResponse
)

// CmdPush 下的动作字（Act）：标识推送类型。
const (
	ActPushSystem uint16 = 1 // 系统通知/公告
	ActPushChat   uint16 = 2 // 私聊/频道消息
	ActPushInvite uint16 = 3 // 组队/好友邀请
	ActPushMail   uint16 = 4 // 新邮件提醒
)

// 各 Cmd 下的动作字（Act）。
const (
	ActLogin        uint16 = 1 // CmdLogin：校验 token
	ActPing         uint16 = 1 // CmdPing：心跳
	ActPlayerData   uint16 = 2 // CmdGame：拉取玩家数据 → PlayerDataResponse
	ActSkillList    uint16 = 3 // CmdGame：技能列表 → SkillListResponse
	ActSkillUpgrade uint16 = 4 // CmdGame：升级技能；请求 SkillUpgradeRequest
	ActQuestList    uint16 = 5 // CmdGame：任务列表 → QuestListResponse
	ActQuestAccept  uint16 = 6 // CmdGame：接任务；请求 QuestAcceptRequest
)
