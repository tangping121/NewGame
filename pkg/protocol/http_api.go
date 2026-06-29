package protocol

// ---------------------------------------------------------------------------
// HTTP/JSON 协议总览
// ---------------------------------------------------------------------------
//
// 除 Gate（TCP）外，各微服务暴露 REST 风格 JSON API。
// 通用约定：
//   - Content-Type: application/json
//   - 业务码 code：0 成功；非 0 见 pkg/errors
//   - 健康检查：GET /health => "ok"
//   - 指标：GET /metrics（JSON）、GET /metrics/prometheus
//
// 默认端口（区服 1）：
//   Login :8080  Gate :9000/:8080(health)  Game :9100  Match :9200
//   Battle :9300  Social :9400  Mail :9500  Rank :9600
//   Activity :9800  Pay :9700  GM :9900
//
// 区服 2 Game :9110，Gate TCP :9010
// ---------------------------------------------------------------------------

// ===================== Login (:8080) =====================

// RouteLogin POST /api/login
// 请求: pb.LoginRequest { username, password, zone_id }
// 响应: pb.LoginResponse { code, token, role_id, gate_addr, message }
const RouteLogin = "/api/login"

// RouteEnter POST /api/enter — 校验 token（不连 Gate 时可用）
// 请求: pb.EnterGateRequest { token }
// 响应: { code, role_id, zone_id }
const RouteEnter = "/api/enter"

// RouteZones GET /api/zones — 区服列表与服务发现 Gate 地址
// 响应: { code, zones: [{ id, name, gate_addr?, online }] }
const RouteZones = "/api/zones"

// ===================== Match (:9200) =====================

// RouteMatchJoin POST /api/match/join
// 请求: pb.MatchRequest { role_id, mode, zone_id }
// 响应: pb.MatchResponse { code, match_id, room_id }
const RouteMatchJoin = "/api/match/join"

// RouteMatchRoom GET /api/match/room?match_id=
// 响应: { code, members: [role_id,...] }
const RouteMatchRoom = "/api/match/room"

// RouteMatchQueue GET /api/match/queue — 当前等待人数
// 响应: { code, cross_zone, waiting }
const RouteMatchQueue = "/api/match/queue"

// ===================== Battle (:9300) =====================

// RouteBattleRoomCreate POST /api/battle/room/create
// 请求: { members: [role_id,...] } 至少 2 人
// 响应: { code, room_id }
const RouteBattleRoomCreate = "/api/battle/room/create"

// RouteBattleRoom GET /api/battle/room?room_id=
// 响应: { code, room_id, members, results }
const RouteBattleRoom = "/api/battle/room"

// RouteBattleSettle POST /api/battle/settle
// 请求: pb.BattleResultRequest { room_id, role_id, win, score }
// 响应: pb.BattleResultResponse { code }
const RouteBattleSettle = "/api/battle/settle"

// ===================== Rank (:9600) =====================

// RouteRankUpdate POST /api/rank/update
// 请求: pb.RankUpdateRequest { zone_id, role_id, score, board }
// 响应: { code: 0 }
const RouteRankUpdate = "/api/rank/update"

// RouteRankTop GET /api/rank/top?board=dungeon&zone_id=1
// 响应: { code, zone_id, list: [{ Member, Score }] }
const RouteRankTop = "/api/rank/top"

// RouteRankGlobalTop GET /api/rank/global/top?board=dungeon
// 响应: { code, list }
const RouteRankGlobalTop = "/api/rank/global/top"

// ===================== Mail (:9500) =====================

// RouteMailSend POST /api/mail/send
// 请求: pb.MailSendRequest { role_id, title, content, items }
// 响应: { code: 0 }
const RouteMailSend = "/api/mail/send"

// RouteMailList GET /api/mail/list?role_id=
// 响应: { code, mails: [...] }
const RouteMailList = "/api/mail/list"

// RouteMailClaim POST /api/mail/claim
// 请求: { role_id, mail_id }
// 响应: { code, items } 或 code 1002 已领
const RouteMailClaim = "/api/mail/claim"

// RouteMailClaimAll POST /api/mail/claim-all
// 请求: { role_id }
// 响应: { code, claimed, failed }
const RouteMailClaimAll = "/api/mail/claim-all"

// RouteMailRead POST /api/mail/read — 仅标记已读
// 请求: { role_id, mail_id }
const RouteMailRead = "/api/mail/read"

// RouteMailReadAll POST /api/mail/read-all
// 请求: { role_id }
// 响应: { code, read }
const RouteMailReadAll = "/api/mail/read-all"

// RouteMailUnread GET /api/mail/unread?role_id=
// 响应: { code, unread, unclaimed }
const RouteMailUnread = "/api/mail/unread"

// ===================== Pay (:9700) =====================

// RoutePayOrderCreate POST /api/pay/order/create
// 请求: { role_id, product_id, amount }
// 响应: { code, order_id }
const RoutePayOrderCreate = "/api/pay/order/create"

// RoutePayNotify POST /api/pay/notify — 支付平台回调
// 请求: pb.PayNotifyRequest
// 响应: { code, message? }
const RoutePayNotify = "/api/pay/notify"

// RoutePayRetry POST /api/pay/retry — 补发未发货已付订单
// 响应: { code, success, failed }
const RoutePayRetry = "/api/pay/retry"

// RoutePayReconcile GET /api/pay/reconcile — 对账汇总
// 响应: { code, summary: { total, pending, paid, ... } }
const RoutePayReconcile = "/api/pay/reconcile"

// ===================== Social (:9400) =====================

// RouteFriendAdd POST /api/social/friend/add
// 请求: { role_id, friend_id, zone_id }
const RouteFriendAdd = "/api/social/friend/add"

// RouteFriendList GET /api/social/friend/list?role_id=
const RouteFriendList = "/api/social/friend/list"

// RouteChatSend POST /api/social/chat/send?channel=
// 请求: { role_id, zone_id, channel, text }
const RouteChatSend = "/api/social/chat/send"

// RouteChatList GET /api/social/chat/list?channel=&limit=
const RouteChatList = "/api/social/chat/list"

// RouteGlobalChatSend POST /api/social/chat/global/send
const RouteGlobalChatSend = "/api/social/chat/global/send"

// RouteGlobalChatList GET /api/social/chat/global/list
const RouteGlobalChatList = "/api/social/chat/global/list"

// ===================== Activity (:9800) =====================

// RouteActivityList GET /api/activity/list
// 响应: { code, activities: { id: { name, status, target } } }
const RouteActivityList = "/api/activity/list"

// RouteActivityProgress GET /api/activity/progress?role_id=&activity_id=
// 响应: { code, progress, claimed }
const RouteActivityProgress = "/api/activity/progress"

// RouteActivityClaim POST /api/activity/claim
// 请求: { role_id, activity_id }
// 响应: { code, claimed, rewards }
const RouteActivityClaim = "/api/activity/claim"

// ===================== Game 内部 (:9100 / :9110) =====================

// RouteGamePlayerMsg POST /internal/player/msg — Gate 转发 CmdGame
// 请求: { cmd, act, body, role_id }
// 响应: 与 TCP CmdGame 各 Act 响应相同（JSON 字节）
const RouteGamePlayerMsg = "/internal/player/msg"

// RouteGameDungeonPass POST /internal/player/dungeon/pass
// 请求: { role_id, dungeon_id }
// 响应: { code, level, gold }
const RouteGameDungeonPass = "/internal/player/dungeon/pass"

// RouteGameGrant POST /internal/player/grant
// 请求: { role_id, gold, items, items_raw, source }
// 响应: { code, gold, bag }
const RouteGameGrant = "/internal/player/grant"

// RouteGuildJoin POST /internal/guild/join
// 请求: { role_id, guild_id }
const RouteGuildJoin = "/internal/guild/join"

// RouteGuildInfo GET /internal/guild/info
// 响应: { code, guild_id, name, members }
const RouteGuildInfo = "/internal/guild/info"

// RouteWorldBossState GET /internal/worldboss/state
// 响应: { code, boss: { hp, max_hp, top_damage } }
const RouteWorldBossState = "/internal/worldboss/state"

// RouteWorldBossAttack POST /internal/worldboss/attack
// 请求: { role_id, damage }
const RouteWorldBossAttack = "/internal/worldboss/attack"

// RouteWorldBossReset POST /internal/worldboss/reset
const RouteWorldBossReset = "/internal/worldboss/reset"

// RouteGuildWarState GET /internal/guildwar/state
const RouteGuildWarState = "/internal/guildwar/state"

// RouteGuildWarAttack POST /internal/guildwar/attack
// 请求: { guild_id, damage }
const RouteGuildWarAttack = "/internal/guildwar/attack"

// RouteGuildWarReset POST /internal/guildwar/reset
const RouteGuildWarReset = "/internal/guildwar/reset"

// RouteAuctionList GET /internal/auction/list
// 响应: { code, listings }
const RouteAuctionList = "/internal/auction/list"

// RouteAuctionCreate POST /internal/auction/create
// 请求: { role_id, item_id, qty, price }
const RouteAuctionCreate = "/internal/auction/create"

// RouteAuctionBuy POST /internal/auction/buy
// 请求: { role_id, listing_id }
const RouteAuctionBuy = "/internal/auction/buy"

// ===================== GM (:9900) =====================

// RouteGMServices GET /api/gm/services?zone_id=
const RouteGMServices = "/api/gm/services"

// RouteGMGrant POST /api/gm/grant?zone_id= — 代理 Game grant
const RouteGMGrant = "/api/gm/grant"

// RouteGMMailSend POST /api/gm/mail/send
const RouteGMMailSend = "/api/gm/mail/send"

// RouteGMDungeonPass POST /api/gm/dungeon/pass?zone_id=
const RouteGMDungeonPass = "/api/gm/dungeon/pass"

// RouteGMPayReconcile GET /api/gm/pay/reconcile
const RouteGMPayReconcile = "/api/gm/pay/reconcile"

// RouteGMGuildWarReset POST /api/gm/season/guildwar/reset?zone_id=
const RouteGMGuildWarReset = "/api/gm/season/guildwar/reset"

// RouteGMWorldBossReset POST /api/gm/season/worldboss/reset?zone_id=
const RouteGMWorldBossReset = "/api/gm/season/worldboss/reset"
