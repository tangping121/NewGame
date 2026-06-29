// Package zone 维护静态区服列表；运行时 Gate 地址由服务发现填充。
package zone

// Meta 区服（分片）元信息。
type Meta struct {
	ID   int32  `json:"id"`
	Name string `json:"name"`
}

// Catalog 静态区服目录，与 configs/*-zone2.yaml 等对应。
var Catalog = []Meta{
	{ID: 1, Name: "服务器1"},
	{ID: 2, Name: "服务器2"},
}

// Name 返回区服显示名，未知 ID 返回 "zone"。
func Name(id int32) string {
	for _, z := range Catalog {
		if z.ID == id {
			return z.Name
		}
	}
	return "zone"
}
