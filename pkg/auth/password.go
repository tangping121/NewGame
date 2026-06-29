// Package auth 账号密码 bcrypt 哈希与校验。
package auth

import "golang.org/x/crypto/bcrypt"

// HashPassword 对明文密码生成 bcrypt 哈希，用于注册/改密入库。
//
// 参数:
//   - password: 明文密码
//
// 返回: 可存入 accounts.password_hash 的字符串
func HashPassword(password string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// CheckPassword 校验明文是否与库中哈希匹配。
//
// 参数:
//   - hash: 数据库存储的 bcrypt 哈希
//   - password: 用户输入的明文密码
func CheckPassword(hash, password string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}
