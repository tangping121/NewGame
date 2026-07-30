.PHONY: infra down build tidy proto fmt vet test test-race test-integration migrate docker-critical

proto:
	protoc --go_out=api/pb --go_opt=paths=source_relative --go-grpc_out=api/pb --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false -I api/proto api/proto/messages.proto

infra:
	docker compose up -d

down:
	docker compose down

tidy:
	go mod tidy

test:
	go test ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

test-race:
	go test -race ./...

test-integration:
	go test -tags=integration ./tests/integration/...

migrate:
	go run ./tools/migrate -dir ./deploy

docker-critical:
	docker build --build-arg SERVICE=login -t newgame/login:dev .
	docker build --build-arg SERVICE=gate -t newgame/gate:dev .
	docker build --build-arg SERVICE=game -t newgame/game:dev .
	docker build -f Dockerfile.migrate -t newgame/migrate:dev .

build:
	go build -o bin/login.exe ./services/login/cmd
	go build -o bin/gate.exe ./services/gate/cmd
	go build -o bin/game.exe ./services/game/cmd
	go build -o bin/match.exe ./services/match/cmd
	go build -o bin/battle.exe ./services/battle/cmd
	go build -o bin/social.exe ./services/social/cmd
	go build -o bin/mail.exe ./services/mail/cmd
	go build -o bin/rank.exe ./services/rank/cmd
	go build -o bin/activity.exe ./services/activity/cmd
	go build -o bin/pay.exe ./services/pay/cmd
	go build -o bin/gm.exe ./services/gm/cmd
	go build -o bin/robot.exe ./tools/robot
	go build -o bin/loadtest.exe ./tools/loadtest

run-login:
	go run ./services/login/cmd -config configs/login.yaml

run-gate:
	go run ./services/gate/cmd -config configs/gate.yaml

run-game:
	go run ./services/game/cmd -config configs/game.yaml

run-game-z2:
	go run ./services/game/cmd -config configs/game-zone2.yaml

run-gate-z2:
	go run ./services/gate/cmd -config configs/gate-zone2.yaml

run-match:
	go run ./services/match/cmd -config configs/match.yaml

run-battle:
	go run ./services/battle/cmd -config configs/battle.yaml

run-social:
	go run ./services/social/cmd -config configs/social.yaml

run-mail:
	go run ./services/mail/cmd -config configs/mail.yaml

run-rank:
	go run ./services/rank/cmd -config configs/rank.yaml

run-activity:
	go run ./services/activity/cmd -config configs/activity.yaml

run-pay:
	go run ./services/pay/cmd -config configs/pay.yaml

run-gm:
	go run ./services/gm/cmd -config configs/gm.yaml

loadtest:
	go run ./tools/loadtest
