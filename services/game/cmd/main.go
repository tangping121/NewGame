package main

import (
	"log"

	"bastion/pkg/app"
	srv "bastion/services/game/internal"
)

func main() {
	s, err := srv.New(app.MustConfigFlag())
	if err != nil {
		log.Fatal(err)
	}
	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}
