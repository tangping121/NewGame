package main

import (
	"log"

	"newgame/pkg/app"
	srv "newgame/services/gate/internal"
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
