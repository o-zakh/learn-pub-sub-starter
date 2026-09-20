package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	function := func(state routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(state)
	}
	return function
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	function := func(move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}
	return function
}
