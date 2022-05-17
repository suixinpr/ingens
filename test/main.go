package main

import (
	"fmt"
	"github/suixinpr/ingens"
)

func main() {
	ing, err := ingens.Open("./", nil)
	if err != nil {
		fmt.Println(err)
	}

	// SetNx a -> b
	err = ing.SetNx([]byte("a"), []byte("b"))
	if err != nil {
		fmt.Println(err)
	}

	// Get a
	v1, err := ing.Get([]byte("a"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(v1))

	// SetNx a -> b
	err = ing.SetNx([]byte("a"), []byte("b"))
	if err != nil {
		fmt.Println(err)
	}

	// SetNx b -> c
	err = ing.SetNx([]byte("b"), []byte("c"))
	if err != nil {
		fmt.Println(err)
	}

	// Get b
	v2, err := ing.Get([]byte("b"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(v2))

	ing.Close()
}
