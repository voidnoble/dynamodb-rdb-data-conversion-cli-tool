package main

import (
	"fmt"
	"os"
)

// ExitWithError is exit with error
func ExitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	// fmt.Println(aerr.Error())
	os.Exit(1)
}
