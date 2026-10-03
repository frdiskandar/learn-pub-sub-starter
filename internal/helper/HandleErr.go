package helper

import (
	"fmt"
	"os"
)

func HandleError(msg string, err error, fatal bool){
	e := fmt.Errorf("Error: %v, %v", msg, err)
	fmt.Println(e.Error())
	if fatal{
		os.Exit(1)
	}
}