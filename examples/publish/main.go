package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pubb "github.com/pubbio/sdk-go"
)

func main() {
	client, err := pubb.NewClient(pubb.Config{AppID: os.Getenv("PUBB_APP_ID"), AppKey: os.Getenv("PUBB_APP_KEY"), AppSecret: os.Getenv("PUBB_APP_SECRET")})
	if err != nil {
		log.Fatal(err)
	}
	result, err := client.Trigger(context.Background(), []string{"updates"}, "message", map[string]string{"text": "Hello from Go"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result.PublicationID)
}
