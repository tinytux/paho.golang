/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v2.0
 *  and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 *  and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.php.
 *
 *  SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
 */

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/extensions/rpc"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/log"
)

const (
	serverURL = "tcp://mosquitto:1883" // server URL
	rTopic    = "rpc/request"          // topic to publish on
	clientID  = "mqtt_request"         // client id to connect with
	qos       = 1                      // qos to utilise when publishing
	keepAlive = 30                     // seconds between keepalive packets
	debug     = true                   // autopaho and paho debug output requested
)

func doRequest(ctx context.Context, cm *autopaho.ConnectionManager, router *paho.StandardRouter, req string) (int, error) {
	// if the request can not be processed withing 5 seconds,
	// we want to cancel it and continue with the next request
	reqTimeout := time.Duration(5 * time.Second)
	reqCtx, cancel := context.WithTimeout(ctx, reqTimeout)
	defer func() {
		fmt.Printf("doRequest: cancel()\n")
		cancel()
	}()

	fmt.Printf("doRequest: rpc.NewHandler() with reqTimeout = %v\n", reqTimeout)
	h, err := rpc.NewHandler(reqCtx, rpc.HandlerOpts{
		Conn:             cm,
		Router:           router,
		ResponseTopicFmt: "%s/responses",
		ClientID:         clientID,
	})
	if err != nil {
		fmt.Printf("doRequest: rpc.NewHandler() returns err: %v\n", err)
		return 0, err
	}

	fmt.Printf("doRequest: h.Request() with reqTimeout = %v: %s\n", reqTimeout, req)
	resp, err := h.Request(reqCtx, &paho.Publish{
		Topic:   rTopic,
		Payload: []byte(req),
	})

	if err != nil {
		fmt.Printf("doRequest: h.Request() returns err: %v\n", err)
		return 0, err
	}

	return strconv.Atoi(string(resp.Payload))
}

func requestLoop(ctx context.Context, cm *autopaho.ConnectionManager, router *paho.StandardRouter) {
	var count int
	for {
		fmt.Printf("\n")

		select {
		case <-ctx.Done():
			fmt.Printf("requestLoop: ctx.Done(), exit loop\n")
			return
		default:
			// AwaitConnection will return immediately if connection is up; adding this call stops publication whilst
			// connection is unavailable.
			fmt.Printf("requestLoop: cm.AwaitConnection()\n")
			err := cm.AwaitConnection(ctx)
			if err != nil { // Should only happen when context is cancelled
				fmt.Printf("requestLoop: cm.AwaitConnection() returns err: %s\n", err)
				return
			}

			count++
			req := strconv.Itoa(count)

			fmt.Printf("requestLoop: doRequest()\n")
			respCount, err := doRequest(ctx, cm, router, req)
			if err != nil {
				continue
			}

			if count != respCount {
				fmt.Printf("requestLoop: received response:*** unexpected requestCount: %d != %d - message lost?\n", count, respCount)
			} else {
				fmt.Printf("requestLoop: received response: %d\n", respCount)
			}

		}
	}
}

// Connect to the server and publish requests periodically
func main() {
	server, err := url.Parse(serverURL)
	if err != nil {
		panic("main: invalid MQTT broker URL")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initialSubscriptionMade := make(chan struct{}) // Closed when subscription made (otherwise we might send request before subscription in place)
	var initialSubscriptionOnce sync.Once          // We only want to close the above once!

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{server},
		KeepAlive:                     keepAlive,
		CleanStartOnInitialConnection: true, // the default is false
		SessionExpiryInterval:         20,   // Session remains live 20 seconds after disconnect
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			fmt.Println("OnConnectionUp: mqtt connection up")
			if _, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: fmt.Sprintf("%s/responses", clientID), QoS: qos},
				},
			}); err != nil {
				fmt.Printf("OnConnectionUp: response handler failed to subscribe: %v\n", err)
				return
			}
			initialSubscriptionOnce.Do(func() { close(initialSubscriptionMade) })
			fmt.Println("OnConnectionUp: mqtt subscription made")
		},
		OnConnectError: func(err error) { fmt.Printf("OnConnectError: error whilst attempting connection: %s\n", err) },
		Debug:          log.NOOPLogger{},
		ClientConfig: paho.ClientConfig{
			ClientID:      clientID,
			OnClientError: func(err error) { fmt.Printf("OnClientError: client error: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					fmt.Printf("OnServerDisconnect: server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					fmt.Printf("OnServerDisconnect: server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}
	router := paho.NewStandardRouter()
	cliCfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
		func(p paho.PublishReceived) (bool, error) {
			router.Route(p.Packet.Packet())
			return false, nil
		}}

	if debug {
		cliCfg.Debug = logger{prefix: "autoPaho"}
		cliCfg.PahoDebug = logger{prefix: "paho"}
	}

	// Connect to the server - this will return immediately after initiating the connection process
	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	// Start off a goRoutine that executes requests
	wg.Add(1)
	go func() {
		defer wg.Done()
		go requestLoop(ctx, cm, router)
	}()

	// Wait for a signal before exiting
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	select {
	case <-sig:
		fmt.Printf("main: signal caught - exiting\n")
	case <-ctx.Done():
		fmt.Printf("main: ctx.Done()\n")
	}

	fmt.Printf("main: exiting\n")
	cancel()

	wg.Wait()
	fmt.Printf("main: shutdown complete")
}

// logger implements the paho.Logger interface
type logger struct {
	prefix string
}

// Println is the library provided NOOPLogger's
// implementation of the required interface function()
func (l logger) Println(v ...interface{}) {
	fmt.Println(append([]interface{}{l.prefix + ":"}, v...)...)
}

// Printf is the library provided NOOPLogger's
// implementation of the required interface function(){}
func (l logger) Printf(format string, v ...interface{}) {
	if len(format) > 0 && format[len(format)-1] != '\n' {
		format = format + "\n" // some log calls in paho do not add \n
	}
	fmt.Printf(l.prefix+":"+format, v...)
}
