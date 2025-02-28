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

// Connect to the server, subscribe, and repond to requests

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
	"github.com/eclipse/paho.golang/paho"
)

const (
	serverURL = "tcp://mosquitto:1883" // server URL
	rTopic    = "rpc/request"          // topic to wait for requests
	clientID  = "mqtt_response"        // client id to connect with
	qos       = 1                      // qos to utilise when publishing
	keepAlive = 30                     // seconds between keepalive packets
	debug     = true                   // autopaho and paho debug output requested
)

var lastCount int
var lastCountMutex sync.Mutex

func handleRequest(ctx context.Context, received paho.PublishReceived) (bool, error) {
	fmt.Printf("\nhandleRequest: %v\n", received.Packet.Topic)
	if received.Packet.Properties != nil && received.Packet.Properties.CorrelationData != nil && received.Packet.Properties.ResponseTopic != "" {
		count, _ := strconv.Atoi(string(received.Packet.Payload))

		lastCountMutex.Lock()
		if (lastCount + 1) != count {
			fmt.Printf("handleRequest: received request: %s - *** unexpected respCount: %d != %d - message lost?\n", string(received.Packet.Payload), (lastCount + 1), count)
		} else {
			fmt.Printf("handleRequest: received request: %s\n", string(received.Packet.Payload))
		}
		lastCount = count
		lastCountMutex.Unlock()

		// Simulate up to 10 seconds processing time
		delay := time.Duration(count%10) * time.Second
		fmt.Printf("handleRequest: simulate %v processing time...", delay)
		time.Sleep(delay)
		resp := strconv.Itoa(count)
		fmt.Printf("done\n")

		_, err := received.Client.Publish(ctx, &paho.Publish{
			Properties: &paho.PublishProperties{
				CorrelationData: received.Packet.Properties.CorrelationData,
			},
			Topic:   received.Packet.Properties.ResponseTopic,
			Payload: []byte(resp),
		})
		if err != nil {
			fmt.Printf("handleRequest: failed to publish message: %s\n", err)
			panic(err)
		}
	}
	return true, nil
}

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
			fmt.Println("OnConnectionUp>: mqtt connection up")
			if _, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: rTopic, QoS: qos},
				},
			}); err != nil {
				fmt.Printf("OnConnectionUp: failed to subscribe (%s). This is likely to mean no messages will be received.", err)
				return
			}
			initialSubscriptionOnce.Do(func() { close(initialSubscriptionMade) })
			fmt.Println("OnConnectionUp: mqtt subscription made")
		},
		OnConnectError: func(err error) { fmt.Printf("OnConnectError: error whilst attempting connection: %s\n", err) },
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					if pr.Packet.Topic == rTopic {
						return handleRequest(ctx, pr)
					}
					return true, nil
				}},
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

	if debug {
		cliCfg.Debug = logger{prefix: "autoPaho"}
		cliCfg.PahoDebug = logger{prefix: "paho"}
	}

	cm, err := autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}

	// Messages will be handled through the callback so we really just need to wait until a shutdown
	// is requested
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Printf("main: signal caught - exiting\n")

	// We could cancel the context at this point but will call Disconnect instead (this waits for autopaho to shutdown)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = cm.Disconnect(ctx)

	fmt.Printf("main: shutdown complete\n")
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
