package config

import (
	"github.com/valerybriz/triper"
	"github.com/valerybriz/triper/commandbus/async"
	"github.com/valerybriz/triper/eventbus/mosquitto"
	"github.com/valerybriz/triper/eventbus/nats"
	"github.com/valerybriz/triper/eventbus/rabbitmq"
	"github.com/valerybriz/triper/eventstore/badger"
	"github.com/valerybriz/triper/eventstore/postgresql"
)

// EventBus returns an triper.EventBus impl
type EventBus func() (triper.EventBus, error)

// EventStore returns an triper.EventStore impl
type EventStore func() (triper.EventStore, error)

// CommandBus returns an triper.CommandBus
type CommandBus func(register triper.CommandHandlerRegister) (triper.CommandBus, error)

// CommandConfig should connect internally commands with an aggregate
type CommandConfig func(repository *triper.Repository, register *triper.CommandRegister)

// commandHandler is the signature used by command handlers constructor
type commandHandler func(repository *triper.Repository, aggregate triper.AggregateHandler, bucket, subset string) triper.CommandHandler

// WireCommands acts as a wired between aggregate, register and commands
func WireCommands(aggregate triper.AggregateHandler, handler commandHandler, bucket, subset string, commands ...interface{}) CommandConfig {
	return func(repository *triper.Repository, register *triper.CommandRegister) {
		h := handler(repository, aggregate, bucket, subset)
		for _, command := range commands {
			register.Add(command, h)
		}
	}
}

// NewClient returns a command bus properly configured
func NewClient(es EventStore, eb EventBus, cb CommandBus, cmdConfigs ...CommandConfig) (triper.CommandBus, error) {
	store, err := es()
	if err != nil {
		return nil, err
	}

	bus, err := eb()
	if err != nil {
		return nil, err
	}

	repository := triper.NewRepository(store, bus)
	register := triper.NewCommandRegister()

	for _, conf := range cmdConfigs {
		conf(repository, register)
	}

	return cb(register)
}

// RabbitMq generates a RabbitMq implementation of EventBus
func RabbitMq(username, password, host string, port int) EventBus {
	return func() (triper.EventBus, error) {
		return rabbitmq.NewClient(username, password, host, port)
	}
}

// Nats generates a Nats implementation of EventBus
func Nats(urls string, useTLS bool) EventBus {
	return func() (triper.EventBus, error) {
		return nats.NewClient(urls, useTLS)
	}
}

// Mosquitto generates a Mosquitto implementation of EventBus
func Mosquitto(method string, host string, port int, clientID string) EventBus {
	return func() (triper.EventBus, error) {
		return mosquitto.NewClientWithPort(method, host, port, clientID)
	}
}

// Badger generates a BadgerDB implementation of EventStore
func Badger(dbDir string, reg triper.Register) EventStore {
	return func() (triper.EventStore, error) {
		return badger.NewClient(dbDir, reg)
	}
}

// Postgres is a implementation of Postgresql EventStore
func Postgres(dbInfo string, reg triper.Register) EventStore {
	return func() (triper.EventStore, error) {
		return postgresql.NewClient(dbInfo, reg)
	}
}

// AsyncCommandBus generates a CommandBus
func AsyncCommandBus(workers int) CommandBus {
	return func(register triper.CommandHandlerRegister) (triper.CommandBus, error) {
		return async.NewBus(register, workers), nil
	}
}
