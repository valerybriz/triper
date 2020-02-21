package postgresql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/valerybriz/triper"
	"reflect"
	"testing"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "pguser"
	password = "pass"
	dbname   = "pgdb"
)

func TestClientClose(t *testing.T) {
	type fields struct {
		connector *sql.DB
		reg       triper.Register
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				connector: tt.fields.connector,
				reg:       tt.fields.reg,
			}
			if err := c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientLoad(t *testing.T) {
	type fields struct {
		connector *sql.DB
		reg       triper.Register
	}
	type args struct {
		aggregateID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []triper.Event
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				connector: tt.fields.connector,
				reg:       tt.fields.reg,
			}
			got, err := c.Load(tt.args.aggregateID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Load() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientSafeSave(t *testing.T) {
	type fields struct {
		connector *sql.DB
		reg       triper.Register
	}
	type args struct {
		events  []triper.Event
		version int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				connector: tt.fields.connector,
				reg:       tt.fields.reg,
			}
			if err := c.SafeSave(tt.args.events, tt.args.version); (err != nil) != tt.wantErr {
				t.Errorf("SafeSave() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClientSave(t *testing.T) {
	type fields struct {
		connector *sql.DB
		reg       triper.Register
	}
	type args struct {
		events  []triper.Event
		version int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				connector: tt.fields.connector,
				reg:       tt.fields.reg,
			}
			if err := c.Save(tt.args.events, tt.args.version); (err != nil) != tt.wantErr {
				t.Errorf("Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewClient(t *testing.T) {
	type args struct {
		psqlInfo string
		reg      triper.Register
	}
	defaultPgInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	defaultRegister := triper.NewEventRegister()
	defaultClient, err := NewClient(defaultPgInfo, defaultRegister)
	if err != nil{
		panic(err)
	}
	tests := []struct {
		name    string
		args 	args
		expectedClient    *Client
		expectedErr bool
	}{
		{
			name: "algo",
			args: args{
				psqlInfo: defaultPgInfo,
				reg: defaultRegister,
			},
			expectedClient: defaultClient,
			expectedErr: false,

		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewClient(tt.args.psqlInfo, tt.args.reg)
			if (err != nil) != tt.expectedErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.expectedErr)
				return
			}
			assert.Equal(t, tt.expectedClient.reg, got.reg)
		})
	}
}

func TestDecode(t *testing.T) {
	type args struct {
		rawData json.RawMessage
		value   interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := decode(tt.args.rawData, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("decode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    json.RawMessage
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encode(tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encode() got = %v, want %v", got, tt.want)
			}
		})
	}
}