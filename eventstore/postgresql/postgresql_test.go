package postgresql

import (
	"database/sql"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/valerybriz/triper"
	"reflect"
	"testing"
)

const defaultPgInfo = "host=localhost port=5432 user=pguser "+
"password=pass dbname=pgdb sslmode=disable"

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

func TestClientSave(t *testing.T) {
	type fields struct {
		connector *sql.DB
		reg       triper.Register
	}
	type args struct {
		events  []triper.Event
		version int
	}
	type DepositPerformed struct {
		Amount int `json:"ammount"`
	}
	defaultRegister := triper.NewEventRegister()
	defaultClient, _ := NewClient(defaultPgInfo, defaultRegister)
	defaultData := DepositPerformed{100}
	defaultEvents := []triper.Event{
		{
			AggregateID:   "some_id",
			AggregateType: "some_aggregate_type",
			CommandID:     "some_command_id",
			Version:       1,
			Type:          "deposit_performed",
			Data:          defaultData,
		},
		{
			AggregateID:   "some_id1",
			AggregateType: "some_aggregate_type1",
			CommandID:     "some_command_id1",
			Version:       2,
			Type:          "deposit_performed",
			Data:          defaultData,
		},

	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		expectedPanic bool
		expectedErr bool
		errorText string
	}{
		{
			name: "save_event_ok",
			fields:  fields{
				defaultClient.connector,
				defaultRegister,
			},
			args:    args{
				defaultEvents,
				0,
			},
			expectedErr: false,
		},
		{
			name: "save_event_already_exists",
			fields:  fields{
				defaultClient.connector,
				defaultRegister,
			},
			args:    args{
				defaultEvents,
				0,
			},
			expectedErr: true,
			errorText: "postgresql: some_id, aggregate already exists",
		},
		{
			name: "save_event_nil_connector",
			fields:  fields{
				nil,
				defaultRegister,
			},
			args:    args{
				defaultEvents,
				0,
			},
			expectedPanic: true,

		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				connector: tt.fields.connector,
				reg:       tt.fields.reg,
			}

			defer func() {
				if r := recover(); r == nil && tt.expectedPanic{
					t.Errorf("The test: %s should have panicked!", tt.name)
				}
			}()
			func() {
				err := c.Save(tt.args.events, tt.args.version)

				if tt.expectedErr {
					assert.EqualError(t, err, tt.errorText)
				} else{
					assert.Equal(t, c.connector.Stats(), defaultClient.connector.Stats())
				}
			}()

		})
	}
}

func TestClientLoad(t *testing.T) {
	type DepositPerformed struct {
		Amount int `json:"ammount"`
	}
	defaultRegister := triper.NewEventRegister()
	defaultRegister.Set(DepositPerformed{})
	defaultClient, _ := NewClient(defaultPgInfo, defaultRegister)
	defaultData := DepositPerformed{100}
	defaultEvents := []triper.Event{
		{
			AggregateID:   "some_id",
			AggregateType: "some_aggregate_type",
			CommandID:     "some_command_id",
			Version:       1,
			Type:          "deposit_performed",
			Data:          &defaultData,
		},

	}

	tests := []struct {
		name    string
		dbClient *Client
		aggregateID string
		expectedPanic bool
		expectedErr bool
		errorText string
		expectedEvents []triper.Event
	}{
		{
			name: "load_event_ok",
			dbClient: defaultClient,
			aggregateID:   "some_id",
			expectedErr: false,
			expectedEvents: defaultEvents,
			expectedPanic: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil && tt.expectedPanic {
					t.Errorf("The test: %s should have panicked!", tt.name)
				}
			}()
			func() {
				got, err := tt.dbClient.Load(tt.aggregateID)
				if tt.expectedErr {
					assert.EqualError(t, err, tt.errorText)

				} else {
					if err != nil {
						t.Errorf("Load() got an unexpected error = %s", err)
						return
					}

					assert.Equal(t, tt.expectedEvents[0], got[0])
				}
			}()
		})
	}
}


func TestNewClient(t *testing.T) {
	type args struct {
		psqlInfo string
		reg      triper.Register
	}
	wrongPgInfo := "host=0.0.1.0 port=3232 user=erw "+
		"password=ggfd dbname=wrongdbname sslmode=disable"
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
		expectedPanic bool
		errorText string
	}{
		{
			name: "new_client_ok",
			args: args{
				psqlInfo: defaultPgInfo,
				reg: defaultRegister,
			},
			expectedClient: defaultClient,
			expectedErr: false,
			expectedPanic: false,

		},
		{
			name: "new_client_pginfo_wrong",
			args: args{
				psqlInfo:wrongPgInfo,
				reg: defaultRegister,
			},
			expectedClient: defaultClient,
			expectedErr: false,
			expectedPanic: true,

		},
		{
			name: "new_client_connector_wrong",
			args: args{
				psqlInfo: "",
				reg: defaultRegister,
			},
			expectedClient: defaultClient,
			expectedErr: true,
			expectedPanic: false,

		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			defer func() {
				if r := recover(); r == nil && tt.expectedPanic {
					t.Errorf("The test: %s should have panicked!", tt.name)
				}
			}()
			func() {
				got, err := NewClient(tt.args.psqlInfo, tt.args.reg)

				if tt.expectedErr {
					assert.EqualError(t, err, tt.errorText)
				} else {
					assert.Equal(t, tt.expectedClient.reg, got.reg)
					assert.Equal(t, tt.expectedClient.connector.Stats(), got.connector.Stats())
				}
			}()
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