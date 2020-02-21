package postgresql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/valerybriz/triper"
)

// AggregateDB defines version and id of an aggregate
type AggregateDB struct {
	ID      string `json:"_id"`
	Version int `json:"version"`
	Events driver.Value `json:"events"`
}

// EventDB defines the structure of the events to be stored
type EventDB struct {
	ID            string `json:"_id"`
	Type          string `json:"type"`
	AggregateID   string `json:"aggregate_id"`
	AggregateType string `json:"aggregate_type"`
	CommandID     string `json:"command_id"`
	RawData       json.RawMessage `json:"raw_data"`
	Timestamp     time.Time `json:"timestamp"`
	Version       int `json:"version"`
}

// Client for access to badger
type Client struct {
	connector *sql.DB
	reg     triper.Register
}

var _ triper.EventStore = (*Client)(nil)



// NewClient generates a new client for access to badger using badgerhold
func NewClient(psqlInfo string, reg triper.Register) (*Client, error) {

	connector, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	cli := &Client{
		connector: connector,
		reg:     reg,
	}
	return cli, nil
}

// Close db connection
func (c *Client) Close() error {
	return c.connector.Close()
}

func (c *Client) save(events []triper.Event, version int, safe bool) error {
	if len(events) == 0 {
		return nil
	}
	var (
		id string
		currentVersion int
	)

	// Now that events are saved, aggregate version needs to be updated
	aggregateID := events[0].AggregateID
	aggregate := AggregateDB{
		ID:      aggregateID,
		Version: version + 1,
		Events: nil,
	}

	/*tx, err := c.connector.Begin()
	if err != nil {
		return err
	}
	*/
	//defer c.connector.Close()

	err := c.connector.QueryRow("SELECT _id,  FROM events WHERE _id = $1", aggregateID).Scan(&id)

	if version == 0 {
		// If it trows an error there are no previous records with the same id
		if err != nil {
			_, err = c.connector.Exec("INSERT INTO events (_id, version) VALUES($1, $2)", aggregate.ID, aggregate.Version)
			if err != nil {
				//tx.Rollback()
				return err
			}
		} else{
			return fmt.Errorf("postgresql: %s, aggregate already exists", aggregateID)
		}

	} else {
		if err != nil {
			return err
		}
		aggregate.Version = version + len(events)
		_, err = c.connector.Exec("UPDATE events SET version = $1 WHERE _id = $2", aggregate.Version, aggregate.ID)
		if err != nil {
			//tx.Rollback()
			return err
		}
	}
	query := `INSERT INTO eventdetails (_id, version, type, aggregate_id, aggregate_type, command_id, timestamp, raw_data) 
			  VALUES($1, $2, $3, $4, $5, $6, $7, $8)`
	for i, event := range events {
		// Encode the specific data of the event
		currentVersion = 1 + version + i
		raw, err := encode(event.Data)
		if err != nil {
			return err
		}
		_, err = c.connector.Exec(query, event.ID, currentVersion, event.Type, aggregate.ID,
			event.AggregateType, event.CommandID, time.Now(), raw)
		if err != nil {
			//tx.Rollback()
			return err
		}

	}

	/*err = tx.Commit()
	if err != nil {
		return err
	}*/

	return nil
}

// SafeSave store the events without check the current version
func (c *Client) SafeSave(events []triper.Event, version int) error {
	return c.save(events, version, true)
}

// Save the events ensuring the current version
func (c *Client) Save(events []triper.Event, version int) error {
	return c.save(events, version, false)
}

// Load the stored events for an AggregateID
func (c *Client) Load(aggregateID string) ([]triper.Event, error) {
	var (
		events   []triper.Event
		id string
		version int
		eventVersion int
		commandID string
		aggregateType string
		eventType string
		eventAggregateID string
		timestamp time.Time
		data json.RawMessage
	)

	tx, err := c.connector.Begin()
	if err != nil {
		return nil, err
	}
	//defer c.connector.Close()

	err = tx.QueryRow("SELECT version FROM events WHERE _id = $1", aggregateID).Scan(&version)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query("SELECT * FROM eventsDetails WHERE aggregate_id = $1", aggregateID)
	if err != nil {
		return nil, err
	}

	events =  make([]triper.Event, version)
	i := 0
	for rows.Next() {
		err = rows.Scan(&id, &eventVersion, &eventType, &eventAggregateID, &aggregateType, &commandID, &timestamp, &data)
		if err != nil {
			return events, err
		}

		dataType, err := c.reg.Get(eventType)
		if err != nil {
			return events, err
		}
		if data != nil{
			if err = decode(data, &dataType); err != nil {
				return events, err
			}
		}

		events[i] = triper.Event{
			AggregateID:   eventAggregateID,
			AggregateType: aggregateType,
			CommandID:     commandID,
			Version:       eventVersion,
			Type:          eventType,
			Data:          dataType,
		}
		i += 1

	}

	err = tx.Commit()
	if err != nil {
		return events, err
	}

	return events, nil
}

func encode(value interface{}) (json.RawMessage, error) {
	// Marshal event data if there is any.
	if value != nil {
		rawData, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return rawData, nil
	}

	return nil, errors.New("encode error null value found")
}

func decode(rawData json.RawMessage, value interface{}) error {
	if rawData != nil {
		err := json.Unmarshal(rawData, &value)
		if err != nil {
			return errors.New("scan could not unmarshal to interface{}")
		}

	} else{
		return errors.New("decode error, null value found")
	}
	return nil
}
