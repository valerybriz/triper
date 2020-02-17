package postgresql

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/valerybriz/triper"
)

// AggregateDB defines version and id of an aggregate
type AggregateDB struct {
	ID      string
	Version int
	Events driver.Value
}

// EventDB defines the structure of the events to be stored
type EventDB struct {
	ID            string
	Type          string
	AggregateID   string
	AggregateType string
	CommandID     string
	RawData       driver.Value
	Timestamp     time.Time
	Version       int
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

	err := c.connector.Ping()
	if err != nil {
		panic(err)
	}

	eventsDB := make([]EventDB, len(events))
	aggregateID := events[0].AggregateID

	for i, event := range events {
		raw, err := encode(event.Data)
		if err != nil {
			return err
		}

		eventsDB[i] = EventDB{
			ID:            event.ID,
			Type:          event.Type,
			AggregateID:   event.AggregateID,
			AggregateType: event.AggregateType,
			CommandID:     event.CommandID,
			RawData:       raw,
		}



		// the id contains the aggregateID as prefix
		// aggregateID.eventID
		//id := fmt.Sprintf("%s.%s", aggregateID, event.ID)
		log.Printf("id %s", event.AggregateID)

	}

	blob, err := encode(eventsDB)
	if err != nil {
		return err
	}

	// Now that events are saved, aggregate version needs to be updated
	aggregate := AggregateDB{
		ID:      aggregateID,
		Version: version + len(events),
		Events: blob,
	}

	/*aggregateBlob, err := encode(aggregate)
	if err != nil {
		return err
	}*/

	_, err = c.connector.Query("SELECT * FROM events WHERE _id = $1", aggregateID)
	if version == 0 {
		log.Println("Version is 0")
		if err == nil {
			return fmt.Errorf("postgresql: %s, aggregate already exists", aggregateID)
		} else{
			_, err = c.connector.Query("INSERT INTO events (attrs) VALUES($1)", aggregate)
			if err != nil {
				log.Fatalf("Error inserting initial event %s", err)
				return err
			}
		}

	} else {
		log.Println("Version is not 0")

		if aggregate.Version != version {
			return fmt.Errorf("badger: %s, aggregate version missmatch, wanted: %d, got: %d", aggregate.ID, version, aggregate.Version)
		}

		_, err = c.connector.Query("INSERT INTO events (attrs) VALUES($1)", aggregate)
		if err != nil {
			return err
		}
	}

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
		//eventsDB []EventDB
		id string
		version int
		jevents driver.Value
	)

	//var aggregate AggregateDB

	rows, err := c.connector.Query("SELECT * FROM events WHERE _id = $1", aggregateID)
	if err != nil {
		return events, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &version, &jevents)
		if err != nil {
			log.Fatalf("Error on cursor %s", err)
		}
		fmt.Printf("aggregate %#v\n %#v\n %#v\n", id, version, jevents)
	}
	/*
	if err = decode(jevents, eventsDB); err != nil {
		return events, err
	}

	events = make([]triper.Event, 1)
	err = decode(aggregate.Events, &events)
	if err != nil {
		return nil, err
	}
	//eventsDB = append(eventsDB, event)


	for i, dbEvent := range events {
		dataType, err := c.reg.Get(dbEvent.Type)
		if err != nil {
			return events, err
		}

		if err = decode(dbEvent.Data, dataType); err != nil {
			return events, err
		}

		// Translate dbEvent to triper.Event
		events[i] = triper.Event{
			AggregateID:   aggregateID,
			AggregateType: dbEvent.AggregateType,
			CommandID:     dbEvent.CommandID,
			Version:       dbEvent.Version,
			Type:          dbEvent.Type,
			Data:          dataType,
		}
	}

	 */

	return events, nil
}

func encode(value interface{}) (driver.Value, error) {
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

func decode(rawData driver.Value, value interface{}) error {
	if rawData != nil {
		b, ok := value.([]byte)
		if !ok {
			return errors.New("type assertion to []byte failed")
		}

		return json.Unmarshal(b, &rawData)

	}

	return errors.New("decode error, null value found")
}
