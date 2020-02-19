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
	var id string

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
			Version:       1 + version + i,
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
		Version: version + len(eventsDB),
		Events: blob,
	}

	/*aggregateBlob, err := encode(aggregate)
	if err != nil {
		return err
	}*/

	err = c.connector.QueryRow("SELECT _id FROM events WHERE _id = $1", aggregateID).Scan(&id)
	if version == 0 && err != nil{
		log.Println("Version is 0")
		if id == aggregateID {
			return fmt.Errorf("postgresql: %s, aggregate already exists", aggregateID)
		} else{
			_, err = c.connector.Query("INSERT INTO events (_id, version, events) VALUES($1, $2, $3)", aggregate.ID, aggregate.Version, aggregate.Events )
			if err != nil {
				log.Fatalf("Error inserting initial event %s", err)
				return err
			}
		}

	} else {
		log.Println("Version is not 0")
		if err != nil {
			log.Fatalln("Error the query should find an initial event")
			return err
		}

		if aggregate.Version != version {
			return fmt.Errorf("postgres: %s, aggregate version missmatch, wanted: %d, got: %d", aggregate.ID, version, aggregate.Version)
		}

		_, err = c.connector.Query("INSERT INTO events (_id, version, events) VALUES($1, $2, $3)", aggregate.ID, aggregate.Version, aggregate.Events)
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
		eventsDB []EventDB
		id string
		version int
		jevents json.RawMessage
		resultData interface{}
		events []triper.Event
	)

	//var aggregate AggregateDB

	err := c.connector.QueryRow("SELECT * FROM events WHERE _id = $1", aggregateID).Scan(&id, &version, &jevents)
	if err != nil {
		log.Fatalln("error couldn't find the aggregate id")
		return nil, err
	}



	/*if err = decode(jevents, eventsDB); err != nil {
		return events, err
	}

   */

	//events :=  make([]triper.Event, version)
	//eventsDB := make([]EventDB, version)
	err = decodeRaw(jevents, &eventsDB)
	if err != nil {
		log.Fatalf("error on decoding %s", err)
		return nil, err
	}

	fmt.Printf("aggregate %#v\n %#v\n  %#v\n ",id, version, eventsDB)
	//eventsDB = append(eventsDB, event)

	for i, dbEvent := range eventsDB {
		//dataType, err := c.reg.Get(dbEvent.Type)
		//if err != nil {
		//	return events, err
		//}

		byteRawData := []byte(dbEvent.RawData)
		if err = decodeRaw(byteRawData, &resultData); err != nil {
			log.Fatalf("error on decoding events %s", err)
			//return events, err
		}



		// Translate dbEvent to triper.Event
		events[i] = triper.Event{
			AggregateID:   aggregateID,
			AggregateType: dbEvent.AggregateType,
			CommandID:     dbEvent.CommandID,
			Version:       dbEvent.Version,
			Type:          dbEvent.Type,
			Data:          resultData,
		}
		fmt.Printf("event version %#v , %#v\n ", dbEvent.Version, resultData)
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

func decode(rawData json.RawMessage, value *[]EventDB) error {
	if rawData != nil {
		return json.Unmarshal(rawData, &value)
	}
	return errors.New("decode error, null value found")
}
func decodeRaw(rawData json.RawMessage, value interface{}) error {
	//if rawData != nil {
		err := json.Unmarshal(rawData, &value)
		if err != nil {
			log.Printf("error unmarshaling %s", err)
			//return errors.New("scan could not unmarshal to interface{}")
		}

	//}
	log.Printf("rawdata %s", rawData)
	//return errors.New("decode error, null value found")
	return nil
}
