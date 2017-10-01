package main

import (
	"database/sql"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/fatih/structs"
	_ "github.com/lib/pq"
	"github.com/linkedin/goavro"
)

const (
	producerURL            string = "localhost:9092"
	kafkaTopic             string = "test"
	postgresURL            string = "postgres://postgres:postgres@localhost/sample?sslmode=disable"
	rentalBookingKeySchema        = `
	{
		"type": "record",
		"name": "Key",
		"namespace": "io.github.ajurasz",
		"fields": [
		  {"name": "provider", "type": "string"},
		  {"name": "provider_uid",  "type": "string"},
		  {"name": "date", "type": "string"}
		]
	  }	
	`
	rentalBookingSchema = `
	{
		"type": "record",
		"name": "Payload",
		"namespace": "io.github.ajurasz",
		"fields": [
		  {"name": "is_booked", "type": ["null", "boolean"], "default": null},
		  {"name": "price", "type": ["null", "double"], "default": null},
		  {"name": "booked_price", "type": ["null", "double"], "default": null},
		  {"name": "price_source", "type": ["null", "string"], "default": null}
		]
	}	
	`
)

type rentalBookingKey struct {
	Provider    string `structs:"provider"`
	ProviderUID string `structs:"provider_uid"`
	Date        string `structs:"date"`
}

type rentalBooking struct {
	IsBooked    sql.NullBool
	Price       sql.NullFloat64
	BookedPrice sql.NullFloat64
	PriceSource sql.NullString
}

func (rb *rentalBooking) ToStringMap() map[string]interface{} {
	datumIn := make(map[string]interface{})

	isBooked, _ := rb.IsBooked.Value()
	if isBooked == nil {
		datumIn["is_booked"] = goavro.Union("null", nil)
	} else {
		datumIn["is_booked"] = goavro.Union("boolean", isBooked)
	}

	price, _ := rb.Price.Value()
	if price == nil {
		datumIn["price"] = goavro.Union("null", nil)
	} else {
		datumIn["price"] = goavro.Union("double", price)
	}

	bookedPrice, _ := rb.BookedPrice.Value()
	if bookedPrice == nil {
		datumIn["booked_price"] = goavro.Union("null", nil)
	} else {
		datumIn["booked_price"] = goavro.Union("double", bookedPrice)
	}

	source, _ := rb.PriceSource.Value()
	if source == nil {
		datumIn["price_source"] = goavro.Union("null", nil)
	} else {
		datumIn["price_source"] = goavro.Union("string", source)
	}

	return datumIn
}

func encode(key rentalBookingKey, payload rentalBooking) ([]byte, []byte, error) {

	payloadCodec, err := goavro.NewCodec(rentalBookingSchema)
	if err != nil {
		return nil, nil, err
	}

	keyCodec, err := goavro.NewCodec(rentalBookingKeySchema)
	if err != nil {
		return nil, nil, err
	}

	payloadBB, err := payloadCodec.BinaryFromNative(nil, payload.ToStringMap())
	if err != nil {
		return nil, nil, err
	}

	keyBB, err := keyCodec.BinaryFromNative(nil, structs.Map(key))
	if err != nil {
		return nil, nil, err
	}

	return keyBB, payloadBB, nil
}

func main() {
	db, err := sql.Open("postgres", postgresURL)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	config := sarama.NewConfig()
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Retry.Max = 1
	config.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewAsyncProducer([]string{producerURL}, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
	doneCh := make(chan struct{})

	go func() {
		rows, err := db.Query("SELECT rental_property_id, is_booked, booking_date, asking_price, actual_price, price_source FROM rental_bookings")
		if err != nil {
			log.Fatal(err)
			return
		}
		for rows.Next() {

			var rentalPropertyID string
			var rentalBookingKey rentalBookingKey
			var rentalBooking rentalBooking
			rows.Scan(
				&rentalPropertyID,
				&rentalBooking.IsBooked,
				&rentalBookingKey.Date,
				&rentalBooking.BookedPrice,
				&rentalBooking.Price,
				&rentalBooking.PriceSource)

			err = db.QueryRow(
				"SELECT provider, provider_uid FROM rental_properties WHERE id = $1",
				rentalPropertyID).Scan(&rentalBookingKey.Provider, &rentalBookingKey.ProviderUID)
			if err != nil {
				log.Fatal(err)
				return
			}

			key, payload, err := encode(rentalBookingKey, rentalBooking)
			if err != nil {
				panic(err)
			}

			total := enqueued + errors
			if total%100000 == 0 {
				log.Println(total)
			}

			msg := &sarama.ProducerMessage{
				Topic: kafkaTopic,
				Key:   sarama.ByteEncoder(key),
				Value: sarama.ByteEncoder(payload),
			}
			select {
			case producer.Input() <- msg:
				enqueued++
			case err := <-producer.Errors():
				errors++
				log.Println("Failed to produce message:", err)
			case <-signals:
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
