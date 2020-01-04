import producer_server


def run_kafka_server():
	# DONE get the json file path
    input_file = "police-department-calls-for-service.json"

    # DONE fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="org.sf.crime.stats",
        bootstrap_servers="localhost:9092",
        client_id="client_producer_jh"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
