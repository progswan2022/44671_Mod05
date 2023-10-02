"""
Course: 44-671 : Module 05 & Module 06
Student: Erin Swan-Siegel
Date: 09-22-2023 & 09-29-2023

This program listens for work messages related to FoodA, contiously.

"""

import pika
import sys
import time
import v3_emitter_of_tasks

foodA_analytics = {"Values":[]}

# define a callback function to be called when a message is received
def foodA_callback(ch, method, properties, body, arguments = foodA_analytics["Values"]):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string and process data by splitting the values by delimiter
    print(f" [x] Received reading {body.decode()}°F for FoodA")
    data = body.decode().split(',')
    # assign the smoker temperature value to a variable
    data_lead = data[1]
    
    # Temperature Analytics, to be run if the value is not null/blank
    if (len(data_lead) > 0):
        # delete the oldest temperature value when 20 measurements exist
        if len(arguments) >= 20:
            arguments.pop(0)

        # append the new smoker temperature value to the smoker_analytics dictionary
        arguments.append(data_lead)

        # if there are 20 values in the dictionary, assess the temperature difference 
        # between the first and the last values of the list; alert if necessary
        tempA = arguments[0]
        print(tempA)
        print(data_lead)
        print(float(data_lead) - float(tempA))
        if len(arguments) == 20:
            if -1 < float(data_lead) - float(tempA) < 1:
                print(f"ALERT: Temperature for FoodA has stalled!! < 1°F change for the previous 10 minutes")
    # when done with task, tell the user
    print(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "DefaultQueue"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to 1 (one) to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, on_message_callback=foodA_callback)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function, listening to queue 02, with the variables from v3_emitter_of_tasks
    main(v3_emitter_of_tasks.host, v3_emitter_of_tasks.queue_02)