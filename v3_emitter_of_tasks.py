"""
Course: 44-671 : Module 05 & Module 06
Student: Erin Swan-Siegel
Date: 09-22-2023 & 09-29-2023

    This program reads rows from a .csv file and sends the value in each field to their respective queues
    on the RabbitMQ server, representing temperature readings from a smoker and up to two foods.
    This is Part 1 of 2, with the listeners/consumers being configured in Module 06.
    

"""
# Import modules
import pika
import sys
import webbrowser
import csv
import socket
import time
import logging

# Configuration Constants
file_name = "smoker-temps.csv"
sleep_secs = 1
host = "localhost"
queue_01 = "01-smoker"
queue_02 = "02-food-A"
queue_03 = "03-food-B"

# Function to prompt user to use the RabbitMQ Admin website; runs when show_offer == True
def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()
    
# Function to remove/clear-out queues from RabbitMQ
def reset_RabbitMQ():
    # create a blocking connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    # delete all known channel names
    ch.queue_delete(queue_01)
    ch.queue_delete(queue_02)
    ch.queue_delete(queue_03)

# Function remains unchanged from v2 
def send_message(queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Define Streaming data process
# Based on streaming code from Module 01
# Function accepts user information regarding the file to be read
def stream_row(file_name):
    """Read from input file and stream data."""
    logging.info(f"Starting to stream data from {file_name}")

    # Create a file object for input (r = read access)
    with open(file_name, "r") as input_file:
        logging.info(f"Opened for reading: {file_name}.")

        # Create a CSV reader object
        reader = csv.reader(input_file, delimiter=",")
        
        # Skip the header row and continue
        # We can get user prompt function from Module04
        header = next(reader)  # Skip header row
        logging.info(f"Skipped header row: {header}")

        # For each data row in the reader, read and send as a message then pause before reading the next row
        # using the value.join([array with comma-separated values]) will allow for future parsing
        for row in reader:
            # print(row)
            send_message(queue_01,",".join([row[0],row[1]]))
            send_message(queue_02,",".join([row[0],row[2]]))
            send_message(queue_03,",".join([row[0],row[3]]))
            logging.info(f"Reading row {reader.line_num}.")
            time.sleep(sleep_secs) # wait 30 seconds between messages


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__": 
    # ask the user if they'd like to open the RabbitMQ Admin site
    # based on the T/F setting of show_offer variable
    show_offer = True 
    if show_offer == True:
        offer_rabbitmq_admin_site()
    
    # Call function reset_RabbitMQ to clear out previously-created queues
    # In the future, this could be based on user input or possibly defining the queues, creating them if they
    # don't exist and purging them if they do, instead of deleting fully each time
    reset_RabbitMQ()

    # Stream values from csv file
    stream_row(file_name)
