# Import required classes and modules
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from datetime import datetime
import time
import json

# Define class
class PublishTrades():



    def __init__(self, projectID, topic, tradesFile):

        self.projectID = projectID
        self.topic = topic
        self.tradesFile = tradesFile

        self.topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=self.projectID,
            topic=self.topic
        )


    def readFile(self):

        print("Reading Trades data")

        with open(self.tradesFile) as handler:
            data = handler.readlines()

        # Convert from list of strings to list of lists
        data_list = []
        for line in data:
            new_line = line.split(",")
            data_list.append(new_line)

        # Strip new line character
        for line in data_list:
            line[10] = line[10].strip()

        # Reformat dates
        for line in data_list[1:]:
            line_format = datetime.strptime(line[6], "%d/%m/%Y").date()
            line[6] = line_format.strftime("%Y-%m-%d")
            if line[9] == "":
                line[9] = None
            else:
                line_format_1 = datetime.strptime(line[9], "%d/%m/%Y").date()
                line[9] = line_format_1.strftime("%Y-%m-%d")

        # Add an additional item per list
        for line in data_list:
            line.append("")
            line.append("")

        data_list[0][11] = "DQMetric"
        data_list[0][12] = "DQMetricResult"

        # Populate DQ Result
        for lines in data_list[1:]:

            try:
                if lines[9] == None:
                    lines[11] = "0"
                    lines[12] = "Fail - Blank date"

                elif datetime.strptime(lines[9], "%Y-%m-%d").date() < datetime(2000, 1, 1).date():

                    lines[11] = "0"
                    lines[12] = "Fail - Invalid date"

                else:
                    lines[11] = "1"
                    lines[12] = "Pass"

            except ValueError:
                lines[11] = "0"
                lines[12] = "Fail - Blank date"

        return data_list


    def createConnection(self):
        print("Establishing connection with Google Cloud Platform...")
        publisher = pubsub_v1.PublisherClient()

        return publisher


    def publishMessage(self, publisher, message):
        future = publisher.publish(self.topic_name, message.encode())
        return_str = "Message successfully sent. Return ID: " + future.result()
        print(return_str)


    def streamTrades(self):

        self.readFile()

        publisher = self.createConnection()

        i = 1
        file_complete = False
        trades = self.readFile()
        headers = trades[0]

        # Populate publish string
        for lines in trades[1:]:
            dict = {headers[i]: lines[i] for i in range(len(headers))}

            publish_message = json.dumps(dict, indent=4, sort_keys=True)

            print("Publishing tades: ID-{}".format(i))
            self.publishMessage(publisher, publish_message)

            dict = dict.clear()
            publish_message = ""
            i += 1

            time.sleep(1)

        print("All Trades have been processed")