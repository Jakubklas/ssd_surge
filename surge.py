import pandas as pd
import json
import boto3
import requests
from io import BytesIO

from config import *


class SurgeStatus():

    def __init__(self, config_uri, fill_uri, threshold, business_type):
        self.config_bucket = "".join(config_uri.split("/")[2])
        self.config_key = "/".join(config_uri.split("/")[3:])
        self.fill_bucket = "".join(fill_uri.split("/")[2])
        self.fill_key = "/".join(fill_uri.split("/")[3:])
        self.threshold = threshold
        self.business_type = business_type
        self.client = boto3.client("s3")
    
    def get_fill(self):
        """
        Retrieves the latest live fill from an S3 bucket
        """
        obj = self.client.get_object(
            Bucket=self.fill_bucket,
            Key=self.fill_key
        )
        buffer = BytesIO()
        buffer.write(obj['Body'].read())
        buffer.seek(0)
        self.fill_df = pd.read_parquet(buffer)
        return self.fill_df

    def get_config(self):
        """
        Fetches the config incl. station RAG status and the needed surge 
        """
        obj = self.client.get_object(
            Bucket=self.config_bucket,
            Key=self.config_key
        )
        self.config = json.loads(obj['Body'].read().decode('utf-8'))
        return self.config
    

    def get_stations(self):
        """
        Fetches the stations, based on buciness_type, where T60 Fill is < 100%
        """
        # Filter stations by business type:
        if self.business_type == "ssd":
            filtered_df = self.fill_df[self.fill_df["Station"].str.startswith("V")]
        elif self.business_type == "core":
            filtered_df = self.fill_df[self.fill_df["Station"].str.startswith("D")]

        mask = (filtered_df["rounded_block_eta"] == self.threshold) & (filtered_df["Fill"] < 1.0)       # Filter for not filled as T-60
        self.stations = filtered_df[mask]["Station"]
        return self.stations


    def get_surge(self):
        """
        Returns 'station': 'surge_price' dictionary for selected stations 
        """
        surge = {}
        for station in self.stations:
            if station in self.config["rag_status"]["green"]:
                surge[station] = self.config["surge"][self.business_type]["green"]
            elif station in self.config["rag_status"]["amber"]:
                surge[station] = self.config["surge"][self.business_type]["amber"]
            elif station in self.config["rag_status"]["red"]:
                surge[station] = self.config["surge"][self.business_type]["red"]
        
        self.surge = surge
        return self.surge

    
    def format_chime_table(self):
        """
        Puts the Dataframe object into a mark-down format for the chat message
        """
        # Isolate/Rename the relevant columns
        table = self.fill_df.loc[self.fill_df["Station"].isin(self.stations), ["Station", "Block_Date_Time", "Duration"]]
        table["EndTime"] = (pd.to_datetime(table["Block_Date_Time"]) + pd.to_timedelta(table["Duration"], unit="m")).dt.strftime("%H:%M")
        table = table.drop("Duration", axis=1)

        # Add surge pricing to the message
        table["SuggestedPrice"] = 0
        for station, surge in self.stations.items():
            table.loc[table["Station"] == station, "SuggestedPrice"] = f"£{surge}"     

        self.markdown_table = "/md\n"
        # Add headers
        self.markdown_table += "| " + " | ".join(table.columns) + " |\n"
        # Add separator
        self.markdown_table += "| " + " | ".join(["---"] * len(table.columns)) + " |\n"
        # Add rows
        for _, row in table.iterrows():
            self.markdown_table += "| " + " | ".join(str(x) for x in row) + " |\n"
        

    def send_SSD_chime(self, webhook_url):
        """
        Notifies a Chime chat room about the latest necessary surge.
        """
        if self.stations.shape[0] != 0:

            header = "Summary of the currently unfilled SSD-D blocks:\n"

            headers = {'Content-Type': 'application/json'}
            payload = {'Content': self.markdown_table}
            payload_header = {'Content': header}
            
            requests.post(webhook_url, headers=headers, data=json.dumps(payload_header))
            response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
        
            if response.status_code == 200:
                print("Chime message sent.")
            else:
                print(f"Failed to send Chime message. Status code: {response.status_code}")
            return
        
        else:
            print("No surge notifications at the moment.")


