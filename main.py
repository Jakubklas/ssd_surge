from surge import SurgeStatus
from config import *

if __name__ == "__main__":
    surge = SurgeStatus(config_uri, fill_uri, threshold, business_type)

    # Get the config and fill data
    surge.get_config()
    surge.get_fill()
    
    # Filter for unfilled stations and add surge price
    surge.get_stations()
    surge.get_surge()

    # Format table and send a chime webhook
    surge.format_chime_table()
    surge.send_SSD_chime(webhook_url)