from surge import SurgeStatus
from config import *

if __name__ == "__main__":
    surge = SurgeStatus(s3_uri, threshold, business_type)