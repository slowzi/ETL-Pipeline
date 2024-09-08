# from bs4 import BeautifulSoup
# from time import sleep
# from tqdm import tqdm
# import pandas as pd
# import requests
import re

# store text to list
def soup2list(src, list_, attr=None):
    if attr:
        for val in src:
            list_.append(val[attr])
    else:
        for val in src:
            list_.append(val.get_text())

# just take date in date of experience
def extract_date(text):
    match = re.search(r'\b(\w+ \d{2}, \d{4})\b', text)
    if match:
        return match.group(1)
    return None
