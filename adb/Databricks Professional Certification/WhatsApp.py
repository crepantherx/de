# Databricks notebook source
# MAGIC %sh
# MAGIC pip install selenium

# COMMAND ----------

from datetime import datetime
import dateutil.relativedelta
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
import urllib.request, json 

# COMMAND ----------

with urllib.request.urlopen("https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json") as url:
    data = json.load(url)
    print(data['channels']['Stable']['version'])
    url = data['channels']['Stable']['downloads']['chromedriver'][0]['url']
    print(url)
    
    # set the url as environment variable to use in scripting 
    os.environ['url']= url

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -N $url  -O /tmp/chromedriver_linux64.zip
# MAGIC
# MAGIC unzip /tmp/chromedriver_linux64.zip -d /tmp/chromedriver/

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo rm -r /var/lib/apt/lists/* 
# MAGIC sudo apt clean && 
# MAGIC    sudo apt update --fix-missing -y

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add
# MAGIC sudo echo "deb https://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list
# MAGIC sudo apt-get -y update
# MAGIC sudo apt-get -y install google-chrome-stable

# COMMAND ----------

def init_chrome_browser(download_path, chrome_driver_path,  url):
     
    options = Options()
    prefs = {'download.default_directory' : download_path, 'profile.default_content_setting_values.automatic_downloads': 1, "download.prompt_for_download": False,
  "download.directory_upgrade": True,   "safebrowsing.enabled": True ,
  "translate_whitelists": {"vi":"en"},
  "translate":{"enabled":"true"}}
    options.add_experimental_option('prefs', prefs)
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')    # wont work without this feature in databricks can't display browser
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    options.add_argument('window-size=2560,1440')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    options.add_argument('--lang=en')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    print(f"{datetime.now()}    Launching Chrome...")
    browser = webdriver.Chrome(service=Service(chrome_driver_path), options=options)
    print(f"{datetime.now()}    Chrome launched.")
    browser.get(url)
    print(f"{datetime.now()}    Browser ready to use.")
    return browser

# COMMAND ----------

driver = init_chrome_browser(
    download_path="/tmp/downloads",
    chrome_driver_path="/tmp/chromedriver/chromedriver-linux64/chromedriver",
    url= "https://www.google.com"
)

# COMMAND ----------

driver.find_element_by_css_selector("img").get_attribute("alt")

# COMMAND ----------

chat=driver.find_element_by_xpath("/html/body/div[1]/div/div/div[3]/div/header/div[2]/div/span/div[2]/div")
chat.click()
search=driver.find_element_by_xpath('//*[@id="app"]/div/div/div[2]/div[1]/span/div/span/div/div[1]/div/label/div/div[2]')
search.click()

# COMMAND ----------


