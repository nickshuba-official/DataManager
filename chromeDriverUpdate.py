import pandas as pd
import tempfile
import urllib.request
import zipfile
import os
import shutil
import configparser

config = configparser.ConfigParser()
config.optionxform = str
config_file = open(
    file=r"C:\Users\Administrator\Desktop\DataManager\.config.ini",
    encoding="utf-8"
)
config.read_file(config_file)

json_link = config["CHROMEDRIVER"]["UPDATE_LINK"]

json_table = pd.read_json(
    path_or_buf=json_link,
)

json_stable = json_table.loc["Stable", "channels"]
json_stable_table = pd.DataFrame(data=json_stable)

json_downloads = json_stable_table.loc["chromedriver", "downloads"]
json_downloads_table = pd.DataFrame(data=json_downloads)

download_link = json_downloads_table[json_downloads_table["platform"] == "win64"]["url"].iloc[0]

with (tempfile.TemporaryDirectory() as tmpdirname):
    chromedriver_save_link = str(tmpdirname) + r"\chromedriver.zip"
    chromedriver_file = urllib.request.urlretrieve(
        url=download_link,
        filename=chromedriver_save_link
    )

    with zipfile.ZipFile(file=chromedriver_save_link, mode='r') as zip_ref:
        zip_ref.extractall(tmpdirname)
        chromedriver_file_name = tmpdirname + r"\\" + str(os.listdir(tmpdirname)[0]) + r"\chromedriver.exe"
        chromedriver_file_dest = config["CHROMEDRIVER"]["FILE_PATH"]
        chromedriver_dest = config["CHROMEDRIVER"]["FOLDER"]

        os.remove(path=chromedriver_file_dest)

        shutil.move(chromedriver_file_name, chromedriver_dest)
