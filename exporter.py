# coding=utf-8
"""
    jira issues export to CSV - all or current.
    default is ALL
    below example uses the current fields
"""
import os
from atlassian import Jira
import multiprocessing as mp
import PyPDF2
import time
import random
import config
# Maximum number of threads to send network requests
# (Reduce this number to reduce the number of concurrent network requests)
# MAX_POOL = 8
MAX_POOL = 10
# How many seconds we wait for a response from the server before timing out
REQUEST_TIMEOUT = 180
# How many seconds to sleep before making a network request, works as random value between 0 to REQUEST_DELAY
# (Set this value only if requests appear to be timing out frequently)
REQUEST_DELAY = 14


PATH = "tickets"
jira = Jira(config.URL, username=config.USERNAME, password=config.PASSWORD, timeout=REQUEST_TIMEOUT)


def download_pdf_file(jira, key, path):
    url = f"rest/com.midori.jira.plugin.pdfview/1.0/pdf/pdf-view/18/render?context=single_issue_view&jql=key={key}"
    try:
        response = jira.get(url, not_json_response=True, headers={"Accept": "application/pdf"})
    except Exception as e:
        print(e)
        time.sleep(random.randint(1, REQUEST_DELAY))
        return
    if not response:
        return
    with open(path + "/" + key + ".pdf", "wb") as file_obj:
        file_obj.write(response)


def is_valid_pdf(file_path):
    try:
        PyPDF2.PdfFileReader(open(file_path, "rb"))
    except Exception as e:
        print(f"invalid PDF file {file_path}")
        return False
    return True


def run_global(key):
    sub_folder = key.split("-")[0]
    number = key.split("-")[1]
    splitter_folder = str(int(int(number) / 1000) * 1000)
    full_path = PATH + "/" + sub_folder + "/" + splitter_folder
    print(f"Start to work with {key}")
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    full_file_name = full_path + "/" + key + ".pdf"
    if not os.path.isfile(full_file_name):
        download_pdf_file(jira=jira, key=key, path=full_path)
    else:
        if not is_valid_pdf(full_file_name):
            print(f"# Double downloading invalid PDF file {key}")
            download_pdf_file(jira=jira, key=key, path=full_path)


def main():
    reader = open("keys.txt", "r")
    keys = []
    for line in reader:
        key = line.strip()
        keys.append(key)
        sub_folder = key.split("-")[0]
        number = key.split("-")[1]
        splitter_folder = str(int(int(number) / 1000) * 1000)
        full_path = PATH + "/" + sub_folder + "/" + splitter_folder
        if not os.path.exists(full_path):
            print(f"Creating a directory for key {key}")
            os.makedirs(full_path)

    number_procs = mp.cpu_count()
    p = mp.Pool(processes=number_procs)
    p.map(run_global, keys)
    p.close()
    p.join()


if __name__ == "__main__":
    main()
