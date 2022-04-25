import json
import globus_sdk
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

# Use some other means to pass the endpoint id in
source_endpoint_id = "61f9954c-a4fa-11ea-8f07-0a21f750d19b" # Bebop
dest_endpoint_id = "08925f04-569f-11e7-bef8-22000b9a448b" # Theta

# Log in to Globus

CLIENT_ID = "1fb9c8a9-1aff-4d46-9f37-e3b0d44194f2"  # The ID of my app

# Start Native App Grant, and print out the URL where users login as part of the flow (step 2 above)
# First, create a client object that tracks state as we do this flow
native_auth_client = globus_sdk.NativeAppAuthClient(CLIENT_ID)

# Explicitly start the flow (some clients may support multiple flows)
native_auth_client.oauth2_start_flow(refresh_tokens=True)
authorize_url = native_auth_client.oauth2_get_authorize_url()
print(f"Login Here:\n\n{authorize_url}")
print(f"Thr programe helps you automatically login in to Globus")
options = webdriver.ChromeOptions()
options.add_argument("user-data-dir=/home/dofbot/.config/google-chrome")
options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
wd = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options)
# wd = webdriver.Chrome('/usr/bin/chromedriver')
wd.get(authorize_url)
element = wd.find_element_by_id('named-grant')
element.send_keys("sz3-io-test")
submit_button = wd.find_element_by_id('submit_approve')
submit_button.click()
wait = WebDriverWait(wd, 30)
try:
    auth_code_elem = wait.until(EC.visibility_of_element_located((By.XPATH, '//input[@id="auth-code"]')))
    auth_code = auth_code_elem.get_attribute('value')
    print("The authentication code: ", auth_code)
except TimeoutException:
    print("cannot obtain the authentication code")
    exit(1)

print("\nIMPORTANT NOTE: the link above can only be used once!")
print("If login or a later step in the flow fails, you must execute this cell again to generate a new link.")
# auth_code = input("Please enter the code you get after login here: ").strip()
token_response = native_auth_client.oauth2_exchange_code_for_tokens(auth_code)

globus_auth_data = token_response.by_resource_server["auth.globus.org"]
globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

# most specifically, you want these tokens as strings
# AUTH_TOKEN = globus_auth_data["access_token"]
# TRANSFER_TOKEN = globus_transfer_data["access_token"]

# the refresh token and access token are often abbreviated as RT and AT
transfer_rt = globus_transfer_data["refresh_token"]
transfer_at = globus_auth_data["access_token"]
expires_at_s = globus_transfer_data["expires_at_seconds"]

# construct a RefreshTokenAuthorizer
# note that `client` is passed to it, to allow it to do the refreshes
authorizer = globus_sdk.RefreshTokenAuthorizer(
    transfer_rt, native_auth_client, access_token=transfer_at, expires_at=expires_at_s
)

# and try using `tc` to make TransferClient calls. Everything should just
# work -- for days and days, months and months, even years
tc = globus_sdk.TransferClient(authorizer=authorizer)
# high level interface; provides iterators for list responses
print("My Endpoints:")
for ep in tc.endpoint_search(filter_scope="my-endpoints"):
    print("[{}] {}".format(ep["id"], ep["display_name"]))

# create a Transfer task consisting of one or more items
task_data = globus_sdk.TransferData(
    tc, source_endpoint_id, dest_endpoint_id
)
task_data.add_item(
    "~/ML_SZ3.ipynb",
    "~/a.ipynb"
)
task_doc = tc.submit_transfer(task_data)
task_id = task_doc["task_id"]
print(f"submitted transfer, task_id={task_id}")
response = tc.get_task(task_doc['task_id'])
print(f"Label: {response['label']}")
print(f"Status: {response['status']}")
print(f"Transfer: {response['source_endpoint_display_name']} -> {response['destination_endpoint_display_name']}")

import time

while(response['status'] == 'ACTIVE'):
    response = tc.get_task(task_id)
    print(f"Bytes transferred: {response['bytes_transferred']}")
    print(f"Files transferred: {response['files_transferred']}")
    print(f"Transfer rate: {response['effective_bytes_per_second']} Bps")
    print("-------------------------------------------------------")
    time.sleep(1)
