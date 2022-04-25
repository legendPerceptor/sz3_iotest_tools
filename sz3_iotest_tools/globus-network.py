import subprocess
import json
import globus_sdk
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from fabric import Connection

from config import GlobusAgentConfig, parse_args, CompressionConfig
import time
import os
import pandas as pd

from jinja2 import Template

class GlobusAgent():
    def __init__(self, config : GlobusAgentConfig):
        self.source_endpoint_id = config.source_endpoint
        self.dest_endpoint_id = config.destination_endpoint
        self.client_id = config.client_id
        self.source_folder = config.source_folder
        self.destination_folder = config.destination_folder
        self.do_compression = config.do_compression
        self.result_path = config.transfer_result_path
        self.compression_config_path = config.compression_config_path
        self.host_source = config.host_source
        self.host_destination = config.host_destination
        
    
    def initialize_globus_app(self):
        self.native_auth_client = globus_sdk.NativeAppAuthClient(self.client_id)
        self.native_auth_client.oauth2_start_flow(refresh_tokens=True)
        authorize_url = self.native_auth_client.oauth2_get_authorize_url()
        print(f"Login Here:\n\n{authorize_url}")
        print(f"Thr programe helps you automatically login in to Globus")
        options = webdriver.ChromeOptions()
        options.add_argument("user-data-dir=/home/dofbot/.config/google-chrome")
        options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        wd = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=options)
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
        except Exception as e:
            print(e)
            print("cannot obtain the authentication code; require user input")
            auth_code = input("paste the authentication code here:")
        token_response = self.native_auth_client.oauth2_exchange_code_for_tokens(auth_code)
        globus_auth_data = token_response.by_resource_server["auth.globus.org"]
        globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
        transfer_rt = globus_transfer_data["refresh_token"]
        transfer_at = globus_auth_data["access_token"]
        expires_at_s = globus_transfer_data["expires_at_seconds"]
        # construct a RefreshTokenAuthorizer
        # note that `client` is passed to it, to allow it to do the refreshes
        self.authorizer = globus_sdk.RefreshTokenAuthorizer(
            transfer_rt, self.native_auth_client, access_token=transfer_at, expires_at=expires_at_s)

        # and try using `tc` to make TransferClient calls. Everything should just
        # work -- for days and days, months and months, even years
        self.tc = globus_sdk.TransferClient(authorizer=self.authorizer)
        self.show_my_endpoints()
        # create a Transfer task consisting of one or more items
        self.task_data = globus_sdk.TransferData(
            self.tc, self.source_endpoint_id, self.dest_endpoint_id
        )
        if self.do_compression:
            self.compress_data()

    def compress_data(self):
        compress_config = CompressionConfig.from_yaml(self.compression_config_path)
        if self.host_source is None:
            print("The source host is not specified, cannot perform compression task")
            return
        if self.host_destination is None:
            print("The destination host is not specified, cannot perform compression")
            return
        self.conn_source = Connection(host=self.host_source, port=22)
        # self.conn_destination = Connection(host=self.host_destination, port=22)
        ls_content= self.conn_source.run(f"ls {self.source_folder}")
        print("run type:", type(ls_content))
        files = [x.strip() for x in ls_content.stdout.split("\n")]
        filtered_files = list(filter(lambda x: x.endswith(str(compress_config.file_extension)), files))
        print(len(filtered_files))
        print(filtered_files)
        # Ues template to render the job.sh
        self.cwd = os.getcwd()
        with open("./job.sh", 'r') as f:
            data = f.read()
        self.conn_source.put(f"{self.cwd}/compression.py", str(compress_config.work_dir))
        self.conn_source.put(f"{self.cwd}/compression_config.yaml", str(compress_config.work_dir))
        # subprocess.run(f"scp {self.cwd}/compression.py {self.host_source}:{compress_config.work_dir}", shell=True)
        # subprocess.run(f"scp {self.cwd}/compression_config.yaml {self.host_source}:{compress_config.work_dir}", shell=True)
        for idx, file in enumerate(filtered_files):
            
            temp = Template(data)
            cfg = {
                "jobname": f"job-{idx}",
                "filepath": f"{self.source_folder / file}",
                "workdir": compress_config.work_dir,
                "compress_cfg": f"{compress_config.work_dir / self.compression_config_path}"
            }
            job = temp.render(cfg = cfg)
            with open(f"./jobs/job-{idx}.sh", 'w') as f:
                f.write(job)
            # Trasnfer job.sh, compression.py, compression_config.yaml to the source machine
            self.conn_source.put(f"{self.cwd}/jobs/job-{idx}.sh", str(compress_config.work_dir))
            self.conn_source.run(f"sbatch {compress_config.work_dir}/job-{idx}.sh")
            # subprocess.run(f"scp {self.cwd}/jobs/job-{idx} {self.host_source}:{compress_config.work_dir}", shell=True)
        
        while True:
            queue_result = self.conn_source.run("squeue -u ac.yuanjian")
            jobs = queue_result.stdout.split("\n")
            if len(jobs) < 3:
                print("all jobs have finished")
                break
            jobs = [row.split() for row in jobs]
            columns = jobs[0]
            df = pd.DataFrame(data=jobs[1:], columns= columns)   
            print(df)
            
            time.sleep(5)
    
    def build_job(self):
        pass
    
    def transfer_data(self):
        # print(str(self.source_folder / "*"))
        # print(str(self.source_folder))
        self.task_data.add_item(
            str(self.source_folder) + "/",
            str(self.destination_folder) + "/",
            recursive= True
        )
        task_doc = self.tc.submit_transfer(self.task_data)
        self.task_id = task_doc["task_id"]
        print(f"submitted transfer, task_id={self.task_id}")
        response = self.tc.get_task(task_doc['task_id'])
        print(f"Label: {response['label']}")
        print(f"Status: {response['status']}")
        print(f"Transfer: {response['source_endpoint_display_name']} -> {response['destination_endpoint_display_name']}")
        self.loop_until_finish(response)
    
    def show_my_endpoints(self):
        print("My Endpoints:")
        for ep in self.tc.endpoint_search(filter_scope="my-endpoints"):
            print("[{}] {}".format(ep["id"], ep["display_name"]))
        
    def loop_until_finish(self, response):
        while(response['status'] == 'ACTIVE'):
            response = self.tc.get_task(self.task_id)
            print(f"Bytes transferred: {response['bytes_transferred']}")
            print(f"Files transferred: {response['files_transferred']}")
            print(f"Transfer rate: {response['effective_bytes_per_second']} Bps")
            print("-------------------------------------------------------")
            time.sleep(1)
        if response["status"] == "SUCCEEDED":
            print(f"Bytes transferred: {response['bytes_transferred']}")
            print(f"Files transferred: {response['files_transferred']}")
            print(f"Transfer rate: {response['effective_bytes_per_second']} Bps")
            print(f"Request Time: {response['request_time']}")
            print(f"Completion Time: {response['completion_time']}")
            response_dict = self.get_dict_from_globus_response(response)
            with open(str(self.result_path), 'w') as f:
                result = json.dump(response_dict, f)
        

    def get_dict_from_globus_response(self, response):
        return {
            "DATA_TYPE": response["DATA_TYPE"],
            "bytes_checksummed": response["bytes_checksummed"],
            "bytes_transferred": response["bytes_transferred"],
            "canceled_by_admin": response["canceled_by_admin"],
            "canceled_by_admin_message": response["canceled_by_admin_message"],
            "command": response["command"],
            "completion_time": response["completion_time"],
            "deadline": response["deadline"],
            "delete_destination_extra": response["delete_destination_extra"],
            "destination_endpoint": response["destination_endpoint"],
            "destination_endpoint_display_name": response["destination_endpoint_display_name"],
            "destination_endpoint_id": response["destination_endpoint_id"],
            "directories": response["directories"],
            "effective_bytes_per_second": response["effective_bytes_per_second"],
            "encrypt_data": response["encrypt_data"],
            "fail_on_quota_errors": response["fail_on_quota_errors"],
            "fatal_error": response["fatal_error"],
            "faults": response["faults"],
            "files": response["files"],
            "files_skipped": response["files_skipped"],
            "files_transferred": response["files_transferred"],
            "history_deleted": response["history_deleted"],
            "is_paused": response["is_paused"],
            "label": response["label"],
            "nice_status": response["nice_status"],
            "nice_status_expires_in": response["nice_status_expires_in"],
            "nice_status_short_description": response["nice_status_short_description"],
            "owner_id": response["owner_id"],
            "preserve_timestamp": response["preserve_timestamp"],
            "recursive_symlinks": response["recursive_symlinks"],
            "request_time": response["request_time"],
            "skip_source_errors": response["skip_source_errors"],
            "source_endpoint": response["source_endpoint"],
            "source_endpoint_display_name": response["source_endpoint_display_name"],
            "source_endpoint_id": response["source_endpoint_id"],
            "status": response["status"],
            "subtasks_canceled": response["subtasks_canceled"],
            "subtasks_expired": response["subtasks_expired"],
            "subtasks_failed": response["subtasks_failed"],
            "subtasks_pending": response["subtasks_pending"],
            "subtasks_retrying": response["subtasks_retrying"],
            "subtasks_skipped_errors": response["subtasks_skipped_errors"],
            "subtasks_succeeded": response["subtasks_succeeded"],
            "subtasks_total": response["subtasks_total"],
            "symlinks": response["symlinks"],
            "sync_level": response["sync_level"],
            "task_id": response["task_id"],
            "type": response["type"],
            "verify_checksum": response["verify_checksum"]
        }
        

def main(cfg: GlobusAgentConfig):
    globus_agent = GlobusAgent(cfg)
    # globus_agent.initialize_globus_app()
    globus_agent.compress_data()
    # globus_agent.transfer_data()
    
if __name__ == "__main__":
    args = parse_args()
    cfg = GlobusAgentConfig.from_yaml(args.config)
    main(cfg)