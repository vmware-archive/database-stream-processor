import dbsp_api_client

from dbsp_api_client.models.compile_project_request import CompileProjectRequest
from dbsp_api_client.api.project import project_status
from dbsp_api_client.api.project import compile_project
import time
import sys

class DBSPProject:
    def __init__(self, api_client, project_id, project_version):
        self.api_client = api_client
        self.project_id = project_id
        self.project_version = project_version

    def compile(self, timeout=sys.maxsize):
        body = CompileProjectRequest(
            project_id=self.project_id,
            version=self.project_version,
        )
        # Queue project for compilation.
        compile_project.sync_detailed(client = self.api_client, json_body=body).unwrap("Failed to queue project for compilation")

        start = time.time()
        while time.time() - start < timeout:
            status = self.status()
            if status != 'Compiling' and status != 'Pending':
                if status == 'Success':
                    return
                elif status['SqlError'] != None:
                    raise RuntimeError("SQL error: " + status['SqlError'])
                elif status['RustError'] != None:
                    raise RuntimeError("Rust compiler error: " + status['RustError'])
                else:
                    raise RuntimeError("Unexpected project status : " + status)
            time.sleep(0.5)
        
        raise RuntimeError("Timeout waiting for the project to compile after " + str(timeout) + "s")

    def status(self):
        response = project_status.sync_detailed(
                client = self.api_client,
                project_id = self.project_id).unwrap("Failed to retrieve project status")
        
        # if api_response.body['version'] != self.project_version:
        #    raise RuntimeError(
        #            "Project modified on the server.  Expected version: " + self.project_version + ". ") from e
        
        return response.status
