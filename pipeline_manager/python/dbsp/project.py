import dbsp_openapi

from dbsp_openapi.apis.tags.project_api import ProjectApi
from dbsp_openapi.apis.tags.pipeline_api import PipelineApi
from dbsp_openapi.model.compile_project_request import CompileProjectRequest
import time
import sys

from pprint import pprint

class DBSPProject:
    def __init__(self, dbsp_connection, project_id, project_version):
        self.dbsp_connection = dbsp_connection
        self.project_api = ProjectApi(self.dbsp_connection.api_client)
        self.pipeline_api = PipelineApi(self.dbsp_connection.api_client)
        self.project_id = project_id
        self.project_version = project_version

    def compile(self, timeout=sys.maxsize):
        body = CompileProjectRequest(
            project_id=self.project_id,
            version=self.project_version,
        )
        try:
            # Queue project for compilation.
            api_response = self.project_api.compile_project(body=body)
        except dbsp_openapi.ApiException as e:
            raise RuntimeError("Failed to retrieve project status") from e

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
        try:
            api_response = self.project_api.project_status(
                path_params={'project_id': self.project_id})
        except dbsp_openapi.ApiException as e:
            raise RuntimeError("Failed to retrieve project status") from e
        
        # if api_response.body['version'] != self.project_version:
        #    raise RuntimeError(
        #            "Project modified on the server.  Expected version: " + self.project_version + ". ") from e
        
        return api_response.body['status']
