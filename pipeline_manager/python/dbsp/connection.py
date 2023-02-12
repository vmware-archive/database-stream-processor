import dbsp_api_client

from dbsp_api_client.models.new_project_request import NewProjectRequest
from dbsp_api_client.api.project import list_projects
from dbsp_api_client.api.project import new_project
from dbsp.project import DBSPProject

# from pprint import pprint


class DBSPConnection:
    def __init__(self, url="http://localhost:8080"):
        self.api_client = dbsp_api_client.Client(
                base_url = "http://localhost:8080",
                timeout = 20.0)

        list_projects.sync_detailed(client = self.api_client).unwrap("Failed to fetch project list from the DBSP server")

    def new_project(self, name, sql_code):
        request = NewProjectRequest(code=sql_code, name=name)

        new_project_response = new_project.sync_detailed(client = self.api_client, json_body=request).unwrap("Failed to create a project")

        return DBSPProject(
            api_client=self.api_client,
            project_id=new_project_response.project_id,
            project_version=new_project_response.version)
