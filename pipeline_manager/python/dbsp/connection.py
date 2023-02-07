import dbsp_openapi

from dbsp_openapi.apis.tags.project_api import ProjectApi
from dbsp_openapi.model.new_project_request import NewProjectRequest
from dbsp.project import DBSPProject

# from pprint import pprint


class DBSPConnection:
    def __init__(self, url="http://localhost:8080"):
        configuration = dbsp_openapi.Configuration(
            host=url
        )

        self.api_client = dbsp_openapi.ApiClient(configuration)
        self.project_api = ProjectApi(self.api_client)

        try:
            self.project_api.list_projects()
        except dbsp_openapi.ApiException as e:
            raise RuntimeError(
                "Failed to establish connection to the DBSP server") from e

    def new_project(self, name, sql_code):
        request = NewProjectRequest(code=sql_code, name=name)

        try:
            api_response = self.project_api.new_project(body=request)
        except dbsp_openapi.ApiException as e:
            raise RuntimeError("Failed to create a project") from e

        return DBSPProject(
            dbsp_connection=self, project_id=api_response.body['project_id'], project_version=api_response.body['version'])
