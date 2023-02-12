from dbsp_api_client.types import Response
from dbsp_api_client.models.error_response import ErrorResponse

class DBSPServerError(Exception):
    """Exception raised when a request to the HTTP server fails.
    """

    def __init__(self, response: Response, description):
        self.description = description
        self.response = response

        if isinstance(response.parsed, ErrorResponse):
            response_body = response.parsed.message
        else:
            response_body = str(response.parsed)
        message = description + "\nHTTP response code: " + str(response.status_code) + "\nResponse body: " + response_body 
        super().__init__(message)


def unwrap(self, description = "DBSP request failed"):
    if 200 <= self.status_code <= 202:
        return self.parsed
    else:
        raise DBSPServerError(response = self, description = description)

Response.unwrap = unwrap

