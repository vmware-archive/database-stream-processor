import dbsp_openapi
import json
import yaml

from dbsp_openapi.apis.tags.config_api import ConfigApi
from dbsp_openapi.apis.tags.pipeline_api import PipelineApi
from dbsp_openapi.model.pipeline_config import PipelineConfig
from dbsp_openapi.model.new_pipeline_request import NewPipelineRequest
from dbsp_openapi.model.transport_config import TransportConfig
from dbsp_openapi.model.format_config import FormatConfig
from dbsp_openapi.model.input_endpoint_config import InputEndpointConfig
from dbsp_openapi.model.output_endpoint_config import OutputEndpointConfig
from dbsp_openapi.model.kafka_input_config import KafkaInputConfig
from dbsp_openapi.model.kafka_output_config import KafkaOutputConfig
from dbsp_openapi.model.csv_parser_config import CsvParserConfig
from dbsp_openapi.model.csv_parser_config import CsvParserConfig
from dbsp_openapi.model.new_config_request import NewConfigRequest
from dbsp_openapi.model.update_config_request import UpdateConfigRequest
from dbsp_openapi.model.csv_encoder_config import CsvEncoderConfig

class ProjectConfig:
    def __init__(self, project, workers):
        self.project = project
        self.config_api = ConfigApi(project.dbsp_connection.api_client)
        self.pipeline_api = PipelineApi(project.dbsp_connection.api_client)
        self.pipeline_config = PipelineConfig(workers = workers)
        self.inputs = {}
        self.outputs = {}
        self.config_id = None
        self.config_version = None
        # self.workers = workers
        # print("config: " + str(self.pipeline_config))

    def add_input(self, name, input_endpoint_config):
        request_body = dbsp_openapi.api_client.RequestBody(
            content={
                'application/json': dbsp_openapi.api_client.MediaType(
                    schema=InputEndpointConfig),
            },
            required=True,
        )

        json_encoder = dbsp_openapi.api_client.JSONEncoder()
        print("yaml: " + str(yaml.dump(json_encoder.default(input_endpoint_config))))

        self.inputs[name] = input_endpoint_config

    def add_output(self, name, output_endpoint_config):
        self.outputs[name] = output_endpoint_config

    # def yaml(self):
    #    # return yaml.dump(self.inputs)

    def run(self):
        # print("yaml:" + self.yaml())
        if self.config_id == None:
            body = NewConfigRequest(
                project_id = self.project.project_id,
                name = '<anon>',
                config = '',
            )
            api_response = self.config_api.new_config(body = body)
            self.config_id = api_response.body['config_id']
            print("new_config response: " + str(api_response.body))
            self.config_version = api_response.body['version']
        else:
            body = UpdateConfigRequest(
                config_id = self.config_id,
                name = '<anon>',
                config = '',
            )
            api_response = self.config_api.update_config(body = body)
            self.config_version = api_response.body['version']

        body = NewPipelineRequest(
            config_id = self.config_id,
            project_id = self.project.project_id,
            config_version = self.config_version,
            project_version = self.project.project_version
        )

        try:
            api_response = self.pipeline_api.new_pipeline(body = body)
        except dbsp_openapi.ApiException as e:
            raise RuntimeError("Failed to create a pipeline") from e

        DBSPPipeline(self.dbsp_connection, api_response.body['pipeline_id'])
