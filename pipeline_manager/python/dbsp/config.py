import dbsp_openapi

from dbsp_openapi.model.pipeline_config import PipelineConfig
from dbsp_openapi.model.transport_config import TransportConfig
from dbsp_openapi.model.format_config import FormatConfig
from dbsp_openapi.model.input_endpoint_config import InputEndpointConfig
from dbsp_openapi.model.output_endpoint_config import OutputEndpointConfig
from dbsp_openapi.model.kafka_input_config import KafkaInputConfig

class ProjectConfig:
    def __init__(self, project, workers):
        self.project_id = project.project_id
        self.pipeline_config = PipelineConfig(workers = workers)
        self.inputs = {}
        self.outputs = {}
        # self.workers = workers
        # print("config: " + str(self.pipeline_config))

    #    self.workers = workers

    #def set_attribute():

    def add_input(name, input_endpoint_config):
        self.inputs[name] = input_endpoint_config

    def add_output(name, output_endpoint_config):
        self.outputs[name] = output_endpoint_config
