import dbsp_api_client

from dbsp_api_client.api.pipeline import pipeline_start
from dbsp_api_client.api.pipeline import pipeline_pause
from dbsp_api_client.api.pipeline import pipeline_shutdown
from dbsp_api_client.api.pipeline import pipeline_delete
from dbsp_api_client.api.pipeline import pipeline_metadata
from dbsp_api_client.api.pipeline import pipeline_status
from dbsp_api_client.models.shutdown_pipeline_request import ShutdownPipelineRequest

class DBSPPipeline:

    def __init__(self, api_client, pipeline_id):
        self.api_client = api_client
        self.pipeline_id = pipeline_id
        
        pipeline_start.sync_detailed(client = self.api_client, pipeline_id = self.pipeline_id).unwrap("Failed to start pipeline")

    def pause(self):
        pipeline_pause.sync_detailed(client = self.api_client, pipeline_id = self.pipeline_id).unwrap("Failed to pause pipeline")

    def shutdown(self):
        request = ShutdownPipelineRequest(pipeline_id = self.pipeline_id)
        status = pipeline_shutdown.sync_detailed(client = self.api_client, json_body = request).unwrap("Failed to stut down pipeline")
    
    def delete(self):
        status = pipeline_delete.sync_detailed(client = self.api_client, pipeline_id = self.pipeline_id).unwrap("Failed to delete pipeline")

    def status(self):
        status = pipeline_status.sync_detailed(client = self.api_client, pipeline_id = self.pipeline_id).unwrap("Failed to retrieve pipeline status")
        return status.additional_properties

    def metadata(self):
        meta = pipeline_metadata.sync_detailed(client = self.api_client, pipeline_id = self.pipeline_id).unwrap("Failed to retrieve pipeline metadata")
        return meta.additional_properties
