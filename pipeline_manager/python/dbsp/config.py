class InputStream:
    def __init__(transport, fmt):
        self.transport = transport
        self.fmt = fmt

class OutputStream:
    def __init__(transport, fmt):
        self.transport = transport
        self.fmt = fmt

class ProjectConfig:
    def __init__(project):
        self.project_id = project.project_id
        self.inputs = {}
        self.outputs = {}

    def add_output(name, transport, fmt):
        self.inputs[name] = InputStream(transport, fmt)
        self.outputs[name] = OutputStream(transport, fmt)
