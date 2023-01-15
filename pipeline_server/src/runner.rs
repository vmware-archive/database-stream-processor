use crate::{
    NewPipelineRequest, NewPipelineResponse, PipelineId, ProjectDB, ProjectId, ProjectStatus,
    ServerConfig, Version,
};
use actix_web::HttpResponse;
use anyhow::{Error as AnyError, Result as AnyResult};
use log::error;
use regex::Regex;
use reqwest::StatusCode;
use serde::Serialize;
use std::{path::Path, pin::Pin, process::Stdio, sync::Arc};
use tokio::{
    fs,
    fs::{create_dir_all, remove_dir_all, File},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeek, BufReader, SeekFrom},
    process::{Child, Command},
    sync::Mutex,
    time::{sleep, Duration, Instant},
};

const STARTUP_TIMEOUT: Duration = Duration::from_millis(10_000);

#[derive(Serialize)]
struct PipelineMetadata {
    project_id: ProjectId,
    version: Version,
    code: String,
}

pub struct Runner {
    db: Arc<Mutex<ProjectDB>>,
    config: ServerConfig,
    // TODO: The Prometheus server should be isntantiated and managed by k8s.
    prometheus_server: Option<Child>,
}

impl Drop for Runner {
    fn drop(&mut self) {
        if let Some(mut prometheus) = self.prometheus_server.take() {
            let _ = prometheus.start_kill();
        }
    }
}

impl Runner {
    pub(crate) async fn new(db: Arc<Mutex<ProjectDB>>, config: &ServerConfig) -> AnyResult<Self> {
        let prometheus_server = Self::start_prometheus(config).await?;
        Ok(Self {
            db,
            config: config.clone(),
            prometheus_server,
        })
    }

    async fn start_prometheus(config: &ServerConfig) -> AnyResult<Option<Child>> {
        // Create Prometheus dir before starting any pipelines so that the
        // Prometheus server can locate the directory to scan.
        let prometheus_dir = config.prometheus_dir();
        create_dir_all(&prometheus_dir).await.map_err(|e| {
            AnyError::msg(format!(
                "error creating Prometheus configs directory '{}': {e}",
                prometheus_dir.display()
            ))
        })?;

        if config.with_prometheus {
            // Prometheus server configuration.
            let prometheus_config = format!(
                r#"
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: dbsp
    file_sd_configs:
    - files:
      - '{}/pipeline*.yaml'
"#,
                prometheus_dir.display()
            );
            let prometheus_config_file = config.prometheus_server_config_file();
            fs::write(&prometheus_config_file, prometheus_config)
                .await
                .map_err(|e| {
                    AnyError::msg(format!(
                        "error writing Prometheus config file '{}': {e}",
                        prometheus_config_file.display()
                    ))
                })?;

            // Start the Prometheus server, which will
            // inherit stdout, stderr from us.
            let prometheus_process = Command::new("prometheus")
                .arg("--config.file")
                .arg(&prometheus_config_file)
                .stdin(Stdio::null())
                .spawn()
                .map_err(|e| AnyError::msg(format!("failed to start Prometheus server, {e}")))?;

            Ok(Some(prometheus_process))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn run_pipeline(
        &self,
        request: &NewPipelineRequest,
    ) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        // Check: project exists, version = current version, compilation completed.
        match db.project_status(request.project_id).await? {
            None => {
                return Ok(HttpResponse::BadRequest()
                    .body(format!("unknown project id '{}'", request.project_id)));
            }
            Some((version, _status)) if version != request.project_version => {
                return Ok(HttpResponse::Conflict().body(format!(
                    "specified version '{}' does not match the latest project version '{version}'",
                    request.project_version
                )));
            }
            Some((_version, status)) if status != ProjectStatus::Success => {
                return Ok(HttpResponse::Conflict().body("project hasn't been compiled yet"));
            }
            _ => {}
        }

        let config_yaml = match db.get_project_config(request.config_id).await? {
            None => {
                return Ok(HttpResponse::BadRequest()
                    .body(format!("unknown project config id '{}'", request.config_id)));
            }
            Some((project_id, _version, _name, _config)) if project_id != request.project_id => {
                return Ok(HttpResponse::BadRequest().body(format!(
                    "config '{}' does not belong to project '{}'",
                    request.config_id, request.project_id
                )));
            }
            Some((_project_id, version, _name, _config)) if version != request.config_version => {
                return Ok(HttpResponse::Conflict().body(format!(
                    "specified config version '{}' does not match the latest config version '{version}'",
                    request.config_version,
                )));
            }
            Some((_project_id, _version, _name, config)) => config,
        };

        let pipeline_id = db.alloc_pipeline_id().await?;

        let mut pipeline_process = self.start(&db, request, &config_yaml, pipeline_id).await?;

        // Unlock db -- the next part can be slow.
        drop(db);

        // Start listening to log file until either port number or error shows up or
        // child process exits.
        match Self::wait_for_startup(&self.config.log_file_path(pipeline_id)).await {
            Ok(port) => {
                // Store pipeline in the database.
                if let Err(e) = self
                    .db
                    .lock()
                    .await
                    .new_pipeline(
                        pipeline_id,
                        request.project_id,
                        request.project_version,
                        port,
                    )
                    .await
                {
                    let _ = pipeline_process.kill().await;
                    return Err(e);
                };
                let json_string =
                    serde_json::to_string(&NewPipelineResponse { pipeline_id, port }).unwrap();

                // Create Prometheus config file for the pipeline.
                // The Prometheus server should pick up this file automatically.
                self.create_prometheus_config(request.project_id, pipeline_id, port)
                    .await
                    // Don't abandon pipeline creation on error.
                    .unwrap_or_else(|e| {
                        error!("Failed to create Prometheus config file for pipeline '{pipeline_id}': {e}");
                    });

                Ok(HttpResponse::Ok()
                    .content_type(mime::APPLICATION_JSON)
                    .body(json_string))
            }
            Err(e) => {
                let _ = pipeline_process.kill().await;
                Err(e)
            }
        }
    }

    async fn start(
        &self,
        db: &ProjectDB,
        request: &NewPipelineRequest,
        config_yaml: &str,
        pipeline_id: PipelineId,
    ) -> AnyResult<Child> {
        // Create pipeline directory (delete old directory if exists); write metadata
        // and config files to it.
        let pipeline_dir = self.config.pipeline_dir(pipeline_id);
        create_dir_all(&pipeline_dir).await?;

        // let config_yaml = self.create_topics(config_yaml).await?;

        let config_file_path = self.config.config_file_path(pipeline_id);
        fs::write(&config_file_path, config_yaml).await?;

        let (_version, code) = db.project_code(request.project_id).await?;

        let metadata = PipelineMetadata {
            project_id: request.project_id,
            version: request.project_version,
            code,
        };
        let metadata_file_path = self.config.metadata_file_path(pipeline_id);
        fs::write(
            &metadata_file_path,
            serde_json::to_string(&metadata).unwrap(),
        )
        .await?;

        let log_file_path = self.config.log_file_path(pipeline_id);
        let log_file = File::create(&log_file_path).await?;
        let out_file = log_file.try_clone().await?;

        // Locate project executable.
        let executable = self.config.project_executable(request.project_id);

        // Run executable, set current directory to pipeline directory, pass metadata
        // file and config as arguments.
        let pipeline_process = Command::new(&executable)
            .arg("--config-file")
            .arg(&config_file_path)
            .arg("--metadata-file")
            .arg(&metadata_file_path)
            .stdin(Stdio::null())
            .stdout(out_file.into_std().await)
            .stderr(log_file.into_std().await)
            .spawn()
            .map_err(|e| AnyError::msg(format!("failed to run '{}': {e}", executable.display())))?;

        Ok(pipeline_process)
    }

    async fn wait_for_startup(log_file_path: &Path) -> AnyResult<u16> {
        let mut log_file_lines = BufReader::new(File::open(log_file_path).await?).lines();

        let start = Instant::now();

        let portnum_regex = Regex::new(r"Started HTTP server on port (\w+)\b").unwrap();
        let error_regex = Regex::new(r"Failed to create server.*").unwrap();

        loop {
            if let Some(line) = log_file_lines.next_line().await? {
                if let Some(captures) = portnum_regex.captures(&line) {
                    if let Some(portnum_match) = captures.get(1) {
                        if let Ok(port) = portnum_match.as_str().parse::<u16>() {
                            return Ok(port);
                        } else {
                            return Err(AnyError::msg("invalid port number in log: '{line}'"));
                        }
                    } else {
                        return Err(AnyError::msg(
                            "couldn't parse server port number from log: '{line}'",
                        ));
                    }
                };
                if let Some(mtch) = error_regex.find(&line) {
                    return Err(AnyError::msg(mtch.as_str().to_string()));
                };
            }

            if start.elapsed() > STARTUP_TIMEOUT {
                let log = Self::log_suffix(log_file_path).await;
                return Err(AnyError::msg(format!("waiting for pipeline initialization status timed out after {STARTUP_TIMEOUT:?}\n{log}")));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn create_prometheus_config(
        &self,
        project_id: ProjectId,
        pipeline_id: PipelineId,
        port: u16,
    ) -> AnyResult<()> {
        let config = format!(
            r#"- targets: [ "localhost:{port}" ]
  labels:
    pipeline_id: {pipeline_id}
    project_id: {project_id}"#
        );
        fs::write(
            self.config.prometheus_pipeline_config_file(pipeline_id),
            config,
        )
        .await?;

        Ok(())
    }

    /*
    async fn create_topics(&self, config_yaml: &str) -> AnyResult<(KafkaResources, String)> {
        let mut config: ControllerConfig = serde_yaml::from_str(config_yaml)
            .map_err(|e| AnyError::msg(format!("error parsing pipeline configuration: {e}")))?;

        for (_, input_config) in config.inputs.iter_mut() {
            if input_config.transport.name == "kafka" {
                if let Some(topics) = input_config.transport.config.get("topics") {
                    let topics_string = serde_yaml::to_string(topics);
                    let topics: Vec<String> = serde_yaml::from_value(topics)
                        .map_err(|e| AnyError::msg(format!("error parsing Kafka topic list '{topics_string}': {e}")))?;
                    for topic in topics.iter_mut() {
                        if topic == "?" {
                            *topic = self.generate_topic_name("input_");
                        }
                    }
                    input_config.transport.config.set("topics", serde_yaml::to_value(topics))
                }
            }
        }

        for (_, output_config) in config.outputs.iter_mut() {
            if input_config.transport.name == "kafka" {
                if let Some(YamlValue::String(topic) = input_config.transport.config.get_mut("topic") {
                    if topic == "?" {
                        topic = Self::generate_topic_name("output_");
                    }
                }
            }
        }

        let new_config = serde_yaml::to_string(config);

        Ok((kafka_resources, new_config))

    }
    */

    async fn log_suffix_inner(log_file_path: &Path) -> AnyResult<String> {
        let mut buf = Vec::with_capacity(10000);

        let mut file = File::open(log_file_path).await?;

        Pin::new(&mut file).start_seek(SeekFrom::End(-10000))?;
        file.read_to_end(&mut buf).await?;

        let suffix = String::from_utf8_lossy(&buf);
        Ok(format!("log file tail:\n{suffix}"))
    }

    async fn log_suffix(log_file_path: &Path) -> String {
        Self::log_suffix_inner(log_file_path)
            .await
            .unwrap_or_else(|e| format!("[unable to read log file: {e}]"))
    }

    pub(crate) async fn kill_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        self.do_kill_pipeline(&db, pipeline_id).await
    }

    async fn do_kill_pipeline(
        &self,
        db: &ProjectDB,
        pipeline_id: PipelineId,
    ) -> AnyResult<HttpResponse> {
        if let Some((port, killed)) = db.pipeline_status(pipeline_id).await? {
            if killed {
                return Ok(HttpResponse::Ok().body("pipeline already killed"));
            };

            let url = format!("http://localhost:{port}/kill");
            let response = reqwest::get(&url).await?;

            if response.status().is_success() {
                db.set_pipeline_killed(pipeline_id).await?;
                Ok(HttpResponse::Ok().finish())
            } else if response.status() == StatusCode::NOT_FOUND {
                db.set_pipeline_killed(pipeline_id).await?;
                Ok(HttpResponse::Ok().body("pipeline at '{url}' already killed"))
            } else {
                Ok(HttpResponse::InternalServerError().body(format!(
                    "failed to kill the pipeline; response from pipeline server: {response:?}"
                )))
            }
        } else {
            Ok(HttpResponse::BadRequest().body(format!("unknown pipeline id '{pipeline_id}'")))
        }
    }

    pub(crate) async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<HttpResponse> {
        let db = self.db.lock().await;

        // Kill pipeline.
        let response = self.do_kill_pipeline(&db, pipeline_id).await?;
        if !response.status().is_success() {
            return Ok(response);
        }

        // TODO: Delete temporary topics.

        // Delete pipeline directory.
        remove_dir_all(self.config.pipeline_dir(pipeline_id)).await?;
        db.delete_pipeline(pipeline_id).await?;

        Ok(HttpResponse::Ok().finish())
    }
}
