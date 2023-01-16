use crate::{PipelineId, ProjectId};
use anyhow::{Error as AnyError, Result as AnyResult};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use tokio::fs::{canonicalize, create_dir_all};

const fn default_server_port() -> u16 {
    8080
}

fn default_pg_connection_string() -> String {
    "host=localhost user=dbsp".to_string()
}

fn default_working_directory() -> String {
    ".".to_string()
}

#[derive(Deserialize, Clone)]
pub(crate) struct ServerConfig {
    #[serde(default = "default_server_port")]
    pub port: u16,
    #[serde(default = "default_pg_connection_string")]
    pub pg_connection_string: String,
    #[serde(default = "default_working_directory")]
    pub working_directory: String,
    pub sql_compiler_home: String,
    pub dbsp_override_path: Option<String>,
    pub static_html: Option<String>,
    #[serde(default)]
    pub with_prometheus: bool,
}

impl ServerConfig {
    pub(crate) async fn canonicalize(self) -> AnyResult<Self> {
        let mut result = self.clone();
        create_dir_all(&result.working_directory)
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "unable to create or open working directry '{}': {e}",
                    result.working_directory
                ))
            })?;

        result.working_directory = canonicalize(&result.working_directory)
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "error canonicalizing working directory path '{}': {e}",
                    result.working_directory
                ))
            })?
            .to_string_lossy()
            .into_owned();
        result.sql_compiler_home = canonicalize(&result.sql_compiler_home)
            .await
            .map_err(|e| {
                AnyError::msg(format!(
                    "failed to access SQL compiler home '{}': {e}",
                    result.sql_compiler_home
                ))
            })?
            .to_string_lossy()
            .into_owned();

        if let Some(path) = result.dbsp_override_path.as_mut() {
            *path = canonicalize(&path)
                .await
                .map_err(|e| {
                    AnyError::msg(format!(
                        "failed to access dbsp override directory '{path}': {e}"
                    ))
                })?
                .to_string_lossy()
                .into_owned();
        }

        if let Some(path) = result.static_html.as_mut() {
            *path = canonicalize(&path)
                .await
                .map_err(|e| AnyError::msg(format!("failed to access '{path}': {e}")))?
                .to_string_lossy()
                .into_owned();
        }

        Ok(result)
    }

    pub(crate) fn crate_name(project_id: ProjectId) -> String {
        format!("project{project_id}")
    }

    pub(crate) fn workspace_dir(&self) -> PathBuf {
        Path::new(&self.working_directory).join("cargo_workspace")
    }

    pub(crate) fn project_dir(&self, project_id: ProjectId) -> PathBuf {
        self.workspace_dir().join(Self::crate_name(project_id))
    }

    pub(crate) fn sql_file_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("project.sql")
    }

    pub(crate) fn sql_compiler_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home)
            .join("SQL-compiler")
            .join("sql-to-dbsp")
    }

    pub(crate) fn sql_lib_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home).join("lib")
    }

    pub(crate) fn stderr_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("err.log")
    }

    pub(crate) fn stdout_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("out.log")
    }

    pub(crate) fn rust_program_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("src").join("main.rs")
    }

    pub(crate) fn project_toml_template_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home)
            .join("temp")
            .join("Cargo.toml")
    }

    pub(crate) fn project_toml_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("Cargo.toml")
    }

    pub(crate) fn workspace_toml_path(&self) -> PathBuf {
        self.workspace_dir().join("Cargo.toml")
    }

    pub(crate) fn project_executable(&self, project_id: ProjectId) -> PathBuf {
        Path::new(&self.workspace_dir())
            .join("target")
            .join("release")
            .join(Self::crate_name(project_id))
    }

    pub(crate) fn pipeline_dir(&self, pipeline_id: PipelineId) -> PathBuf {
        Path::new(&self.working_directory)
            .join("pipelines")
            .join(format!("pipeline{pipeline_id}"))
    }

    pub(crate) fn config_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id).join("config.yaml")
    }

    pub(crate) fn metadata_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id).join("metadata.json")
    }

    pub(crate) fn log_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id).join("pipeline.log")
    }

    pub(crate) fn out_file_path(&self, pipeline_id: PipelineId) -> PathBuf {
        self.pipeline_dir(pipeline_id).join("pipeline.out")
    }

    pub(crate) fn prometheus_dir(&self) -> PathBuf {
        Path::new(&self.working_directory).join("prometheus")
    }

    pub(crate) fn prometheus_server_config_file(&self) -> PathBuf {
        Path::new(&self.working_directory).join("prometheus.yaml")
    }

    pub(crate) fn prometheus_pipeline_config_file(&self, pipeline_id: PipelineId) -> PathBuf {
        self.prometheus_dir()
            .join(format!("pipeline{pipeline_id}.yaml"))
    }
}
