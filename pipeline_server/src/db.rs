use crate::{ProjectStatus, ServerConfig};
use anyhow::{Error as AnyError, Result as AnyResult};
use log::error;
use std::collections::BTreeMap;
use tokio_postgres::{Client, NoTls};

pub struct ProjectDB {
    dbclient: Client,
}

pub type ProjectId = i64;
pub type ConfigId = i64;
pub type PipelineId = i64;
pub type Version = i64;

impl ProjectStatus {
    fn from_columns(status_string: Option<&str>, error_string: Option<String>) -> AnyResult<Self> {
        match status_string {
            None => Ok(Self::None),
            Some("success") => Ok(Self::Success),
            Some("pending") => Ok(Self::Pending),
            Some("compiling") => Ok(Self::Compiling),
            Some("sql_error") => Ok(Self::SqlError(error_string.unwrap_or_default())),
            Some("rust_error") => Ok(Self::RustError(error_string.unwrap_or_default())),
            Some(status) => Err(AnyError::msg(format!("invalid status string '{status}'"))),
        }
    }
    fn to_columns(&self) -> (Option<String>, Option<String>) {
        match self {
            ProjectStatus::None => (None, None),
            ProjectStatus::Success => (Some("success".to_string()), None),
            ProjectStatus::Pending => (Some("pending".to_string()), None),
            ProjectStatus::Compiling => (Some("compiling".to_string()), None),
            ProjectStatus::SqlError(error) => (Some("sql_error".to_string()), Some(error.clone())),
            ProjectStatus::RustError(error) => {
                (Some("rust_error".to_string()), Some(error.clone()))
            }
        }
    }
}

impl ProjectDB {
    pub(crate) async fn connect(config: &ServerConfig) -> AnyResult<Self> {
        let (dbclient, connection) =
            tokio_postgres::connect(&config.pg_connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("database connection error: {}", e);
            }
        });

        Ok(Self { dbclient })
    }

    pub async fn clear_pending_projects(&self) -> AnyResult<()> {
        self.dbclient
            .execute("UPDATE project SET status = NULL, error = NULL;", &[])
            .await?;

        Ok(())
    }

    pub async fn list_projects(&self) -> AnyResult<BTreeMap<ProjectId, (String, Version)>> {
        let rows = self
            .dbclient
            .query("SELECT id, name, version FROM project", &[])
            .await?;
        let mut result = BTreeMap::new();

        for row in rows.into_iter() {
            result.insert(row.try_get(0)?, (row.try_get(1)?, row.try_get(2)?));
        }

        Ok(result)
    }

    pub async fn project_code(&self, project_id: ProjectId) -> AnyResult<(Version, String)> {
        let row = self
            .dbclient
            .query_opt(
                "SELECT version, code FROM project WHERE id = $1",
                &[&project_id],
            )
            .await?
            .ok_or_else(|| AnyError::msg(format!("unknown project id '{project_id}'")))?;

        Ok((row.try_get(0)?, row.try_get(1)?))
    }

    pub async fn new_project(
        &self,
        project_name: &str,
        project_code: &str,
    ) -> AnyResult<(ProjectId, Version)> {
        let row = self
            .dbclient
            .query_one("SELECT nextval('project_id_seq')", &[])
            .await?;
        let id: ProjectId = row.try_get(0)?;

        self.dbclient
            .execute(
                "INSERT INTO project (id, version, name, code, status_since) VALUES($1, 1, $2, $3, now())",
                &[&id, &project_name, &project_code],
            )
            .await?;

        Ok((id, 1))
    }

    pub async fn update_project(
        &mut self,
        project_id: ProjectId,
        project_name: &str,
        project_code: &Option<String>,
    ) -> AnyResult<Version> {
        let transaction = self.dbclient.transaction().await?;

        let res = transaction
            .query_opt(
                "SELECT version, code FROM project where id = $1",
                &[&project_id],
            )
            .await?
            .ok_or_else(|| AnyError::msg(format!("unknown project id '{project_id}'")))?;

        let mut version: Version = res.try_get(0)?;
        let old_code: String = res.try_get(1)?;

        match project_code {
            Some(code) if &old_code != code => {
                version += 1;
                transaction
                    .execute(
                        "UPDATE project SET version = $1, name = $2, code = $3, status = NULL, error = NULL WHERE id = $4",
                        &[&version, &project_name, code, &project_id],
                    )
                    .await?;
            }
            _ => {
                transaction
                    .execute(
                        "UPDATE project SET name = $1 WHERE id = $2",
                        &[&project_name, &project_id],
                    )
                    .await?;
            }
        }

        transaction.commit().await?;

        Ok(version)
    }

    pub async fn project_status(
        &self,
        project_id: ProjectId,
    ) -> AnyResult<Option<(Version, ProjectStatus)>> {
        let row = self
            .dbclient
            .query_opt(
                "SELECT version, status, error FROM project WHERE id = $1",
                &[&project_id],
            )
            .await?;

        if let Some(row) = row {
            let version: Version = row.try_get(0)?;
            let status: Option<&str> = row.try_get(1)?;
            let error: Option<String> = row.try_get(2)?;

            let status = ProjectStatus::from_columns(status, error)?;
            Ok(Some((version, status)))
        } else {
            Ok(None)
        }
    }

    pub async fn set_project_status(
        &self,
        project_id: ProjectId,
        status: ProjectStatus,
    ) -> AnyResult<()> {
        let (status, error) = status.to_columns();

        self.dbclient
            .execute(
                "UPDATE project SET status = $1, error = $2, status_since = now() WHERE id = $3",
                &[&status, &error, &project_id],
            )
            .await?;

        Ok(())
    }

    pub async fn set_project_status_guarded(
        &mut self,
        project_id: ProjectId,
        expected_version: Version,
        status: ProjectStatus,
    ) -> AnyResult<bool> {
        let (status, error) = status.to_columns();

        let transaction = self.dbclient.transaction().await?;

        let res = transaction
            .query_opt("SELECT version FROM project where id = $1", &[&project_id])
            .await?;

        if res.is_none() {
            return Ok(false);
        }

        let version: Version = res.unwrap().try_get(0)?;

        if expected_version == version {
            transaction.execute(
                    "UPDATE project SET status = $1, error = $2, status_since = now() WHERE id = $3",
                    &[&status, &error, &project_id],
                )
                .await?;
        }

        transaction.commit().await?;

        Ok(expected_version == version)
    }

    pub async fn set_project_pending(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<bool> {
        let ver_stat = self.project_status(project_id).await?;
        if ver_stat.is_none() {
            return Ok(false);
        }

        let (version, status) = ver_stat.unwrap();

        if version != expected_version {
            return Ok(false);
        }

        if status == ProjectStatus::Pending || status == ProjectStatus::Compiling {
            return Ok(false);
        }

        self.set_project_status(project_id, ProjectStatus::Pending)
            .await?;

        Ok(true)
    }

    pub async fn cancel_project(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<bool> {
        let ver_stat = self.project_status(project_id).await?;
        if ver_stat.is_none() {
            return Ok(false);
        }

        let (version, status) = ver_stat.unwrap();

        if version != expected_version {
            return Ok(false);
        }

        if status != ProjectStatus::Pending || status != ProjectStatus::Compiling {
            return Ok(false);
        }

        self.set_project_status(project_id, ProjectStatus::None)
            .await?;

        Ok(true)
    }

    pub async fn delete_project(&self, project_id: ProjectId) -> AnyResult<bool> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM project WHERE id = $1", &[&project_id])
            .await?;

        Ok(num_deleted > 0)
    }

    pub async fn next_job(&self) -> AnyResult<Option<(ProjectId, Version)>> {
        // Find the oldest pending project.
        let rows = self
            .dbclient
            .query("SELECT id, version FROM project WHERE status = 'pending' AND status_since = (SELECT min(status_since) FROM project WHERE status = 'pending')", &[])
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let project_id: ProjectId = rows[0].try_get(0)?;
        let version: Version = rows[0].try_get(1)?;

        Ok(Some((project_id, version)))
    }

    pub async fn list_project_configs(&self, project_id: ProjectId) -> AnyResult<BTreeMap<ConfigId, (Version, String, String)>> {
        let rows = self
            .dbclient
            .query("SELECT id, version, name, config FROM project_config WHERE project_id = $1", &[&project_id])
            .await?;
        let mut result = BTreeMap::new();

        for row in rows.into_iter() {
            result.insert(row.try_get(0)?, (row.try_get(1)?, row.try_get(2)?, row.try_get(3)?));
        }

        Ok(result)
    }

    pub async fn get_project_config(&self, config_id: ConfigId) -> AnyResult<Option<(ProjectId, Version, String, String)>> {
        let res = self
            .dbclient
            .query_opt("SELECT project_id, version, name, config FROM project_config WHERE config_id = $1", &[&config_id])
            .await?;

        match res {
            None => Ok(None),
            Some(row) => Ok(Some((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?, row.try_get(3)?)))
        }
    }

    pub async fn new_config(
        &self,
        project_id: ProjectId,
        config_name: &str,
        config: &str,
    ) -> AnyResult<(ConfigId, Version)> {
        let row = self
            .dbclient
            .query_one("SELECT nextval('project_config_id_seq')", &[])
            .await?;
        let id: ConfigId = row.try_get(0)?;

        self.dbclient
            .execute(
                "INSERT INTO project_config (id, project_id, version, name, config) VALUES($1, $2, 1, $3, $4)",
                &[&id, &project_id, &config_name, &config],
            )
            .await?;

        Ok((id, 1))
    }

    pub async fn update_config(
        &mut self,
        config_id: ConfigId,
        config_name: &str,
        config: &Option<String>,
    ) -> AnyResult<Version> {
        let transaction = self.dbclient.transaction().await?;

        let res = transaction
            .query_opt(
                "SELECT version, config FROM project_config where id = $1",
                &[&config_id],
            )
            .await?
            .ok_or_else(|| AnyError::msg(format!("unknown config id '{config_id}'")))?;

        let mut version: Version = res.try_get(0)?;
        let old_config: String = res.try_get(1)?;

        let config = config.clone().unwrap_or(old_config);

        version += 1;
        transaction
            .execute(
                "UPDATE project_config SET version = $1, name = $2, config = $3 WHERE id = $4",
                &[&version, &config_name, &config, &config_id],
            )
            .await?;

        transaction.commit().await?;

        Ok(version)
    }

    pub async fn delete_config(&self, config_id: ConfigId) -> AnyResult<bool> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM project_config WHERE id = $1", &[&config_id])
            .await?;

        Ok(num_deleted > 0)
    }

    pub async fn alloc_pipeline_id(&self) -> AnyResult<PipelineId> {
        let row = self
            .dbclient
            .query_one("SELECT nextval('pipeline_id_seq')", &[])
            .await?;
        let id: PipelineId = row.try_get(0)?;

        Ok(id)
    }

    pub async fn new_pipeline(
        &self,
        pipeline_id: PipelineId,
        project_id: ProjectId,
        project_version: Version,
        port: u16,
    ) -> AnyResult<()> {
        // Convert port to a SQL-compatible type (see `trait ToSql`).
        let port = port as i16;

        self.dbclient
            .execute(
                "INSERT INTO pipeline (id, project_id, project_version, port, created) VALUES($1, $2, $3, $4, now())",
                &[&pipeline_id, &project_id, &project_version, &port],
            )
            .await?;

        Ok(())
    }

    pub async fn delete_pipeline(&self, pipeline_id: PipelineId) -> AnyResult<bool> {
        let num_deleted = self
            .dbclient
            .execute("DELETE FROM pipeline WHERE id = $1", &[&pipeline_id])
            .await?;

        Ok(num_deleted > 0)
    }
}
