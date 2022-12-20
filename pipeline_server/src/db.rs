use anyhow::{Error as AnyError, Result as AnyResult};
use log::error;
use serde::Serialize;
use std::collections::BTreeMap;
use tokio_postgres;
use tokio_postgres::{Client, NoTls};

pub struct DBConfig {
    pub connection_string: String,
}

pub struct ProjectDB {
    dbclient: Client,
}

pub type ProjectId = i64;
pub type Version = i64;

#[derive(Serialize)]
pub enum ProjectStatus {
    None,
    Pending,
    Compiling,
    Success,
    SqlError(String),
    RustError(String),
}

impl ProjectStatus {
    fn new(status_string: Option<&str>, error_string: Option<String>) -> AnyResult<Self> {
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
            ProjectStatus::Success => (Some("success"), None),
            ProjectStatus::Pending => (Some("pending"), None),
            ProjectStatus::Compiling => (Some("compiling"), None),
            ProjectStatus::SqlError(error) => (Some("sql_error"), Some(error)),
            ProjectStatus::RustError(error) => (Some("rust_error"), Some(error)),
        }
    }
}

impl ProjectDB {
    pub async fn connect(config: &DBConfig) -> AnyResult<Self> {
        let (dbclient, connection) =
            tokio_postgres::connect(&config.connection_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("database connection error: {}", e);
            }
        });

        Ok(Self {
            dbclient: Mutex::new()
        })
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

    pub async fn project_code(&self, project_id: ProjectId) -> AnyResult<(String, Version)> {
        let row = self
            .dbclient
            .query_opt("SELECT code, version FROM project WHERE id = $1", &[&project_id])
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
        &self,
        project_id: ProjectId,
        project_name: &str,
        project_code: &Option<String>,
    ) -> AnyResult<Version> {
        let transaction = self.dbclient.transaction()?;

        res = query_opt("SELECT version, code FROM project where id = $1")
            .await?
            .ok_or_else(|| AnyError::msg(format!("unknown project id '{project_id}'")))?;

        let mut version: Version = res.try_get(0)?;
        let old_code: String = res.try_get(1)?;

        match project_code {
            Some(code) if old_code != code => {
                if old_code != code {
                    version += 1;
                    transaction
                        .execute(
                            "UPDATE project SET version = $1, name = $2, code = $3, status = NULL, error = NULL WHERE id = $4",
                            &[&version, &project_name, code, &project_id],
                        )
                        .await?;
                }
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

        transaction.commit()?;

        Ok(version)
    }

    pub async fn project_status(&self, project_id: ProjectId) -> AnyResult<(Version, ProjectStatus)> {
        let row = self
            .dbclient
            .query_one(
                "SELECT version, status, error FROM project WHERE id = $1",
                &[&project_id],
            )
            .await?;

        let version: Option<Version> = row.try_get(0)?;
        let status: Option<&str> = row.try_get(1)?;
        let error: Option<String> = row.try_get(2)?;

        let status = ProjectStatus::new(status, error)?;
        Ok((version, status))
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
        &self,
        project_id: ProjectId,
        expected_version: Version,
        status: ProjectStatus,
    ) -> AnyResult<bool> {
        let (status, error) = status.to_columns();

        let transaction = self.dbclient.transaction()?;

        res = transaction.query_opt("SELECT version FROM project where id = $1", &[&project_id])
            .await?
            .ok_or_else(|| AnyError::msg(format!("unknown project id '{project_id}'")))?;

        let version = res.try_get(0);

        if expected_version == version {
            transaction.execute(
                    "UPDATE project SET status = $1, error = $2, status_since = now() WHERE id = $3",
                    &[&status, &error, &project_id],
                )
                .await?;
        }

        transaction.commit()?;

        Ok(expected_version == version)
    }

    pub async fn set_project_pending(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<bool> {
        let (version, status) = self.project_status(project_id)?;
        if version != expected_version {
            return false;
        }

        if status == ProjectStatus::Pending || status == ProjectStatus::Compiling {
            return false;
        }

        set_project_status(project_id, ProjectStatus::Pending)?;

        return true;
    }

    pub async fn cancel_project(
        &self,
        project_id: ProjectId,
        expected_version: Version,
    ) -> AnyResult<bool> {
        let (version, status) = self.project_status(project_id)?;
        if version != expected_version {
            return false;
        }

        if status != ProjectStatus::Pending || status != ProjectStatus::Compiling {
            return false;
        }

        set_project_status(project_id, ProjectStatus::None)?;

        return true;
    }
}
