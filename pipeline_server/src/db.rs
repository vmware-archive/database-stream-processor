use tokio_postgres;
use tokio_postgres::{Client, NoTls};
use anyhow::{
    Result as AnyResult,
    Error as AnyError,
};
use log::error;

pub struct DBConfig {
    pub connection_string: String,
}

pub struct ProjectDB {
    dbclient: Client,
}

pub type ProjectId = i64;

pub enum ProjectStatus {
    None,
    Success,
    SqlError(String),
    RustError(String),
}

impl ProjectStatus {
    fn new(status_string: Option<&str>, error_string: Option<String>) -> AnyResult<Self> {
        match status_string {
            None => Ok(Self::None),
            Some("success") => Ok(Self::Success),
            Some("sql_error") => Ok(Self::SqlError(error_string.unwrap_or_default())),
            Some("rust_error") => Ok(Self::RustError(error_string.unwrap_or_default())),
            Some(status) => Err(AnyError::msg(format!("invalid status string '{status}'"))),
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
            dbclient
        })
    }

    pub async fn list_projects(&self) -> AnyResult<Vec<(ProjectId, String)>> {
        let rows = self.dbclient.query("SELECT id, name FROM project", &[]).await?;
        let mut result: Vec<(ProjectId, String)> = Vec::with_capacity(rows.len());

        for row in rows.into_iter() {
            result.push((row.try_get(0)?, row.try_get(1)?));
        }

        Ok(result)
    }

    pub async fn get_project_code(&self, project_id: ProjectId) -> AnyResult<String> {
        let row = self.dbclient.query_one("SELECT code FROM project WHERE id = $1", &[&project_id]).await?;
        let code = row.try_get(0)?;

        Ok(code)
    }

    pub async fn new_project(&self, project_name: &str, project_code: &str) -> AnyResult<ProjectId> {
        let row = self.dbclient.query_one("SELECT nextval('project_id_seq')", &[]).await?;
        let id: ProjectId = row.try_get(0)?;

        self.dbclient.execute("INSERT INTO project (id, name, code) VALUES($1, $2, $3)", &[&id, &project_name, &project_code]).await?;

        Ok(id)
    }

    pub async fn update_project(&self, project_id: ProjectId, project_name: &str, project_code: Option<String>) -> AnyResult<()> {
        if let Some(code) = project_code {
            self.dbclient.execute("UPDATE project SET name = $1, code = $2 WHERE id = $3", &[&project_name, &code, &project_id]).await?;
        } else {
            self.dbclient.execute("UPDATE project SET name = $1 WHERE id = $2", &[&project_name, &project_id]).await?;
        }

        Ok(())
    }

    pub async fn project_status(&self, project_id: ProjectId) -> AnyResult<ProjectStatus> {
        let row = self.dbclient.query_one("SELECT status, error FROM project WHERE id = $1", &[&project_id]).await?;

        let status: Option<&str> = row.try_get(0)?;
        let error: Option<String> = row.try_get(1)?;

        let status = ProjectStatus::new(status, error)?;
        Ok(status)
    }

    pub async fn set_project_status(&self, project_id: ProjectId, status: ProjectStatus) -> AnyResult<()> {
        let (status, error) = match status {
            ProjectStatus::None => (None, None),
            ProjectStatus::Success => (Some("success"), None),
            ProjectStatus::SqlError(error) => (Some("sql_error"), Some(error)),
            ProjectStatus::RustError(error) => (Some("rust_error"), Some(error)),
        };

        self.dbclient.execute("UPDATE project SET status = $1, error = $2 WHERE id = $3", &[&status, &error, &project_id]).await?;

        Ok(())
    }
}
