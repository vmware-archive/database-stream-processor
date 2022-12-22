use crate::{ProjectDB, ProjectId, ProjectStatus, Version};
use anyhow::Result as AnyResult;
use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
};
use tokio::{
    fs,
    fs::File,
    process::{Child, Command},
    select, spawn,
    sync::Mutex,
    task::JoinHandle,
    time::{sleep, Duration},
};

const COMPILER_POLL_INTERVAL: Duration = Duration::from_millis(1000);

#[derive(Clone)]
pub struct CompilerConfig {
    pub workspace_directory: String,
    pub sql_compiler_home: String,
}

impl CompilerConfig {
    fn project_dir(&self, project_id: ProjectId) -> PathBuf {
        Path::new(&self.workspace_directory).join(project_id.to_string())
    }

    fn sql_file_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("project.sql")
    }

    fn sql_compiler_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home)
            .join("SQL-compiler")
            .join("sql-to-dbsp")
    }

    fn sql_lib_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home).join("lib")
    }

    fn stderr_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("err.log")
    }

    fn stdout_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("out.log")
    }

    fn rust_program_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("src").join("lib.rs")
    }

    fn project_toml_template_path(&self) -> PathBuf {
        Path::new(&self.sql_compiler_home)
            .join("temp")
            .join("Cargo.toml")
    }

    fn project_toml_path(&self, project_id: ProjectId) -> PathBuf {
        self.project_dir(project_id).join("Cargo.toml")
    }

    fn workspace_toml_path(&self) -> PathBuf {
        Path::new(&self.workspace_directory).join("Cargo.toml")
    }
}

pub struct Compiler {
    // config: CompilerConfig,
    // command_sender: Sender<CompilerCommand>,
    compiler_task: JoinHandle<AnyResult<()>>,
}

impl Compiler {
    pub fn new(config: &CompilerConfig, db: Arc<Mutex<ProjectDB>>) -> Self {
        // let (command_sender, command_receiver) = channel(100);

        let compiler_task = spawn(Self::compiler_task(config.clone(), db));
        Self {
            //command_sender,
            compiler_task,
        }
    }

    async fn compiler_task(
        /* command_receiver: Receiver<CompilerCommand>, */ config: CompilerConfig,
        db: Arc<Mutex<ProjectDB>>,
    ) -> AnyResult<()> {
        let mut job: Option<CompilationJob> = None;

        loop {
            select! {
                _ = sleep(COMPILER_POLL_INTERVAL) => {
                    let mut cancel = false;
                    if let Some(job) = &job {
                        let ver_status = db.lock().await.project_status(job.project_id).await?;
                        if ver_status != Some((job.version, ProjectStatus::Compiling)) {
                            cancel = true;
                        }
                    }
                    if cancel {
                        job.unwrap().cancel().await;
                        job = None;
                    }
                }
                Some(exit_status) = async {
                    if let Some(job) = &mut job {
                        Some(job.wait().await)
                    } else {
                        None
                    }
                }, if job.is_some() => {
                    let project_id = job.as_ref().unwrap().project_id;
                    let version = job.as_ref().unwrap().version;
                    let mut db = db.lock().await;

                    match exit_status {
                        Ok(status) if status.success() && job.as_ref().unwrap().is_sql() => {
                            // SQL compiler succeeded -- start Rust job.
                            job = Some(CompilationJob::rust(&config, project_id, version).await?);
                        }
                        Ok(status) if status.success() && job.as_ref().unwrap().is_rust() => {
                            // Rust compiler succeeded -- declare victory.
                            db.set_project_status_guarded(project_id, version, ProjectStatus::Success).await?;
                            job = None;
                        }
                        Ok(status) => {
                            let output = job.as_ref().unwrap().error_output(&config).await?;
                            let status = if job.as_ref().unwrap().is_rust() {
                                ProjectStatus::RustError(format!("{output}\nexit code: {status}"))
                            } else {
                                ProjectStatus::SqlError(format!("{output}\nexit code: {status}"))
                            };
                            // change project status to error
                            db.set_project_status_guarded(project_id, version, status).await?;
                            job = None;
                        }
                        Err(e) => {
                            let status = if job.unwrap().is_rust() {
                                ProjectStatus::RustError(format!("I/O error: {e}"))
                            } else {
                                ProjectStatus::SqlError(format!("I/O error: {e}"))
                            };
                            // change project status to error
                            db.set_project_status_guarded(project_id, version, status).await?;
                            job = None;
                        }
                    }
                }
            }
            if job.is_none() {
                let mut db = db.lock().await;
                if let Some((project_id, version)) = db.next_job().await? {
                    job = Some(CompilationJob::sql(&config, &db, project_id, version).await?);
                    db.set_project_status_guarded(project_id, version, ProjectStatus::Compiling)
                        .await?;
                }
            }
        }
    }
}

#[derive(Eq, PartialEq)]
enum Stage {
    Sql,
    Rust,
}

struct CompilationJob {
    stage: Stage,
    project_id: ProjectId,
    version: Version,
    compiler_process: Child,
}

impl CompilationJob {
    fn is_sql(&self) -> bool {
        self.stage == Stage::Sql
    }

    fn is_rust(&self) -> bool {
        self.stage == Stage::Rust
    }

    async fn sql(
        config: &CompilerConfig,
        db: &ProjectDB,
        project_id: ProjectId,
        version: Version,
    ) -> AnyResult<Self> {
        // Read code from DB (we assume that the DB is locked by the caller,
        // so no need for a version check).
        let (_version, code) = db.project_code(project_id).await?;

        // Create project directory.
        let sql_file_path = config.sql_file_path(project_id);
        fs::create_dir_all(sql_file_path.parent().unwrap()).await?;

        // Write SQL code to file.
        fs::write(&sql_file_path, code).await?;

        let rust_file_path = config.rust_program_path(project_id);
        fs::create_dir_all(rust_file_path.parent().unwrap()).await?;

        let err_file = File::create(config.stderr_path(project_id)).await?;
        let rust_file = File::create(rust_file_path).await?;

        // Run compiler, direct output to lib.rs, direct stderr to file.
        let compiler_process = Command::new(config.sql_compiler_path())
            .arg(sql_file_path.as_os_str())
            .arg("-i")
            .stdin(Stdio::null())
            .stderr(Stdio::from(err_file.into_std().await))
            .stdout(Stdio::from(rust_file.into_std().await))
            .spawn()?;

        Ok(Self {
            stage: Stage::Sql,
            project_id,
            version,
            compiler_process,
        })
    }

    async fn rust(
        config: &CompilerConfig,
        project_id: ProjectId,
        version: Version,
    ) -> AnyResult<Self> {
        // Write `project/Cargo.toml`.
        let template_toml = fs::read_to_string(&config.project_toml_template_path()).await?;
        let project_name = format!("name = \"{project_id}\"");
        let project_toml_code = template_toml
            .replace("name = \"temp\"", &project_name)
            .replace("../lib", config.sql_lib_path().to_str().unwrap());

        fs::write(&config.project_toml_path(project_id), project_toml_code).await?;

        // Write `Cargo.toml`.
        let workspace_toml_code = format!("[workspace]\n members = [\"{project_id}\"]");

        fs::write(&config.workspace_toml_path(), workspace_toml_code).await?;

        let err_file = File::create(&config.stderr_path(project_id)).await?;
        let out_file = File::create(&config.stdout_path(project_id)).await?;

        // Run cargo, direct stdout and stderr to the same file.
        let compiler_process = Command::new("cargo")
            .arg("build")
            .arg("--release")
            .arg("--workspace")
            .stdin(Stdio::null())
            .stderr(Stdio::from(err_file.into_std().await))
            .stdout(Stdio::from(out_file.into_std().await))
            .spawn()?;

        Ok(Self {
            stage: Stage::Rust,
            project_id,
            version,
            compiler_process,
        })
    }

    async fn wait(&mut self) -> AnyResult<ExitStatus> {
        let exit_status = self.compiler_process.wait().await?;
        Ok(exit_status)
        // doesn't update status
    }

    async fn error_output(&self, config: &CompilerConfig) -> AnyResult<String> {
        let output = match self.stage {
            Stage::Sql => fs::read_to_string(config.stderr_path(self.project_id)).await?,
            Stage::Rust => {
                let stdout = fs::read_to_string(config.stdout_path(self.project_id)).await?;
                let stderr = fs::read_to_string(config.stderr_path(self.project_id)).await?;
                format!("stdout:\n{stdout}\nstderr:\n{stderr}")
            }
        };

        Ok(output)
    }

    async fn cancel(&mut self) {
        let _ = self.compiler_process.kill().await;
    }
}

/*enum CompilerCommand {
    Enqueue(ProjectId, Version),
    Cancel(ProjectId, Version),
}*/

impl Drop for Compiler {
    fn drop(&mut self) {
        self.compiler_task.abort();
    }
}
