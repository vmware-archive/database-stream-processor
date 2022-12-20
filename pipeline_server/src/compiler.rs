#[derive(Clone)]
struct CompilerConfig {
    workspace_directory: PathBuf,
    sql_compiler_home: PathBuf,
}

struct Compiler {
    config: CompilerConfig,
    command_sender: Sender<CompilerCommand>,
    compiler_task: JoinHandle,
}

impl Compiler {
    pub fn new(config: &CompilerConfig, db: Arc<ProjectDB>) {
        let (command_sender, command_receiver) = channel(100);

        let compiler_task = spawn(Self::compiler_task(command_receiver, db));
        Self {
            config: config.clone(),
            command_sender,
            compiler_task,
        }
    }

    pub async fn compile(&self, project_id: &ProjectId) -> {
        self.command_sender.try_send()
    }

    fn compiler_task(command_receiver: Receiver<CompilerCommand>, db: Arc<ProjectDB>) {
        let mut job_queue: JobQueue::new(&db);
        let mut job: Option<CompilationJob> = None;

        loop {
            select! {
                command = command_receiver.recv() => {
                    match command {
                        None => {
                            // Channel closed -- cancel running job, exit thread.
                            job.map(CompilationJob::cancel);
                            *job = None;
                            return;
                        }
                        Some(CompilerCommand::Enqueue(project_id, version)) => {
                            // Project already being compiled?
                            //      Same version or newer => noop
                            //      older version =>
                            //          cancel job;
                            //          job_queue.push()
                            // else
                            //          job_queue.push()


                            if job.map(CompilationJob.project_id) != Some(project_id) {
                                if job_queue.push(project_id).is_ok() {
                                    db.set_project_status(project_id, queued);
                                } else {
                                    db.set_project_status(project_id, retry);
                                }
                            }
                        }
                        Some(CompilerCommand::Cancel(project_id, version)) {
                            // Project already being compiled?
                            //      Same version? =>
                            //          cancel job
                            // else
                            //      job_queue.cancel(project_id, version)
                            if job.map(CompilationJob::project_id) == Some(project_id) {
                                job.map(CompilationJob::cancel);
                                *job = None;
                            } else {
                                job_queue.cancel(project_id);
                            }
                            db.set_project_status(project_id, None);
                        }
                    }
                },
                exit_status = job.unwrap().wait(), if job.is_some() => {
                    match exit_status {
                        Ok((status, _output)) if status.success() && sql => {
                            // SQL compiler succeeded -- start Rust job.
                            *job = Some(CompilationJob::rust());
                        }
                        Ok((status, _output)) if status.success() && job.unwrap().is_rust() => {
                            // Rust compiler succeeded -- declare victory.
                            db.set_project_status_guarded(project_id, job.version, Success);
                            *job = None;
                        }
                        Ok((status, output)) => {
                            let status = if job.unwrap().is_rust() {
                                RustError(format!("{output}\nexit code: {status}"))
                            } else {
                                SqlError(format!("{output}\nexit code: {status}"))
                            };
                            // change project status to error
                            db.set_project_status_guarded(project_id, job.version, status);
                            *job = None;
                        }
                        Err(e) => {
                            let status = if job.unwrap().is_rust() {
                                RustError(format!("I/O error: {e}"))
                            } else {
                                SqlError(format!("I/O error: {e}"))
                            };
                            // change project status to error
                            db.set_project_status_guarded(project_id, job.version, status);
                            *job = None;
                        }
                    }
                },
            }

            if job.is_none() {
                if let Some(project_id) = job_queue.pop() {
                    job = Some(CompilationJob::sql(project_id));
                    db.set_project_status(project_id, inprogress);
                }
            }
        }
    }
}

struct JobQueue {
    db: Arc<ProjectDB>,
    queue: VecDeque<(ProjectId, Version)>,
}

impl JobQueue {
    fn new(db: &Arc<ProjectDB>) -> Self {
        Self {
            db: db.clone(),
            queue: VecDeque::new(JOB_QUEUE_CAPACITY)
        }
    }

    fn push(&mut self, project_id: ProjectId, version: Version) -> AnyResult<()> {
        if let Some(existing_index) = self.queue.iter().position(|id, _version| id == project_id) {
            if self.queue[existing_index].1 < version {
                self.queue.remove(existing_index);
                if db.set_project_status_guarded(project_id, version, ProjectStatus::Queued).await? {
                    self.queue.push_back((project_id, version));
                }
            }
        } else {
            if self.queue.len() >= JOB_QUEUE_CAPACITY {
                db.set_project_status_guarded(project_id, version, ProjectStatus::Retry).await?;
            } else {
                if db.set_project_status_guarded(project_id, version, ProjectStatus::Queued).await? {
                    self.queue.push_back((project_id, version));
                }
            }
        };

        Ok(())
    }

    fn pop(&mut self) -> Option<(ProjectId, Version)> {
        self.queue.pop_front()
    }

    fn cancel(&mut self, project_id: ProjectId, version: Version) {
        if let Some(index) = self.queue.iter().position(|id_ver| id_ver == (project_id, version)) {
            db.set_project_status_guarded(project_id, version, ProjectStatus::None).await?;
            self.queue.remove(index);
        }
    }
}
enum Stage {
    Idle,
    Sql {
        project_id: ProjectId,
        version: Version,
        compiler_process: Child,
    },
    Rust {
        project_id: ProjectId,
        version: Version,
        compiler_process: Child,
    },
}


struct CompilationJob {
    stage: Stage,
    db: Arc<ProjectDB>,
    config: CompilerConfig,
}

impl CompilationJob {
    fn new(db: &Arc<ProjectDB>, config: &CompilerConfig) -> Self {
        Self {
            state: Stage::Idle,
            db: db.clone(),
            config: config.clone(),
        }
    }

    fn project_dir(&self, project_id: ProjectId) -> Path {
        Path::new(self.config.workspace_directory).join(project_id.to_string());
    }

    fn sql_file_path(&self, project_id: ProjectId) -> Path {
        self.project_dir(project_id).join("project.sql")
    }

    fn sql(&mut self, project_id: ProjectId, version: Version) -> AnyResult<()> {
        assert!(self.stage.is_idle());

        // Read code from DB.
        let (code, db_version) = self.db.project_code(project_id)?;

        if db_version != version {
            return Ok(());
        }
        if !db.set_project_status_guarded(project_id, version, ProjectStatus::Running) {
            return Ok(());
        }

        // Create project directory.
        let sql_file_path = self.sql_file_path(project_id);
        fs::create_dir_all(sql_file_path.parent().unwrap()).await?;

        // Write SQL code to file
        fs::write(sql_file_path, code).await?;

        // Run compiler, direct output to lib.rs, direct stderr to file.
        let compiler_process = Command::new();

        // Update status on error

        self.stage = State::Sql {
            project_id,
            version,
            compiler_process
        };

        Ok(())
    }
    fn rust(project_id: ProjectId) -> Self {
        // Write `Cargo.toml`.

        // Run cargo, direct stdout and stderr to the same file.


        // Update status on error
    }
    async fn wait(&mut self) -> Result<ExitStatus> {
        self.compiler_process.wait().await;
        // doesn't update status
    }
    fn project_id(&self) -> ProjectId {
        self.project_id
    }
    fn cancel(&mut self) {
        let _ = self.compiler_process.kill();
        self.db.set_project_status_guarded(project_id, version, ProjectStatus::None) {
    }
}

enum CompilerCommand {
    Enqueue(ProjectId, Version),
    Cancel(ProjectId, Version),
}

impl Drop for Compiler {
}

        // Run SQL compiler.

        // Create project folder

        // project/Cargo.toml

        // project/src/lib.rs

        // Cargo.toml

        // cargo build --release --workspace

