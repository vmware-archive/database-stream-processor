use actix_files as fs;
use actix_files::NamedFile;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest},
    get,
    http::header::{CacheControl, CacheDirective},
    middleware::Logger,
    post, web,
    web::Data as WebData,
    App, Error as ActixError, HttpRequest, HttpResponse, HttpServer, Responder,
    Result as ActixResult,
};
use anyhow::{Error as AnyError, Result as AnyResult};
use clap::Parser;
use log::{LevelFilter, Log, Metadata, Record};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::{
    fs::{canonicalize, read},
    sync::Mutex,
};

mod compiler;
mod db;

const fn default_server_port() -> u16 {
    8080
}

fn default_pg_connection_string() -> String {
    "host=localhost user=dbsp".to_string()
}

fn default_working_directory() -> String {
    ".".to_string()
}

pub use compiler::Compiler;
pub use db::{ProjectDB, ProjectId, Version};

#[derive(Deserialize, Clone)]
pub(self) struct ServerConfig {
    #[serde(default = "default_server_port")]
    port: u16,
    #[serde(default = "default_pg_connection_string")]
    pg_connection_string: String,
    #[serde(default = "default_working_directory")]
    working_directory: String,
    sql_compiler_home: String,
}

impl ServerConfig {
    async fn canonicalize(self) -> AnyResult<Self> {
        let mut result = self.clone();
        result.working_directory = canonicalize(result.working_directory)
            .await?
            .to_string_lossy()
            .into_owned();
        result.sql_compiler_home = canonicalize(result.sql_compiler_home)
            .await?
            .to_string_lossy()
            .into_owned();

        Ok(result)
    }
}

#[derive(Serialize, Eq, PartialEq)]
pub enum ProjectStatus {
    None,
    Pending,
    Compiling,
    Success,
    SqlError(String),
    RustError(String),
}

// FIXME: Use a real logger.
pub struct TestLogger;
pub static TEST_LOGGER: TestLogger = TestLogger;

impl Log for TestLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        println!("{} - {}", record.level(), record.args());
    }

    fn flush(&self) {}
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server configuration YAML file
    #[arg(short, long)]
    config_file: Option<String>,
}

#[actix_web::main]
async fn main() -> AnyResult<()> {
    let _ = log::set_logger(&TEST_LOGGER);
    log::set_max_level(LevelFilter::Debug);

    let args = Args::try_parse()?;
    let config_file = &args
        .config_file
        .unwrap_or_else(|| "config.yaml".to_string());
    let config_yaml = read(config_file)
        .await
        .map_err(|e| AnyError::msg(format!("error reading config file '{config_file}': {e}")))?;
    let config_yaml = String::from_utf8_lossy(&config_yaml);
    let config: ServerConfig = serde_yaml::from_str(&config_yaml)
        .map_err(|e| AnyError::msg(format!("error parsing config file '{config_file}': {e}")))?;
    let config = config.canonicalize().await?;

    run(config).await
}

struct ServerState {
    db: Arc<Mutex<ProjectDB>>,
    _compiler: Compiler,
}

impl ServerState {
    fn new(db: Arc<Mutex<ProjectDB>>, compiler: Compiler) -> Self {
        Self {
            db,
            _compiler: compiler,
        }
    }
}

async fn run(config: ServerConfig) -> AnyResult<()> {
    let db = Arc::new(Mutex::new(ProjectDB::connect(&config).await?));
    let compiler = Compiler::new(&config, db.clone());

    let state = WebData::new(ServerState::new(db, compiler));

    HttpServer::new(move || build_app(App::new().wrap(Logger::default()), state.clone()))
        .bind(("127.0.0.1", config.port))?
        .run()
        .await?;

    Ok(())
}

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    app.app_data(state)
        .route("/", web::get().to(index))
        .route("/index.html", web::get().to(index))
        .service(fs::Files::new("/static", "static").show_files_listing())
        .service(list_projects)
        .service(project_code)
        .service(project_status)
        .service(new_project)
        .service(update_project)
        .service(compile_project)
}

async fn index() -> ActixResult<NamedFile> {
    Ok(NamedFile::open("static/index.html")?)
}

#[derive(Serialize)]
struct ProjectDescr {
    project_id: ProjectId,
    name: String,
    version: Version,
}

#[get("/list_projects")]
async fn list_projects(state: WebData<ServerState>) -> impl Responder {
    match state.db.lock().await.list_projects().await {
        Ok(projects) => {
            let project_list = projects
                .into_iter()
                .map(|(project_id, (name, version))| ProjectDescr {
                    project_id,
                    name,
                    version,
                })
                .collect::<Vec<_>>();
            let json_string = serde_json::to_string(&project_list).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to retrieve project list: {e}")),
    }
}

#[derive(Serialize)]
struct ProjectCodeResponse {
    version: Version,
    code: String,
}

#[get("/project_code/{project_id}")]
async fn project_code(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let project_id = match req.match_info().get("project_id") {
        None => {
            return HttpResponse::BadRequest().body("missing project id argument");
        }
        Some(project_id) => match project_id.parse::<ProjectId>() {
            Err(e) => {
                return HttpResponse::BadRequest()
                    .body(format!("invalid project id '{project_id}': {e}"));
            }
            Ok(project_id) => project_id,
        },
    };

    match state.db.lock().await.project_code(project_id).await {
        Ok((version, code)) => {
            let json_string =
                serde_json::to_string(&ProjectCodeResponse { version, code }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to retrieve project code: {e}")),
    }
}

#[derive(Serialize)]
struct ProjectStatusResponse {
    version: Version,
    status: ProjectStatus,
}

#[get("/project_status/{project_id}")]
async fn project_status(state: WebData<ServerState>, req: HttpRequest) -> impl Responder {
    let project_id = match req.match_info().get("project_id") {
        None => {
            return HttpResponse::BadRequest().body("missing project id argument");
        }
        Some(project_id) => match project_id.parse::<ProjectId>() {
            Err(e) => {
                return HttpResponse::BadRequest()
                    .body(format!("invalid project id '{project_id}': {e}"));
            }
            Ok(project_id) => project_id,
        },
    };

    match state.db.lock().await.project_status(project_id).await {
        Ok(Some((version, status))) => {
            let json_string =
                serde_json::to_string(&ProjectStatusResponse { version, status }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Ok(None) => {
            HttpResponse::BadRequest().body(format!("project id {project_id} does not exist"))
        }
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to retrieve project status: {e}")),
    }
}

#[derive(Deserialize)]
struct NewProjectRequest {
    name: String,
    code: String,
}

#[derive(Serialize)]
struct NewProjectResponse {
    project_id: ProjectId,
    version: Version,
}

// curl -X POST http://localhost:8080/new_project  -H 'Content-Type: application/json' -d '{"name":"my_name","code":"my_code"}'
#[post("/new_project")]
async fn new_project(
    state: WebData<ServerState>,
    request: web::Json<NewProjectRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .new_project(&request.name, &request.code)
        .await
    {
        Ok((project_id, version)) => {
            let json_string = serde_json::to_string(&NewProjectResponse {
                project_id,
                version,
            })
            .unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("failed to create project: {e}"))
        }
    }
}

#[derive(Deserialize)]
struct UpdateProjectRequest {
    project_id: ProjectId,
    name: String,
    code: Option<String>,
}

#[derive(Serialize)]
struct UpdateProjectResponse {
    version: Version,
}

// curl -X POST http://localhost:8080/update_project -H 'Content-Type: application/json' -d '{"project_id"=2,"name":"my_name2","code":"my_code2"}'
#[post("/update_project")]
async fn update_project(
    state: WebData<ServerState>,
    request: web::Json<UpdateProjectRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .update_project(request.project_id, &request.name, &request.code)
        .await
    {
        Ok(version) => {
            let json_string = serde_json::to_string(&UpdateProjectResponse { version }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("failed to update project: {e}"))
        }
    }
}

#[derive(Deserialize)]
struct CompileProjectRequest {
    project_id: ProjectId,
    version: Version,
}

#[post("/compile_project")]
async fn compile_project(
    state: WebData<ServerState>,
    request: web::Json<CompileProjectRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .set_project_pending(request.project_id, request.version)
        .await
    {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to queue project for compilation: {e}")),
    }
}

#[derive(Deserialize)]
struct CancelProjectRequest {
    project_id: ProjectId,
    version: Version,
}

#[post("/cancel_project")]
async fn cancel_project(
    state: WebData<ServerState>,
    request: web::Json<CancelProjectRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .cancel_project(request.project_id, request.version)
        .await
    {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("failed to cancel compilation: {e}"))
        }
    }
}

/*
   /list_pipelines -> list of running pipelines, with links to their HTTP endpoints
   /start_pipeline?project_name,config -> pipeline_id
   /shutdown_pipeline?pipeline_id
*/
