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
use actix_web_static_files::ResourceFiles;
use anyhow::{Error as AnyError, Result as AnyResult};
use clap::Parser;
use env_logger::Env;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::{fs::read, sync::Mutex};

mod compiler;
mod config;
mod db;
mod runner;

pub(crate) use compiler::Compiler;
pub(crate) use config::ServerConfig;
use db::{ConfigId, PipelineId, ProjectDB, ProjectId, Version};
use runner::Runner;

#[derive(Serialize, Eq, PartialEq)]
pub enum ProjectStatus {
    None,
    Pending,
    Compiling,
    Success,
    SqlError(String),
    RustError(String),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server configuration YAML file
    #[arg(short, long)]
    config_file: Option<String>,

    /// [Developers only] serve static content from the specified directory
    #[arg(short, long)]
    static_html: Option<String>,
}

#[actix_web::main]
async fn main() -> AnyResult<()> {
    // Create env logger.
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = Args::try_parse()?;
    let config_file = &args
        .config_file
        .unwrap_or_else(|| "config.yaml".to_string());
    let config_yaml = read(config_file)
        .await
        .map_err(|e| AnyError::msg(format!("error reading config file '{config_file}': {e}")))?;
    let config_yaml = String::from_utf8_lossy(&config_yaml);
    let mut config: ServerConfig = serde_yaml::from_str(&config_yaml)
        .map_err(|e| AnyError::msg(format!("error parsing config file '{config_file}': {e}")))?;
    if let Some(static_html) = &args.static_html {
        config.static_html = Some(static_html.clone());
    }
    let config = config.canonicalize().await?;

    run(config).await
}

struct ServerState {
    db: Arc<Mutex<ProjectDB>>,
    _compiler: Compiler,
    runner: Runner,
    config: ServerConfig,
}

impl ServerState {
    fn new(config: ServerConfig, db: Arc<Mutex<ProjectDB>>, compiler: Compiler) -> Self {
        let runner = Runner::new(db.clone(), &config);

        Self {
            db,
            _compiler: compiler,
            runner,
            config,
        }
    }
}

async fn run(config: ServerConfig) -> AnyResult<()> {
    let db = Arc::new(Mutex::new(ProjectDB::connect(&config).await?));
    let compiler = Compiler::new(&config, db.clone()).await?;

    db.lock().await.clear_pending_projects().await?;

    let port = config.port;
    let state = WebData::new(ServerState::new(config, db, compiler));

    HttpServer::new(move || build_app(App::new().wrap(Logger::default()), state.clone()))
        .bind(("127.0.0.1", port))?
        .run()
        .await?;

    Ok(())
}

include!(concat!(env!("OUT_DIR"), "/generated.rs"));

fn build_app<T>(app: App<T>, state: WebData<ServerState>) -> App<T>
where
    T: ServiceFactory<ServiceRequest, Config = (), Error = ActixError, InitError = ()>,
{
    let generated = generate();

    let index_data = match generated.get("index.html") {
        None => "<html><head><title>DBSP server</title></head></html>"
            .as_bytes()
            .to_owned(),
        Some(resource) => resource.data.to_owned(),
    };

    let app = app
        .app_data(state.clone())
        .service(list_projects)
        .service(project_code)
        .service(project_status)
        .service(new_project)
        .service(update_project)
        .service(compile_project)
        .service(delete_project)
        .service(new_config)
        .service(update_config)
        .service(delete_config)
        .service(list_project_configs)
        .service(new_pipeline)
        .service(kill_pipeline)
        .service(delete_pipeline)
        .service(list_project_pipelines);

    if let Some(static_html) = &state.config.static_html {
        app.route("/", web::get().to(index))
            .service(fs::Files::new("/static", static_html).show_files_listing())
    } else {
        app.route(
            "/",
            web::get().to(move || {
                let index_data = index_data.clone();
                async { HttpResponse::Ok().body(index_data) }
            }),
        )
        .service(ResourceFiles::new("/static", generated))
    }
}

async fn index() -> ActixResult<NamedFile> {
    Ok(NamedFile::open("static/index.html")?)
}

#[get("/list_projects")]
async fn list_projects(state: WebData<ServerState>) -> impl Responder {
    match state.db.lock().await.list_projects().await {
        Ok(projects) => {
            let json_string = serde_json::to_string(&projects).unwrap();
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

#[derive(Deserialize)]
struct DeleteProjectRequest {
    project_id: ProjectId,
}

#[post("/delete_project")]
async fn delete_project(
    state: WebData<ServerState>,
    request: web::Json<DeleteProjectRequest>,
) -> impl Responder {
    let db = state.db.lock().await;

    match db.list_project_pipelines(request.project_id).await {
        Ok(pipelines) => {
            if pipelines.iter().any(|pipeline| !pipeline.killed) {
                return HttpResponse::BadRequest()
                    .body("cannot delete a project while some of its pipelines are running");
            }
        }
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!(
                "failed to fetch the list of project pipelines: {e}"
            ));
        }
    }

    match db.delete_project(request.project_id).await {
        Ok(true) => HttpResponse::Ok().finish(),
        Ok(false) => {
            HttpResponse::NotFound().body(format!("unknown project id '{}'", request.project_id))
        }
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("failed to delete project: {e}"))
        }
    }
}

#[derive(Deserialize)]
struct NewConfigRequest {
    project_id: ProjectId,
    name: String,
    config: String,
}

#[derive(Serialize)]
struct NewConfigResponse {
    config_id: ConfigId,
    version: Version,
}

#[post("/new_config")]
async fn new_config(
    state: WebData<ServerState>,
    request: web::Json<NewConfigRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .new_config(request.project_id, &request.name, &request.config)
        .await
    {
        Ok((config_id, version)) => {
            let json_string =
                serde_json::to_string(&NewConfigResponse { config_id, version }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to create config: {e}")),
    }
}

#[derive(Deserialize)]
struct UpdateConfigRequest {
    config_id: ConfigId,
    name: String,
    config: Option<String>,
}

#[derive(Serialize)]
struct UpdateConfigResponse {
    version: Version,
}

#[post("/update_config")]
async fn update_config(
    state: WebData<ServerState>,
    request: web::Json<UpdateConfigRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .update_config(request.config_id, &request.name, &request.config)
        .await
    {
        Ok(version) => {
            let json_string = serde_json::to_string(&UpdateConfigResponse { version }).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to update config: {e}")),
    }
}

#[derive(Deserialize)]
struct DeleteConfigRequest {
    config_id: ConfigId,
}

#[post("/delete_config")]
async fn delete_config(
    state: WebData<ServerState>,
    request: web::Json<DeleteConfigRequest>,
) -> impl Responder {
    match state.db.lock().await.delete_config(request.config_id).await {
        Ok(true) => HttpResponse::Ok().finish(),
        Ok(false) => {
            HttpResponse::BadRequest().body(format!("unknown config id '{}'", request.config_id))
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("failed to delete config: {e}")),
    }
}

#[derive(Deserialize)]
struct ListProjectConfigsRequest {
    project_id: ProjectId,
}

#[post("/list_project_configs")]
async fn list_project_configs(
    state: WebData<ServerState>,
    request: web::Json<ListProjectConfigsRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .list_project_configs(request.project_id)
        .await
    {
        Ok(configs) => {
            let json_string = serde_json::to_string(&configs).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to retrieve project configs list: {e}")),
    }
}

#[derive(Deserialize)]
pub(self) struct NewPipelineRequest {
    project_id: ProjectId,
    project_version: Version,
    config_id: ConfigId,
    config_version: Version,
}

#[derive(Serialize)]
struct NewPipelineResponse {
    pipeline_id: PipelineId,
    port: u16,
}

#[post("/new_pipeline")]
async fn new_pipeline(
    state: WebData<ServerState>,
    request: web::Json<NewPipelineRequest>,
) -> impl Responder {
    state
        .runner
        .run_pipeline(&request)
        .await
        .unwrap_or_else(|e| {
            HttpResponse::InternalServerError().body(format!("failed to start pipeline: {e}"))
        })
}

#[derive(Deserialize)]
struct ListProjectPipelinesRequest {
    project_id: ProjectId,
}

#[post("/list_project_pipelines")]
async fn list_project_pipelines(
    state: WebData<ServerState>,
    request: web::Json<ListProjectPipelinesRequest>,
) -> impl Responder {
    match state
        .db
        .lock()
        .await
        .list_project_pipelines(request.project_id)
        .await
    {
        Ok(pipelines) => {
            let json_string = serde_json::to_string(&pipelines).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to retrieve project pipeline list: {e}")),
    }
}

#[derive(Deserialize)]
pub(self) struct KillPipelineRequest {
    pipeline_id: PipelineId,
}

#[post("/kill_pipeline")]
async fn kill_pipeline(
    state: WebData<ServerState>,
    request: web::Json<KillPipelineRequest>,
) -> impl Responder {
    state
        .runner
        .kill_pipeline(request.pipeline_id)
        .await
        .unwrap_or_else(|e| {
            HttpResponse::InternalServerError().body(format!("failed to stop the pipeline: {e}"))
        })
}

#[derive(Deserialize)]
pub(self) struct DeletePipelineRequest {
    pipeline_id: PipelineId,
}

#[post("/delete_pipeline")]
async fn delete_pipeline(
    state: WebData<ServerState>,
    request: web::Json<DeletePipelineRequest>,
) -> impl Responder {
    state
        .runner
        .delete_pipeline(request.pipeline_id)
        .await
        .unwrap_or_else(|e| {
            HttpResponse::InternalServerError().body(format!("failed to delete the pipeline: {e}"))
        })
}
