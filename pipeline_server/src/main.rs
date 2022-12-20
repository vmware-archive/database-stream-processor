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
use anyhow::Result as AnyResult;
use serde::Deserialize;

mod db;
mod compiler;

pub use db::{DBConfig, ProjectDB, ProjectId};
pub use compiler::{Compiler, CompilerConfig};

struct ServerConfig {
    port: u16,
    compiler_config: CompilerConfig,
    db_config: DBConfig,
}

#[actix_web::main]
async fn main() -> AnyResult<()> {
    let config = ServerConfig {
        port: 8080,
        db_config: DBConfig {
            connection_string: "host=localhost user=dbsp".to_string(),
        },
    };

    run(config).await
}

struct ServerState {
    db: Arc<Mutex<ProjectDB>>,
    compiler: Compiler,
}

impl ServerState {
    fn new(db: ProjectDB, compiler: Compiler) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            compiler,
        }
    }
}

async fn run(config: ServerConfig) -> AnyResult<()> {
    let db = ProjectDB::connect(&config.db_config).await?;
    let compiler = Compiler::new(&config.compiler_config)?;

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
        Ok(code) => HttpResponse::Ok()
            .insert_header(CacheControl(vec![CacheDirective::NoCache]))
            .body(code),
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("failed to retrieve project code: {e}")),
    }
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
        Ok(status) => {
            let json_string = serde_json::to_string(&status).unwrap();
            HttpResponse::Ok()
                .insert_header(CacheControl(vec![CacheDirective::NoCache]))
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
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

// curl -X POST http://localhost:8080/new_project  -H 'Content-Type: application/json' -d '{"name":"my_name","code":"my_code"}'
#[post("/new_project")]
async fn new_project(
    state: WebData<ServerState>,
    request: web::Json<NewProjectRequest>,
) -> impl Responder {
    match state.db.lock().await.new_project(&request.name, &request.code).await {
        Ok(project_id) => HttpResponse::Ok().body(project_id.to_string()),
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
        Ok(()) => HttpResponse::Ok().finish(),
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("failed to update project: {e}"))
        }
    }
}

struct CompileProjectRequest {
    project_id: ProjectId,
    version: Version,
}

#[post("/compile_project")]
async fn compile_project(
    state: WebData<ServerState>,
    request: web::Json<CompileProjectRequest>,
) -> impl Responder {
    let db = state.db.lock().await;
    
    match state
        .db
        .lock()
        .await
        .set_project_pending(request.project_id, &request.version)
        .await
    {
        Ok(()) => HttpResponse::Ok().finish(),
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("failed to queue project for compilation: {e}"))
        }
    }
}

#[post("/cancel_project")]
async fn compile_project(
    state: WebData<ServerState>,
    request: web::Json<CompileProjectRequest>,
) -> impl Responder {
    let db = state.db.lock().await;
    
    match state
        .db
        .lock()
        .await
        .cancel_project(request.project_id, &request.version)
        .await
    {
        Ok(()) => HttpResponse::Ok().finish(),
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
