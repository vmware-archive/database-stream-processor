use actix_files as fs;
use actix_files::NamedFile;
use actix_web::{
    dev::{ServiceFactory, ServiceRequest},
    get,
    middleware::Logger,
    rt, web,
    web::Data as WebData,
    App, Error as ActixError, HttpResponse, HttpServer, Responder, Result as ActixResult,
};
use anyhow::Result as AnyResult;

mod db;

pub use db::{ProjectDB, DBConfig};

struct ServerConfig {
    port: u16,
    // compiler_config: CompilerConfig,
    db_config: DBConfig,
}

struct CompilerConfig {
}

#[actix_web::main]
async fn main() -> AnyResult<()> {
    let config = ServerConfig {
        port: 8080,
        db_config: DBConfig {
            connection_string: "host=localhost user=dbsp".to_string(),
        }
    };

    run(config).await
}

struct ServerState {
    db: ProjectDB,
    // compiler: Compiler,
}

impl ServerState {
    fn new(db: ProjectDB/*, compiler: Compiler*/) -> Self {
        Self {
            db,
            // compiler,
        }
    }
}

/*
struct Compiler {
    pub fn new(config: &CompilerConfig) -> AnyResult<Self>;
    pub fn compile(&self, project_name: &str, project_code: &str, callback: FnOnce(CompilerResult));
}

impl Drop for Compiler {
}
*/

async fn run(config: ServerConfig) -> AnyResult<()> {

    let db = ProjectDB::connect(&config.db_config).await?;
    // let compiler = Compiler::new(&config.compiler_config)?;

    let state = WebData::new(ServerState::new(db/*, compiler*/));

    HttpServer::new(move || build_app(App::new().wrap(Logger::default()), state.clone()))
        .bind(("127.0.0.1", config.port))?
        .run().await?;

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
}

async fn index() -> ActixResult<NamedFile> {
    Ok(NamedFile::open("static/index.html")?)
}

/*
   /list_projects -> list of projects
   /get_project?project_name -> project_code
   /project_status?project_name -> compilation_status
   /new_project?project_name,project_code
   /update_project?project_name,project_code

   /list_pipelines -> list of running pipelines, with links to their HTTP endpoints
   /start_pipeline?project_name,config -> pipeline_id
   /shutdown_pipeline?pipeline_id
*/
