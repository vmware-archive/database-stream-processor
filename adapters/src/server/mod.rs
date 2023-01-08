use crate::{Catalog, Controller, ControllerConfig, ControllerError};
use actix_web::{
    dev::{Server, ServiceFactory, ServiceRequest},
    get,
    middleware::Logger,
    rt, web,
    web::Data as WebData,
    App, Error as ActixError, HttpResponse, HttpServer, Responder,
};
use actix_web_static_files::ResourceFiles;
use anyhow::{Error as AnyError, Result as AnyResult};
use clap::Parser;
use dbsp::DBSPHandle;
use env_logger::Env;
use log::{error, info};
use std::{net::TcpListener, sync::Mutex};
use tokio::{
    spawn,
    sync::mpsc::{channel, Receiver, Sender},
};

// TODO:
//
// - grafana

struct ServerState {
    metadata: String,
    controller: Mutex<Option<Controller>>,
    terminate_sender: Option<Sender<()>>,
}

impl ServerState {
    fn new(controller: Controller, meta: String, terminate_sender: Option<Sender<()>>) -> Self {
        Self {
            metadata: meta,
            controller: Mutex::new(Some(controller)),
            terminate_sender,
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Pipeline configuration YAML file
    #[arg(short, long)]
    config_file: String,

    /// Pipeline metadata JSON file
    #[arg(short, long)]
    metadata_file: Option<String>,

    /// Run the server on this port if it is available. If the port is in
    /// use or no default port is specified, an unused TCP port is allocated
    /// automatically
    #[arg(short = 'p', long)]
    default_port: Option<u16>,
}

pub fn server_main<F>(circuit_factory: &F) -> AnyResult<()>
where
    F: Fn(usize) -> (DBSPHandle, Catalog),
{
    // Create env logger.
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    server_main_inner(circuit_factory).map_err(|e| {
        error!("{e}");
        e
    })
}

pub fn server_main_inner<F>(circuit_factory: &F) -> AnyResult<()>
where
    F: Fn(usize) -> (DBSPHandle, Catalog),
{
    let args = Args::try_parse()?;

    let yaml_config = std::fs::read(&args.config_file)?;
    let yaml_config = String::from_utf8(yaml_config)?;

    let meta = match args.metadata_file {
        None => String::new(),
        Some(metadata_file) => {
            let meta = std::fs::read(metadata_file)?;
            String::from_utf8(meta)?
        }
    };

    run_server(circuit_factory, &yaml_config, meta, args.default_port)?;

    Ok(())
}

pub fn run_server<F>(
    circuit_factory: &F,
    yaml_config: &str,
    meta: String,
    default_port: Option<u16>,
) -> AnyResult<()>
where
    F: Fn(usize) -> (DBSPHandle, Catalog),
{
    let (port, server, mut terminate_receiver) =
        create_server(circuit_factory, yaml_config, meta, default_port)
            .map_err(|e| AnyError::msg(format!("Failed to create server: {e}")))?;

    info!("Started HTTP server on port {port}");

    rt::System::new().block_on(async {
        // Spawn a task that will shutdown the server on `/kill`.
        let server_handle = server.handle();
        spawn(async move {
            terminate_receiver.recv().await;
            server_handle.stop(true).await
        });

        server.await
    })?;
    Ok(())
}

pub fn create_server<F>(
    circuit_factory: &F,
    yaml_config: &str,
    meta: String,
    default_port: Option<u16>,
) -> AnyResult<(u16, Server, Receiver<()>)>
where
    F: Fn(usize) -> (DBSPHandle, Catalog),
{
    let config: ControllerConfig = serde_yaml::from_str(yaml_config)
        .map_err(|e| AnyError::msg(format!("error parsing pipeline configuration: {e}")))?;

    let (circuit, catalog) = circuit_factory(config.global.workers as usize);

    let controller = Controller::with_config(
        circuit,
        catalog,
        &config,
        Box::new(|e| error!("{e}")) as Box<dyn Fn(ControllerError) + Send + Sync>,
    )?;

    let listener = match default_port {
        Some(port) => TcpListener::bind(("127.0.0.1", port))
            .or_else(|_| TcpListener::bind(("127.0.0.1", 0)))?,
        None => TcpListener::bind(("127.0.0.1", 0))?,
    };

    let port = listener.local_addr()?.port();

    let (terminate_sender, terminate_receiver) = channel(1);
    let state = WebData::new(ServerState::new(controller, meta, Some(terminate_sender)));
    let server =
        HttpServer::new(move || build_app(App::new().wrap(Logger::default()), state.clone()))
            .workers(1)
            .listen(listener)?
            .run();

    Ok((port, server, terminate_receiver))
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

    app.app_data(state)
        .route(
            "/",
            web::get().to(move || {
                let index_data = index_data.clone();
                async { HttpResponse::Ok().body(index_data) }
            }),
        )
        .service(ResourceFiles::new("/static", generated))
        .service(start)
        .service(pause)
        .service(shutdown)
        .service(status)
        .service(metadata)
        .service(kill)
}

#[get("/start")]
async fn start(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.start();
            HttpResponse::Ok().body("The pipeline is running")
        }
        None => HttpResponse::Conflict().body("The pipeline has been terminated"),
    }
}

#[get("/pause")]
async fn pause(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            controller.pause();
            HttpResponse::Ok().body("Pipeline paused")
        }
        None => HttpResponse::Conflict().body("The pipeline has been terminated"),
    }
}

#[get("/status")]
async fn status(state: WebData<ServerState>) -> impl Responder {
    match &*state.controller.lock().unwrap() {
        Some(controller) => {
            let json_string = serde_json::to_string(controller.status()).unwrap();
            HttpResponse::Ok()
                .content_type(mime::APPLICATION_JSON)
                .body(json_string)
        }
        None => HttpResponse::Conflict().body("The pipeline has been terminated"),
    }
}

#[get("/metadata")]
async fn metadata(state: WebData<ServerState>) -> impl Responder {
    HttpResponse::Ok()
        .content_type(mime::APPLICATION_JSON)
        .body(state.metadata.clone())
}

#[get("/shutdown")]
async fn shutdown(state: WebData<ServerState>) -> impl Responder {
    let controller = state.controller.lock().unwrap().take();
    if let Some(controller) = controller {
        match controller.stop() {
            Ok(()) => HttpResponse::Ok().body("Pipeline terminated"),
            Err(e) => HttpResponse::InternalServerError()
                .body(format!("Failed to terminate the pipeline: {e}")),
        }
    } else {
        HttpResponse::Ok().body("Pipeline already terminated")
    }
}

#[get("/kill")]
async fn kill(state: WebData<ServerState>) -> impl Responder {
    if let Some(sender) = &state.terminate_sender {
        let _ = sender.send(()).await;
    }
    HttpResponse::Ok()
}

#[cfg(test)]
#[cfg(feature = "with-kafka")]
mod test_with_kafka {
    use super::{build_app, ServerState};
    use crate::{
        test::{
            generate_test_batches,
            kafka::{BufferConsumer, KafkaResources, TestProducer},
            test_circuit, wait, TEST_LOGGER,
        },
        Controller, ControllerConfig, ControllerError,
    };
    use actix_web::{http::StatusCode, middleware::Logger, test, web::Data as WebData, App};
    use crossbeam::queue::SegQueue;
    use log::{error, LevelFilter};
    use proptest::{
        strategy::{Strategy, ValueTree},
        test_runner::TestRunner,
    };
    use std::{sync::Arc, thread::sleep, time::Duration};

    #[actix_web::test]
    async fn test_server() {
        // We cannot use proptest macros in `async` context, so generate
        // some random data manually.
        let mut runner = TestRunner::default();
        let data = generate_test_batches(100, 1000)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        let _ = log::set_logger(&TEST_LOGGER);
        log::set_max_level(LevelFilter::Debug);

        // Create topics.
        let kafka_resources = KafkaResources::create_topics(&[
            ("test_server_input_topic", 1),
            ("test_server_output_topic", 1),
        ]);

        // Create buffer consumer
        let buffer_consumer = BufferConsumer::new("test_server_output_topic");

        // Config string
        let config_str = format!(
            r#"
inputs:
    test_input1:
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                auto.offset.reset: "earliest"
                topics: [test_server_input_topic]
                log_level: debug
        format:
            name: csv
            config:
                input_stream: test_input1
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: kafka
            config:
                bootstrap.servers: "localhost"
                topic: test_server_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#
        );

        // Create circuit
        println!("Creating circuit");
        let (circuit, catalog) = test_circuit(4);

        let errors = Arc::new(SegQueue::new());
        let errors_clone = errors.clone();

        let config: ControllerConfig = serde_yaml::from_str(&config_str).unwrap();
        let controller = Controller::with_config(
            circuit,
            catalog,
            &config,
            Box::new(move |e| {
                error!("{e}");
                errors_clone.push(e);
            }) as Box<dyn Fn(ControllerError) + Send + Sync>,
        )
        .unwrap();

        // Create service
        println!("Creating HTTP server");
        let state = WebData::new(ServerState::new(controller, "metadata".to_string(), None));
        let app =
            test::init_service(build_app(App::new().wrap(Logger::default()), state.clone())).await;

        // Write data to Kafka.
        println!("Send test data");
        let producer = TestProducer::new();
        producer.send_to_topic(&data, "test_server_input_topic");

        sleep(Duration::from_millis(2000));
        assert!(buffer_consumer.is_empty());

        // Start command; wait for data.
        println!("/start");
        let req = test::TestRequest::get().uri("/start").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("/status");
        let req = test::TestRequest::get().uri("/status").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        println!("/metadata");
        let req = test::TestRequest::get().uri("/metadata").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        // Pause command; send more data, receive none.
        println!("/pause");
        let req = test::TestRequest::get().uri("/pause").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        sleep(Duration::from_millis(1000));

        producer.send_to_topic(&data, "test_server_input_topic");
        sleep(Duration::from_millis(2000));
        assert_eq!(buffer_consumer.len(), 0);

        // Start; wait for data
        println!("/start");
        let req = test::TestRequest::get().uri("/start").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        buffer_consumer.wait_for_output_unordered(&data);
        buffer_consumer.clear();

        println!("Testing invalid input");
        producer.send_string("invalid\n", "test_server_input_topic");
        wait(|| errors.len() == 1, None);

        // Shutdown
        println!("/shutdown");
        let req = test::TestRequest::get().uri("/shutdown").to_request();
        let resp = test::call_service(&app, req).await;
        // println!("Response: {resp:?}");
        assert!(resp.status().is_success());

        // Start after shutdown must fail.
        println!("/start");
        let req = test::TestRequest::get().uri("/start").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        drop(buffer_consumer);
        drop(kafka_resources);
    }
}
