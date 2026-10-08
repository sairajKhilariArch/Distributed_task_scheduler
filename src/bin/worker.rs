use clap::Parser;
use distributed_scheduler::{RegisterWorkerRequest, TaskResultRequest, TaskStatus};
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    worker_id: String,

    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    master: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let client = Client::new();

    info!("Starting worker: {}", args.worker_id);

    // Register with master
    let register_url = format!("{}/workers/register", args.master);
    loop {
        let res = client
            .post(&register_url)
            .json(&RegisterWorkerRequest {
                worker_id: args.worker_id.clone(),
            })
            .send()
            .await;

        match res {
            Ok(resp) if resp.status().is_success() => {
                info!("Successfully registered with master.");
                break;
            }
            _ => {
                warn!("Failed to register with master. Retrying in 2 seconds...");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }

    // Spawn heartbeat loop
    let master_hb = args.master.clone();
    let worker_id_hb = args.worker_id.clone();
    let client_hb = client.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5)).await;
            let hb_url = format!("{}/workers/heartbeat", master_hb);
            let res = client_hb
                .post(&hb_url)
                .json(&serde_json::json!({ "worker_id": worker_id_hb }))
                .send()
                .await;

            if let Err(e) = res {
                error!("Heartbeat failed: {}", e);
            }
        }
    });

    // Main task polling loop
    let poll_url = format!("{}/workers/poll?worker_id={}", args.master, args.worker_id);
    loop {
        let res = client.get(&poll_url).send().await;

        match res {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(Some(task)) = resp.json::<Option<distributed_scheduler::Task>>().await {
                    info!("Received task {}: {}", task.id, task.payload);

                    // Simulate task execution
                    let execution_result = execute_task(&task.payload).await;

                    let status = match execution_result {
                        Ok(res_str) => TaskStatus::Completed { result: res_str },
                        Err(err_str) => TaskStatus::Failed { error: err_str },
                    };

                    // Report result
                    let result_url = format!("{}/tasks/{}/result", args.master, task.id);
                    let result_payload = TaskResultRequest {
                        worker_id: args.worker_id.clone(),
                        status,
                    };

                    let post_res = client.post(&result_url).json(&result_payload).send().await;
                    match post_res {
                        Ok(r) if r.status().is_success() => {
                            info!("Successfully reported result for task {}", task.id);
                        }
                        _ => {
                            error!("Failed to report result for task {}", task.id);
                        }
                    }
                } else {
                    // No task available, sleep before polling again
                    sleep(Duration::from_secs(2)).await;
                }
            }
            _ => {
                warn!("Error polling for tasks. Retrying in 2 seconds...");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn execute_task(payload: &str) -> Result<String, String> {
    info!("Executing task payload: {}...", payload);
    // Simulate some compute work
    sleep(Duration::from_secs(3)).await;

    if payload.contains("fail") {
        Err("Task explicitly requested failure".into())
    } else {
        Ok(format!("Successfully processed: {}", payload))
    }
}
