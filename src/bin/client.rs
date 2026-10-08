use clap::{Parser, Subcommand};
use distributed_scheduler::{SubmitTaskRequest, Task};
use reqwest::Client;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "http://127.0.0.1:8080")]
    master: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Submit a new task with a payload string
    Submit { payload: String },
    /// List all tasks and their current status
    List,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = Client::new();

    match args.command {
        Commands::Submit { payload } => {
            let url = format!("{}/tasks", args.master);
            let res = client
                .post(&url)
                .json(&SubmitTaskRequest { payload })
                .send()
                .await?;

            if res.status().is_success() {
                let task = res.json::<Task>().await?;
                println!("Task submitted successfully!");
                println!("ID: {}", task.id);
                println!("Status: {:?}", task.status);
            } else {
                eprintln!("Failed to submit task: status {}", res.status());
            }
        }
        Commands::List => {
            let url = format!("{}/tasks", args.master);
            let res = client.get(&url).send().await?;

            if res.status().is_success() {
                let tasks = res.json::<Vec<Task>>().await?;
                println!("{:<36} | {:<12} | {:<30} | {:<15}", "ID", "STATUS", "PAYLOAD", "WORKER");
                println!("{}", "-".repeat(100));
                for task in tasks {
                    let status_str = match &task.status {
                        distributed_scheduler::TaskStatus::Pending => "Pending".to_string(),
                        distributed_scheduler::TaskStatus::Running => "Running".to_string(),
                        distributed_scheduler::TaskStatus::Completed { .. } => "Completed".to_string(),
                        distributed_scheduler::TaskStatus::Failed { .. } => "Failed".to_string(),
                    };
                    let worker = task.assigned_worker.unwrap_or_else(|| "None".to_string());
                    println!("{:<36} | {:<12} | {:<30} | {:<15}", task.id, status_str, task.payload, worker);
                }
            } else {
                eprintln!("Failed to list tasks: status {}", res.status());
            }
        }
    }

    Ok(())
}
