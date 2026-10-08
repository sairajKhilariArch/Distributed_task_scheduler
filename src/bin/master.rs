use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use clap::Parser;
use distributed_scheduler::{
    RegisterWorkerRequest, SubmitTaskRequest, Task, TaskResultRequest, TaskStatus,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "8080")]
    port: u16,
}

#[derive(Default)]
struct MasterState {
    tasks: HashMap<Uuid, Task>,
    workers: HashMap<String, std::time::Instant>,
}

type SharedState = Arc<RwLock<MasterState>>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let state: SharedState = Arc::new(RwLock::new(MasterState::default()));

    // Background task to clean up dead workers
    let state_clone = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            let mut state = state_clone.write().await;
            let now = std::time::Instant::now();
            let mut dead_workers = vec![];

            for (worker_id, last_hb) in &state.workers {
                if now.duration_since(*last_hb).as_secs() > 15 {
                    dead_workers.push(worker_id.clone());
                }
            }

            for worker in dead_workers {
                warn!("Worker {} timed out and was removed.", worker);
                state.workers.remove(&worker);

                for task in state.tasks.values_mut() {
                    if let Some(w) = &task.assigned_worker {
                        if w == &worker && matches!(task.status, TaskStatus::Running) {
                            warn!("Re-queuing task {} due to worker failure.", task.id);
                            task.status = TaskStatus::Pending;
                            task.assigned_worker = None;
                            task.updated_at = Utc::now();
                        }
                    }
                }
            }
        }
    });

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/tasks", post(submit_task).get(list_tasks))
        .route("/workers/register", post(register_worker))
        .route("/workers/heartbeat", post(worker_heartbeat))
        .route("/workers/poll", get(poll_task))
        .route("/tasks/:id/result", post(task_result))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();
    info!("Master server running on http://127.0.0.1:{}", args.port);
    axum::serve(listener, app).await.unwrap();
}

// HTML Dashboard route
async fn dashboard() -> Html<&'static str> {
    Html(
        r#"
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Distributed Task Scheduler Dashboard</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-900 text-gray-100 font-sans p-6 md:p-10">
        <div class="max-w-5xl mx-auto">
            <header class="flex flex-col md:flex-row justify-between items-start md:items-center mb-8 border-b border-gray-800 pb-5 gap-4">
                <div>
                    <h1 class="text-3xl font-extrabold text-indigo-400 tracking-tight">⚡ Distributed Task Scheduler</h1>
                    <p class="text-gray-400 text-sm mt-1">Master Coordinator Control Center (Rust + Tokio + Axum)</p>
                </div>
                <div class="flex items-center gap-3">
                    <span class="flex h-3 w-3 relative">
                      <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                      <span class="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
                    </span>
                    <span class="text-xs font-semibold text-green-400 uppercase tracking-wider">Live System</span>
                    <button onclick="fetchTasks()" class="ml-4 bg-indigo-600 hover:bg-indigo-500 text-white px-4 py-2 rounded-lg text-sm font-medium transition shadow-lg shadow-indigo-600/20">Refresh Now</button>
                </div>
            </header>

            <div class="bg-gray-800 rounded-xl shadow-2xl overflow-hidden border border-gray-700">
                <div class="p-4 bg-gray-800/80 border-b border-gray-700 flex justify-between items-center">
                    <h2 class="text-lg font-semibold text-gray-200">Task Execution Queue</h2>
                    <span class="text-xs text-gray-400">Auto-refreshes every 3 seconds</span>
                </div>
                <div class="overflow-x-auto">
                    <table class="w-full text-left border-collapse">
                        <thead>
                            <tr class="bg-gray-900/50 text-gray-400 text-xs uppercase tracking-wider">
                                <th class="p-4">Task ID</th>
                                <th class="p-4">Payload</th>
                                <th class="p-4">Status</th>
                                <th class="p-4">Assigned Worker</th>
                                <th class="p-4">Updated At</th>
                            </tr>
                        </thead>
                        <tbody id="task-table" class="divide-y divide-gray-700/60 text-sm">
                            <tr><td colspan="5" class="p-6 text-center text-gray-500">Loading tasks...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <script>
            async function fetchTasks() {
                try {
                    const res = await fetch('/tasks');
                    const tasks = await res.json();
                    const tbody = document.getElementById('task-table');
                    tbody.innerHTML = '';
                    
                    if (tasks.length === 0) {
                        tbody.innerHTML = `<tr><td colspan="5" class="p-8 text-center text-gray-500">No tasks in queue yet. Submit some using your CLI client!</td></tr>`;
                        return;
                    }

                    // Sort tasks by updated_at descending
                    tasks.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));

                    tasks.forEach(t => {
                        let statusBadge = '';
                        if (t.status === 'Pending') {
                            statusBadge = '<span class="px-2.5 py-1 bg-yellow-500/10 text-yellow-400 border border-yellow-500/20 rounded-full text-xs font-semibold">Pending</span>';
                        } else if (t.status === 'Running') {
                            statusBadge = '<span class="px-2.5 py-1 bg-blue-500/10 text-blue-400 border border-blue-500/20 rounded-full text-xs font-semibold animate-pulse">Running</span>';
                        } else if (t.status.Completed) {
                            statusBadge = `<span class="px-2.5 py-1 bg-green-500/10 text-green-400 border border-green-500/20 rounded-full text-xs font-semibold" title="${t.status.Completed.result}">Completed ✓</span>`;
                        } else if (t.status.Failed) {
                            statusBadge = `<span class="px-2.5 py-1 bg-red-500/10 text-red-400 border border-red-500/20 rounded-full text-xs font-semibold" title="${t.status.Failed.error}">Failed ✕</span>`;
                        }

                        const timeStr = new Date(t.updated_at).toLocaleTimeString();
                        
                        tbody.innerHTML += `
                            <tr class="hover:bg-gray-700/30 transition">
                                <td class="p-4 font-mono text-xs text-indigo-300">${t.id.slice(0, 8)}...</td>
                                <td class="p-4 font-medium text-gray-100">${t.payload}</td>
                                <td class="p-4">${statusBadge}</td>
                                <td class="p-4 text-gray-300 font-mono text-xs">${t.assigned_worker || '<span class="text-gray-500 italic">Unassigned</span>'}</td>
                                <td class="p-4 text-gray-400 text-xs">${timeStr}</td>
                            </tr>
                        `;
                    });
                } catch (e) {
                    console.error("Failed to fetch tasks", e);
                }
            }

            fetchTasks();
            setInterval(fetchTasks, 3000);
        </script>
    </body>
    </html>
    "#,
    )
}

async fn submit_task(
    State(state): State<SharedState>,
    Json(payload): Json<SubmitTaskRequest>,
) -> (StatusCode, Json<Task>) {
    let task = Task::new(payload.payload);
    let mut state = state.write().await;
    state.tasks.insert(task.id, task.clone());
    info!("Task submitted: {}", task.id);
    (StatusCode::CREATED, Json(task))
}

async fn list_tasks(State(state): State<SharedState>) -> Json<Vec<Task>> {
    let state = state.read().await;
    let tasks: Vec<Task> = state.tasks.values().cloned().collect();
    Json(tasks)
}

async fn register_worker(
    State(state): State<SharedState>,
    Json(payload): Json<RegisterWorkerRequest>,
) -> StatusCode {
    let mut state = state.write().await;
    state
        .workers
        .insert(payload.worker_id.clone(), std::time::Instant::now());
    info!("Worker registered: {}", payload.worker_id);
    StatusCode::OK
}

#[derive(serde::Deserialize)]
struct HeartbeatQuery {
    worker_id: String,
}

async fn worker_heartbeat(
    State(state): State<SharedState>,
    Json(payload): Json<HeartbeatQuery>,
) -> StatusCode {
    let mut state = state.write().await;
    if state.workers.contains_key(&payload.worker_id) {
        state
            .workers
            .insert(payload.worker_id, std::time::Instant::now());
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn poll_task(
    State(state): State<SharedState>,
    Query(params): Query<HeartbeatQuery>,
) -> (StatusCode, Json<Option<Task>>) {
    let mut state = state.write().await;

    if state.workers.contains_key(&params.worker_id) {
        state
            .workers
            .insert(params.worker_id.clone(), std::time::Instant::now());
    } else {
        return (StatusCode::UNAUTHORIZED, Json(None));
    }

    let pending_task = state
        .tasks
        .values_mut()
        .find(|t| matches!(t.status, TaskStatus::Pending));

    if let Some(task) = pending_task {
        task.status = TaskStatus::Running;
        task.assigned_worker = Some(params.worker_id);
        task.updated_at = Utc::now();
        info!("Assigned task {} to worker", task.id);
        (StatusCode::OK, Json(Some(task.clone())))
    } else {
        (StatusCode::OK, Json(None))
    }
}

async fn task_result(
    State(state): State<SharedState>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
    Json(payload): Json<TaskResultRequest>,
) -> StatusCode {
    let mut state = state.write().await;
    if let Some(task) = state.tasks.get_mut(&id) {
        task.status = payload.status;
        task.updated_at = Utc::now();
        info!("Task {} reported result by {}", id, payload.worker_id);
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}
