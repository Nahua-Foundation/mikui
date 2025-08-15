// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/

use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

// Backend application state/logic holder
#[derive(Default)]
pub struct App {}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ClusterConnectPayload {
    pub name: String,
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keystore_path: Option<String>,
    pub keystore_password: Option<String>,
    pub truststore_path: Option<String>,
    pub truststore_password: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MessageFiltersDto {
    pub key: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartFrom {
    Oldest,
    Newest,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GetMessagesParams {
    pub topic_name: String,
    pub search_query: Option<String>,
    pub filters: Option<MessageFiltersDto>,
    pub partition: Option<i32>,
    pub limit: u32,
    pub start_from: Option<StartFrom>,
    pub offset: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub struct KafkaMessage {
    pub partition: i32,
    pub key: String,
    pub offset: i64,
    pub message: String,
    pub timestamp: String,
    pub headers: std::collections::HashMap<String, String>,
}

impl App {
    // Async implementation with 3-second delay to simulate work without blocking UI
    pub async fn cluster_connect(&self, _payload: ClusterConnectPayload) -> Result<(), String> {
        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    // Async implementation with 3-second delay to simulate work without blocking UI
    pub async fn cluster_test(&self, _payload: ClusterConnectPayload) -> Result<(), String> {
        sleep(Duration::from_secs(3)).await;
        Ok(())
    }

    // Returns a list of topics. For now, mock data with slight delay to simulate fetching.
    pub async fn get_topics(&self) -> Result<Vec<String>, String> {
        sleep(Duration::from_secs(1)).await;
        Ok(vec![
            "orders".to_string(),
            "payments".to_string(),
            "users".to_string(),
            "events".to_string(),
        ])
    }

    // Returns a vector of messages for a topic with basic pagination and filtering (mocked).
    pub async fn get_messages(&self, params: GetMessagesParams) -> Result<Vec<KafkaMessage>, String> {
        // Simulate a short delay as if reading from Kafka
        sleep(Duration::from_millis(300)).await;

        let limit = params.limit.max(1).min(500); // cap to a reasonable number
        let start_from = params.start_from.unwrap_or(StartFrom::Newest);
        let base_partition = params.partition.unwrap_or(0);

        // Generate mock messages deterministically based on topic and offset
        let mut messages: Vec<KafkaMessage> = Vec::with_capacity(limit as usize);
        for i in 0..limit {
            let idx = params.offset + i;
            let (key, body) = (
                format!("key-{}-{}", params.topic_name, idx),
                format!(
                    "message {} for topic '{}'{}{}",
                    idx,
                    params.topic_name,
                    params
                        .search_query
                        .as_ref()
                        .map(|q| format!(" | search:{}", q))
                        .unwrap_or_default(),
                    params
                        .filters
                        .as_ref()
                        .map(|f| {
                            let mut s = String::new();
                            if !f.key.trim().is_empty() { s.push_str(&format!(" | f_key:{}", f.key)); }
                            if !f.message.trim().is_empty() { s.push_str(&format!(" | f_msg:{}", f.message)); }
                            s
                        })
                        .unwrap_or_default()
                ),
            );

            messages.push(KafkaMessage {
                partition: base_partition,
                key,
                offset: match start_from {
                    StartFrom::Oldest => idx as i64,
                    StartFrom::Newest => -((idx as i64) + 1), // negative to indicate reverse in mock
                },
                message: body,
                timestamp: chrono::Utc::now().to_rfc3339(),
                headers: Default::default(),
            });
        }

        Ok(messages)
    }
}

// Web-exposed commands that delegate to App via managed state

#[tauri::command]
async fn cluster_connect(state: tauri::State<'_, App>, payload: ClusterConnectPayload) -> Result<(), String> {
    state.cluster_connect(payload).await
}

#[tauri::command]
async fn cluster_test(state: tauri::State<'_, App>, payload: ClusterConnectPayload) -> Result<(), String> {
    state.cluster_test(payload).await
}

#[tauri::command]
async fn get_topics(state: tauri::State<'_, App>) -> Result<Vec<String>, String> {
    state.get_topics().await
}

#[tauri::command]
async fn get_messages(state: tauri::State<'_, App>, params: GetMessagesParams) -> Result<Vec<KafkaMessage>, String> {
    state.get_messages(params).await
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(App::default())
        .invoke_handler(tauri::generate_handler![cluster_connect, cluster_test, get_topics, get_messages])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
