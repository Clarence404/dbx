//! InfluxDB 3.x native driver.
//!
//! Supports two transports:
//!
//! * **Flight SQL** (default) — over gRPC via `arrow-flight`, decoding
//!   Arrow `RecordBatch`es into JSON rows. This is InfluxDB 3.x's
//!   native query interface.
//! * **HTTP** (opt-in via `external_config.transport = "http"`) —
//!   over the JSON `/api/v3/query_sql` endpoint. Useful when Flight SQL
//!   is not reachable (e.g. a proxy in the way) or for debugging.
//!
//! Auth is a Bearer token (`config.password`). Target database comes
//! from `config.database` (or `external_config.database`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::{DataType, TimeUnit};
use futures::TryStreamExt;
use reqwest::{Certificate, Client as HttpClient};
use serde::{Deserialize, Serialize};
use std::fs;
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use super::with_connection_timeout;
use crate::models::connection::ConnectionConfig;
use crate::types::{ColumnInfo, DatabaseInfo, QueryResult, TableInfo};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportKind {
    Http,
    FlightSql,
}

pub struct Influxdb3Client {
    transport: TransportKind,
    /// Bearer token — sent as `Authorization: Bearer <token>` (HTTP) or
    /// `authorization: Bearer <token>` metadata (Flight SQL).
    token: Option<String>,
    /// Default target database (bucket). Sent as `db=` in HTTP or as
    /// `database` gRPC metadata in Flight SQL.
    database: Option<String>,
    // HTTP transport fields:
    http: HttpClient,
    /// Normalized base URL for HTTP — `http[s]://host:port`, no trailing slash.
    base_url: String,
    url_params: Option<String>,
    // Flight SQL transport fields:
    /// Pre-configured gRPC endpoint. `None` when transport == Http.
    endpoint: Option<Arc<Endpoint>>,
    /// Cached connected channel; lazily populated on first Flight SQL call.
    channel: Arc<AsyncMutex<Option<Channel>>>,
}

impl Clone for Influxdb3Client {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport,
            token: self.token.clone(),
            database: self.database.clone(),
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            url_params: self.url_params.clone(),
            endpoint: self.endpoint.clone(),
            // Do not share the connected channel across clones — each clone
            // reconnects lazily. Prevents a poisoned channel from
            // propagating across pool users.
            channel: Arc::new(AsyncMutex::new(None)),
        }
    }
}

impl Influxdb3Client {
    pub fn new_for_config(url: &str, config: &ConnectionConfig, timeout: Duration) -> Result<Self, String> {
        let transport = influxdb3_transport(config.external_config.as_ref());
        let database = influxdb3_database(config);
        let token = (!config.password.is_empty()).then_some(config.password.clone());
        let http = build_http_client(Some(&config.ca_cert_path), timeout)?;
        let base_url = url.trim_end_matches('/').to_string();

        let endpoint = match transport {
            TransportKind::FlightSql => {
                Some(Arc::new(build_flight_endpoint(&base_url, timeout, &config.ca_cert_path)?))
            }
            TransportKind::Http => None,
        };

        Ok(Self {
            transport,
            token,
            database,
            http,
            base_url,
            url_params: config.url_params.clone(),
            endpoint,
            channel: Arc::new(AsyncMutex::new(None)),
        })
    }
}

fn influxdb3_transport(external_config: Option<&serde_json::Value>) -> TransportKind {
    match external_config
        .and_then(|value| value.get("transport"))
        .and_then(serde_json::Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("http") => TransportKind::Http,
        _ => TransportKind::FlightSql,
    }
}

fn influxdb3_database(config: &ConnectionConfig) -> Option<String> {
    let explicit = config
        .external_config
        .as_ref()
        .and_then(|value| value.get("database").or_else(|| value.get("db")))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if explicit.is_some() {
        return explicit;
    }
    config.database.as_deref().map(str::trim).filter(|value| !value.is_empty()).map(str::to_string)
}

fn build_http_client(ca_cert_path: Option<&str>, timeout: Duration) -> Result<HttpClient, String> {
    let mut builder = HttpClient::builder().connect_timeout(timeout);
    if let Some(path) = ca_cert_path.map(str::trim).filter(|path| !path.is_empty()) {
        let path = expand_cert_path(path);
        let cert_bytes =
            fs::read(&path).map_err(|e| format!("Failed to read InfluxDB 3 CA certificate at {path}: {e}"))?;
        let cert = Certificate::from_pem(&cert_bytes)
            .or_else(|_| Certificate::from_der(&cert_bytes))
            .map_err(|e| format!("Failed to parse InfluxDB 3 CA certificate at {path}: {e}"))?;
        builder = builder.add_root_certificate(cert);
    }
    builder.build().map_err(|e| format!("Failed to configure InfluxDB 3 HTTP client: {e}"))
}

fn build_flight_endpoint(base_url: &str, timeout: Duration, ca_cert_path: &str) -> Result<Endpoint, String> {
    let is_tls = base_url.starts_with("https://");
    // Flight SQL uses gRPC (h2). The URL scheme http/https drives TLS.
    let mut endpoint = Endpoint::from_shared(base_url.to_string())
        .map_err(|e| format!("Invalid InfluxDB 3 Flight endpoint {base_url}: {e}"))?
        .connect_timeout(timeout)
        .timeout(Duration::from_secs(60))
        .tcp_keepalive(Some(Duration::from_secs(60)));
    if is_tls {
        let mut tls = ClientTlsConfig::new().with_enabled_roots();
        let trimmed = ca_cert_path.trim();
        if !trimmed.is_empty() {
            let path = expand_cert_path(trimmed);
            let cert_bytes =
                fs::read(&path).map_err(|e| format!("Failed to read InfluxDB 3 CA certificate at {path}: {e}"))?;
            tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(cert_bytes));
        }
        endpoint = endpoint.tls_config(tls).map_err(|e| format!("Failed to configure InfluxDB 3 Flight TLS: {e}"))?;
    }
    Ok(endpoint)
}

fn expand_cert_path(path: &str) -> String {
    let home = || std::env::var(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).ok();
    if path == "~" || path.starts_with("~/") || path.starts_with("~\\") {
        if let Some(home) = home() {
            return format!("{}{}", home, &path[1..]);
        }
    }
    if let Some(rest) = path.strip_prefix("$HOME") {
        if let Some(home) = home() {
            return format!("{home}{rest}");
        }
    }
    if let Some(rest) = path.strip_prefix("${HOME}") {
        if let Some(home) = home() {
            return format!("{home}{rest}");
        }
    }
    if let Some(rest) = path.strip_prefix("%USERPROFILE%") {
        if let Ok(home) = std::env::var("USERPROFILE") {
            return format!("{home}{rest}");
        }
    }
    path.to_string()
}

// ----- HTTP transport ------------------------------------------------------

#[derive(Serialize)]
struct HttpQueryBody<'a> {
    db: &'a str,
    q: &'a str,
    format: &'static str,
}

#[derive(Deserialize, Default)]
struct HttpErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Option<serde_json::Value>,
}

fn http_query_url(client: &Influxdb3Client) -> String {
    match client.url_params.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        Some(params) => format!("{}/api/v3/query_sql?{params}", client.base_url),
        None => format!("{}/api/v3/query_sql", client.base_url),
    }
}

fn http_auth(client: &Influxdb3Client, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    if let Some(token) = client.token.as_deref().map(str::trim).filter(|token| !token.is_empty()) {
        req.header("Authorization", format!("Bearer {token}"))
    } else {
        req
    }
}

async fn http_post_query(
    client: &Influxdb3Client,
    database: &str,
    sql: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let url = http_query_url(client);
    let body = HttpQueryBody { db: database, q: sql, format: "json" };
    let resp = http_auth(client, client.http.post(&url).json(&body))
        .send()
        .await
        .map_err(|e| format!("InfluxDB 3 request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(http_error_message(resp).await);
    }
    let text = resp.text().await.unwrap_or_default();
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str::<Vec<serde_json::Value>>(&text)
        .map_err(|e| format!("InfluxDB 3 JSON parse error: {e}; response: {text}"))
}

async fn http_error_message(resp: reqwest::Response) -> String {
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    let extracted = serde_json::from_str::<HttpErrorBody>(&body).ok().and_then(|value| {
        value
            .error
            .filter(|msg| !msg.trim().is_empty())
            .or_else(|| value.message.filter(|msg| !msg.trim().is_empty()))
            .or_else(|| value.data.and_then(|v| v.as_str().map(str::to_string)))
    });
    match extracted {
        Some(msg) => format!("InfluxDB 3 error: status = {}, message = {msg}", status.as_str()),
        None if body.trim().is_empty() => format!("InfluxDB 3 error: status = {}", status.as_str()),
        None => format!("InfluxDB 3 error: status = {}, body = {body}", status.as_str()),
    }
}

async fn http_list_databases(client: &Influxdb3Client) -> Result<Vec<DatabaseInfo>, String> {
    // Core exposes a dedicated admin endpoint that lists databases as
    // `[{ "iox::database": "<name>", ... }, ...]`. Prefer that over
    // querying `SHOW DATABASES` via SQL, which Core does not implement.
    // `?format=json` is REQUIRED — omitting it returns HTTP 400.
    let url = format!("{}/api/v3/configure/database?format=json", client.base_url);
    let resp =
        http_auth(client, client.http.get(&url)).send().await.map_err(|e| format!("InfluxDB 3 request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(http_error_message(resp).await);
    }
    let text = resp.text().await.unwrap_or_default();
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(&text).map_err(|e| format!("InfluxDB 3 JSON parse error: {e}; response: {text}"))?;
    let mut out = Vec::new();
    for row in rows {
        let Some(obj) = row.as_object() else { continue };
        // Accept the several key names Core has used across versions.
        let name = obj
            .get("iox::database")
            .or_else(|| obj.get("db_name"))
            .or_else(|| obj.get("name"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if let Some(name) = name.filter(|value| !value.is_empty()) {
            out.push(DatabaseInfo { name });
        }
    }
    Ok(out)
}

async fn http_list_tables(client: &Influxdb3Client, database: &str) -> Result<Vec<TableInfo>, String> {
    let rows = http_post_query(client, database, "SHOW TABLES").await?;
    // "SHOW TABLES" returns rows across every schema, including Core's own
    // `system` catalog (`queries`, `parquet_files`, ...) and the standard
    // `information_schema` (`tables`, `columns`, ...). User measurements
    // live under the `iox` schema; keep only those to avoid drowning the
    // sidebar in engine internals.
    let mut out = Vec::new();
    for row in rows {
        let Some(obj) = row.as_object() else { continue };
        let schema = obj.get("table_schema").and_then(serde_json::Value::as_str).unwrap_or("");
        if schema != "iox" {
            continue;
        }
        let Some(name) = obj.get("table_name").or_else(|| obj.get("name")).and_then(serde_json::Value::as_str) else {
            continue;
        };
        out.push(TableInfo {
            name: name.to_string(),
            table_type: "TABLE".to_string(),
            comment: None,
            parent_schema: None,
            parent_name: None,
        });
    }
    Ok(out)
}

async fn http_get_columns(client: &Influxdb3Client, database: &str, table: &str) -> Result<Vec<ColumnInfo>, String> {
    let sql = format!(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
         WHERE table_name = '{}'",
        escape_sql_literal(table)
    );
    let rows = http_post_query(client, database, &sql).await?;
    let mut out = Vec::new();
    for row in rows {
        let Some(obj) = row.as_object() else { continue };
        let name = obj.get("column_name").and_then(serde_json::Value::as_str).unwrap_or("").to_string();
        if name.is_empty() {
            continue;
        }
        let data_type = obj.get("data_type").and_then(serde_json::Value::as_str).unwrap_or("").to_string();
        let is_nullable = obj
            .get("is_nullable")
            .and_then(serde_json::Value::as_str)
            .map(|value| !value.eq_ignore_ascii_case("NO"))
            .unwrap_or(true);
        let is_primary_key = name == "time";
        out.push(ColumnInfo { name, data_type, is_nullable, is_primary_key, ..Default::default() });
    }
    Ok(out)
}

async fn http_execute_query(client: &Influxdb3Client, database: &str, sql: &str) -> Result<QueryResult, String> {
    let start = Instant::now();
    let rows = http_post_query(client, database, sql).await?;
    Ok(build_query_result_from_json_rows(rows, start))
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Flatten `Vec<serde_json::Value::Object>` into a `QueryResult` in
/// first-seen column order.
fn build_query_result_from_json_rows(rows: Vec<serde_json::Value>, start: Instant) -> QueryResult {
    let mut columns: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in &rows {
        let Some(obj) = row.as_object() else { continue };
        for key in obj.keys() {
            if seen.insert(key.clone()) {
                columns.push(key.clone());
            }
        }
    }
    let out_rows: Vec<Vec<serde_json::Value>> = rows
        .into_iter()
        .map(|row| {
            let obj = row.as_object().cloned().unwrap_or_default();
            columns.iter().map(|col| obj.get(col).cloned().unwrap_or(serde_json::Value::Null)).collect()
        })
        .collect();
    let affected = out_rows.len() as u64;
    QueryResult {
        column_sortables: columns.iter().map(|_| false).collect(),
        spatial_columns: vec![],
        spatial_values: vec![],
        columns,
        column_types: vec![],
        affected_rows: affected,
        rows: out_rows,
        execution_time_ms: start.elapsed().as_millis(),
        truncated: false,
        session_id: None,
        has_more: false,
        elasticsearch_raw_body: None,
    }
}

// ----- Flight SQL transport -----------------------------------------------

async fn flight_channel(client: &Influxdb3Client) -> Result<Channel, String> {
    let endpoint = client.endpoint.as_ref().ok_or_else(|| "InfluxDB 3 Flight endpoint not configured".to_string())?;
    let mut guard = client.channel.lock().await;
    if let Some(existing) = guard.as_ref() {
        return Ok(existing.clone());
    }
    let channel = endpoint.connect().await.map_err(|e| format!("InfluxDB 3 Flight connect failed: {e}"))?;
    *guard = Some(channel.clone());
    Ok(channel)
}

async fn flight_client(client: &Influxdb3Client) -> Result<FlightSqlServiceClient<Channel>, String> {
    let channel = flight_channel(client).await?;
    let mut fsql = FlightSqlServiceClient::new(channel);
    if let Some(token) = client.token.as_deref().map(str::trim).filter(|token| !token.is_empty()) {
        fsql.set_header("authorization", format!("Bearer {token}"));
    }
    Ok(fsql)
}

async fn flight_run_query(client: &Influxdb3Client, database: &str, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let mut fsql = flight_client(client).await?;
    // InfluxDB 3.x scopes queries to a database via `database` metadata.
    fsql.set_header("database", database.to_string());
    let info =
        fsql.execute(sql.to_string(), None).await.map_err(|e| format!("InfluxDB 3 Flight execute failed: {e}"))?;
    let mut batches = Vec::new();
    for ep in info.endpoint {
        let Some(ticket) = ep.ticket else { continue };
        let stream = fsql.do_get(ticket).await.map_err(|e| format!("InfluxDB 3 Flight do_get failed: {e}"))?;
        let collected: Vec<RecordBatch> =
            stream.try_collect().await.map_err(|e| format!("InfluxDB 3 Flight stream error: {e}"))?;
        batches.extend(collected);
    }
    Ok(batches)
}

async fn flight_test_connection(client: &Influxdb3Client, timeout: Duration) -> Result<(), String> {
    // If the user configured a database, prove the Flight SQL channel by
    // running a trivial query there. Otherwise fall back to the HTTP
    // /health endpoint — the admin surface is always accessible.
    match client.database.as_deref() {
        Some(database) => flight_run_query(client, database, "SELECT 1").await.map(|_| ()),
        None => {
            let url = format!("{}/health", client.base_url);
            let req = http_auth(client, client.http.get(&url));
            let resp = with_connection_timeout("InfluxDB 3", timeout, async {
                req.send().await.map_err(|e| format!("InfluxDB 3 connection failed: {e}"))
            })
            .await?;
            if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
                return Err(http_error_message(resp).await);
            }
            Ok(())
        }
    }
}

async fn flight_list_databases(client: &Influxdb3Client) -> Result<Vec<DatabaseInfo>, String> {
    // The HTTP admin endpoint is the canonical way to list databases on
    // InfluxDB 3 Core. Flight SQL does not expose an equivalent RPC, and
    // `SHOW DATABASES` is not a valid statement in Core's SQL dialect.
    http_list_databases(client).await
}

async fn flight_list_tables(client: &Influxdb3Client, database: &str) -> Result<Vec<TableInfo>, String> {
    // Filter to `iox` schema so the sidebar doesn't drown in `system` and
    // `information_schema` internals. Mirrors http_list_tables.
    let batches = flight_run_query(client, database, "SHOW TABLES").await?;
    let names = batches_extract_column(&batches, "table_name");
    let schemas = batches_extract_column(&batches, "table_schema");
    Ok(names
        .into_iter()
        .enumerate()
        .filter(|(index, _)| schemas.get(*index).map(String::as_str) == Some("iox"))
        .map(|(_, name)| TableInfo {
            name,
            table_type: "TABLE".to_string(),
            comment: None,
            parent_schema: None,
            parent_name: None,
        })
        .collect())
}

async fn flight_get_columns(client: &Influxdb3Client, database: &str, table: &str) -> Result<Vec<ColumnInfo>, String> {
    let sql = format!(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
         WHERE table_name = '{}'",
        escape_sql_literal(table)
    );
    let batches = flight_run_query(client, database, &sql).await?;
    let names = batches_extract_column(&batches, "column_name");
    let types = batches_extract_column(&batches, "data_type");
    let nullables = batches_extract_column(&batches, "is_nullable");
    let mut out = Vec::new();
    for (index, name) in names.into_iter().enumerate() {
        if name.is_empty() {
            continue;
        }
        let data_type = types.get(index).cloned().unwrap_or_default();
        let is_nullable = nullables.get(index).map(|value| !value.eq_ignore_ascii_case("NO")).unwrap_or(true);
        let is_primary_key = name == "time";
        out.push(ColumnInfo { name, data_type, is_nullable, is_primary_key, ..Default::default() });
    }
    Ok(out)
}

async fn flight_execute_query(client: &Influxdb3Client, database: &str, sql: &str) -> Result<QueryResult, String> {
    let start = Instant::now();
    let batches = flight_run_query(client, database, sql).await?;
    Ok(record_batches_to_query_result(&batches, start))
}

fn record_batches_to_query_result(batches: &[RecordBatch], start: Instant) -> QueryResult {
    let (columns, column_types) = match batches.first() {
        Some(batch) => {
            let schema = batch.schema();
            let cols = schema.fields().iter().map(|field| field.name().clone()).collect::<Vec<_>>();
            let types = schema.fields().iter().map(|field| format!("{}", field.data_type())).collect::<Vec<_>>();
            (cols, types)
        }
        None => (Vec::new(), Vec::new()),
    };
    let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
    for batch in batches {
        let num_rows = batch.num_rows();
        for row_idx in 0..num_rows {
            let mut row: Vec<serde_json::Value> = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                row.push(arrow_cell_to_json(col.as_ref(), row_idx));
            }
            rows.push(row);
        }
    }
    let affected = rows.len() as u64;
    QueryResult {
        column_sortables: columns.iter().map(|_| false).collect(),
        spatial_columns: vec![],
        spatial_values: vec![],
        columns,
        column_types,
        affected_rows: affected,
        rows,
        execution_time_ms: start.elapsed().as_millis(),
        truncated: false,
        session_id: None,
        has_more: false,
        elasticsearch_raw_body: None,
    }
}

fn batches_extract_column(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let Some(index) = batch.schema().index_of(column).ok() else { continue };
        let array = batch.column(index);
        for row_idx in 0..batch.num_rows() {
            match arrow_cell_to_json(array.as_ref(), row_idx) {
                serde_json::Value::String(value) => out.push(value),
                serde_json::Value::Null => {}
                other => out.push(other.to_string()),
            }
        }
    }
    out
}

/// Convert a single Arrow cell to a JSON value with enough type fidelity
/// for the grid: numbers stay numbers, strings stay strings, booleans stay
/// booleans, timestamps render as RFC3339 strings, everything else falls
/// back to a `Debug`-style string so nothing is silently lost.
fn arrow_cell_to_json(array: &dyn Array, row: usize) -> serde_json::Value {
    if array.is_null(row) {
        return serde_json::Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int8 => downcast_int::<Int8Array>(array, row, |v| v as i64),
        DataType::Int16 => downcast_int::<Int16Array>(array, row, |v| v as i64),
        DataType::Int32 => downcast_int::<Int32Array>(array, row, |v| v as i64),
        DataType::Int64 => downcast_int::<Int64Array>(array, row, |v| v),
        DataType::UInt8 => downcast_int::<UInt8Array>(array, row, |v| v as i64),
        DataType::UInt16 => downcast_int::<UInt16Array>(array, row, |v| v as i64),
        DataType::UInt32 => downcast_int::<UInt32Array>(array, row, |v| v as i64),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| {
                let v = a.value(row);
                if v <= i64::MAX as u64 {
                    serde_json::Value::Number(serde_json::Number::from(v as i64))
                } else {
                    serde_json::Value::String(v.to_string())
                }
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(row) as f64).map(serde_json::Value::Number))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(row)).map(serde_json::Value::Number))
            .unwrap_or(serde_json::Value::Null),
        DataType::Utf8 | DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(row).to_string()))
            .unwrap_or_else(|| serde_json::Value::String(format!("{:?}", array_debug_at(array, row)))),
        DataType::Timestamp(unit, _tz) => timestamp_cell_to_json(array, row, *unit),
        _ => serde_json::Value::String(format!("{}", array_debug_at(array, row))),
    }
}

fn downcast_int<T: Array + 'static>(
    array: &dyn Array,
    row: usize,
    to_i64: impl FnOnce(<T as PrimitiveValueAccess>::Value) -> i64,
) -> serde_json::Value
where
    T: PrimitiveValueAccess,
{
    array
        .as_any()
        .downcast_ref::<T>()
        .map(|a| serde_json::Value::Number(serde_json::Number::from(to_i64(a.value_at(row)))))
        .unwrap_or(serde_json::Value::Null)
}

/// Small helper trait so `downcast_int` can be generic over the primitive
/// arrow arrays without pulling in every private trait from arrow-array.
trait PrimitiveValueAccess {
    type Value: Copy;
    fn value_at(&self, row: usize) -> Self::Value;
}

macro_rules! impl_primitive_value_access {
    ($ty:ty, $val:ty) => {
        impl PrimitiveValueAccess for $ty {
            type Value = $val;
            fn value_at(&self, row: usize) -> Self::Value {
                self.value(row)
            }
        }
    };
}

impl_primitive_value_access!(Int8Array, i8);
impl_primitive_value_access!(Int16Array, i16);
impl_primitive_value_access!(Int32Array, i32);
impl_primitive_value_access!(Int64Array, i64);
impl_primitive_value_access!(UInt8Array, u8);
impl_primitive_value_access!(UInt16Array, u16);
impl_primitive_value_access!(UInt32Array, u32);

fn timestamp_cell_to_json(array: &dyn Array, row: usize, unit: TimeUnit) -> serde_json::Value {
    let nanos: Option<i64> = match unit {
        TimeUnit::Second => {
            array.as_any().downcast_ref::<TimestampSecondArray>().map(|a| a.value(row).saturating_mul(1_000_000_000))
        }
        TimeUnit::Millisecond => {
            array.as_any().downcast_ref::<TimestampMillisecondArray>().map(|a| a.value(row).saturating_mul(1_000_000))
        }
        TimeUnit::Microsecond => {
            array.as_any().downcast_ref::<TimestampMicrosecondArray>().map(|a| a.value(row).saturating_mul(1_000))
        }
        TimeUnit::Nanosecond => array.as_any().downcast_ref::<TimestampNanosecondArray>().map(|a| a.value(row)),
    };
    match nanos {
        Some(ns) => {
            let secs = ns.div_euclid(1_000_000_000);
            let sub_nanos = ns.rem_euclid(1_000_000_000) as u32;
            match chrono::DateTime::<chrono::Utc>::from_timestamp(secs, sub_nanos) {
                Some(dt) => serde_json::Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)),
                None => serde_json::Value::String(ns.to_string()),
            }
        }
        None => serde_json::Value::Null,
    }
}

fn array_debug_at(array: &dyn Array, row: usize) -> String {
    // arrow-cast::pretty::pretty_format_columns would be prettier but pulls
    // extra weight; a slice-and-Debug is enough for the fallback path.
    let slice = array.slice(row, 1);
    format!("{:?}", slice)
}

// ----- Public API ----------------------------------------------------------

pub async fn test_connection(client: &Influxdb3Client, timeout: Duration) -> Result<(), String> {
    match client.transport {
        TransportKind::Http => {
            let url = format!("{}/health", client.base_url);
            let req = http_auth(client, client.http.get(&url));
            let resp = with_connection_timeout("InfluxDB 3", timeout, async {
                req.send().await.map_err(|e| format!("InfluxDB 3 connection failed: {e}"))
            })
            .await?;
            if !resp.status().is_success() && resp.status() != reqwest::StatusCode::NOT_FOUND {
                return Err(http_error_message(resp).await);
            }
            Ok(())
        }
        TransportKind::FlightSql => {
            with_connection_timeout("InfluxDB 3", timeout, flight_test_connection(client, timeout)).await
        }
    }
}

pub async fn list_databases(client: &Influxdb3Client) -> Result<Vec<DatabaseInfo>, String> {
    match client.transport {
        TransportKind::Http => http_list_databases(client).await,
        TransportKind::FlightSql => flight_list_databases(client).await,
    }
}

pub async fn list_tables(client: &Influxdb3Client, database: &str) -> Result<Vec<TableInfo>, String> {
    match client.transport {
        TransportKind::Http => http_list_tables(client, database).await,
        TransportKind::FlightSql => flight_list_tables(client, database).await,
    }
}

pub async fn get_columns(client: &Influxdb3Client, database: &str, table: &str) -> Result<Vec<ColumnInfo>, String> {
    match client.transport {
        TransportKind::Http => http_get_columns(client, database, table).await,
        TransportKind::FlightSql => flight_get_columns(client, database, table).await,
    }
}

pub async fn execute_query(client: &Influxdb3Client, database: &str, sql: &str) -> Result<QueryResult, String> {
    match client.transport {
        TransportKind::Http => http_execute_query(client, database, sql).await,
        TransportKind::FlightSql => flight_execute_query(client, database, sql).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_to_flight_sql_transport() {
        let config: ConnectionConfig = serde_json::from_value(json!({
            "id": "influx3",
            "name": "InfluxDB 3",
            "db_type": "influxdb3",
            "host": "127.0.0.1",
            "port": 8181,
            "username": "",
            "password": "my-token",
            "database": "metrics"
        }))
        .unwrap();
        let client =
            Influxdb3Client::new_for_config("http://localhost:8181/", &config, Duration::from_secs(1)).unwrap();
        assert_eq!(client.transport, TransportKind::FlightSql);
        assert_eq!(client.token.as_deref(), Some("my-token"));
        assert_eq!(client.database.as_deref(), Some("metrics"));
        assert_eq!(client.base_url, "http://localhost:8181");
        assert!(client.endpoint.is_some());
    }

    #[test]
    fn http_transport_opt_in_via_external_config() {
        let config: ConnectionConfig = serde_json::from_value(json!({
            "id": "influx3-http",
            "name": "InfluxDB 3 HTTP",
            "db_type": "influxdb3",
            "host": "127.0.0.1",
            "port": 8181,
            "username": "",
            "password": "t",
            "external_config": {
                "transport": "http",
                "database": "alt"
            }
        }))
        .unwrap();
        let client = Influxdb3Client::new_for_config("http://localhost:8181", &config, Duration::from_secs(1)).unwrap();
        assert_eq!(client.transport, TransportKind::Http);
        assert_eq!(client.database.as_deref(), Some("alt"));
        assert!(client.endpoint.is_none());
    }

    #[test]
    fn http_query_url_includes_url_params() {
        let client = Influxdb3Client {
            transport: TransportKind::Http,
            token: None,
            database: None,
            http: HttpClient::new(),
            base_url: "http://localhost:8181".to_string(),
            url_params: Some("pretty=true".to_string()),
            endpoint: None,
            channel: Arc::new(AsyncMutex::new(None)),
        };
        assert_eq!(http_query_url(&client), "http://localhost:8181/api/v3/query_sql?pretty=true");
    }

    #[test]
    fn build_query_result_flattens_json_rows() {
        let rows = vec![
            json!({"time": "2026-01-01T00:00:00Z", "host": "a", "value": 1.5}),
            json!({"time": "2026-01-01T00:00:01Z", "host": "b", "value": 2.5}),
        ];
        let result = build_query_result_from_json_rows(rows, Instant::now());
        assert_eq!(result.columns, vec!["time", "host", "value"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][1], json!("a"));
        assert_eq!(result.rows[1][2], json!(2.5));
    }

    #[test]
    fn escape_sql_literal_doubles_single_quotes() {
        assert_eq!(escape_sql_literal("O'Brien"), "O''Brien");
    }
}
