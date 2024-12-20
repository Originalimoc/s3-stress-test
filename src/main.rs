use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, Client, error::DisplayErrorContext};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::ObjectCannedAcl;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;
use bytes::Bytes;
use uuid::Uuid;

const MIN_MULTIPART_SIZE: usize = 5 * 1024 * 1024; // 5 MB
const MAX_PARTS: usize = 10000;
const MAX_CHECKSUM_SIZE: usize = 1024; // 1 KB

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    bucket: String,

    #[arg(short, long, default_value_t = 1000)]
    num_objects: usize,

    #[arg(short, long, default_value_t = 4096)]
    object_size: usize, // in bytes

    #[arg(short, long, default_value_t = 100)]
    concurrency: usize,

    #[arg(long, default_value_t = false)]
    large_object_test: bool,

    #[arg(long, default_value_t = 1024 * 1024 * 1024)] // 1 GB default
    large_object_size: usize, // in bytes for large object test

    #[arg(long, default_value_t = String::from("us-east-1"))]
    region: String,

    #[arg(long)]
    endpoint_url: Option<String>, // Add endpoint URL argument

    #[arg(long, default_value_t = false)]
    cleanup: bool,
}

// Function to initialize the S3 client
async fn initialize_s3_client(region: &str, endpoint_url: &Option<String>) -> Client {
    let region_provider = RegionProviderChain::first_try(Region::new(region.to_owned()))
        .or_default_provider()
        .or_else(Region::new("us-east-1"));

    let shared_config_builder = aws_config::from_env().region(region_provider);

    let shared_config = if let Some(endpoint) = endpoint_url {
        shared_config_builder.endpoint_url(endpoint).load().await
    } else {
        shared_config_builder.load().await
    };

    Client::new(&shared_config)
}

// Function to upload a single object
async fn upload_object(
    client: Arc<Client>,
    bucket: String,
    key: String,
    body: Bytes,
) -> Result<(), String> {
    let result = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body.into())
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Upload error: {}", DisplayErrorContext(&e))),
    }
}

// Function to download a single object with optional checksum validation
async fn download_object(
    client: Arc<Client>,
    bucket: String,
    key: String,
    size: usize,
    original_data: Option<&[u8]>,
) -> Result<usize, String> {
    let result = client.get_object().bucket(bucket).key(key).send().await;

    match result {
        Ok(mut resp) => {
            let mut total_bytes_downloaded = 0;
            let mut first_chunk = Vec::new();
            let mut last_chunk = Vec::new();

            while let Some(chunk) = resp.body.try_next().await.map_err(|e| format!("Error reading chunk: {}", DisplayErrorContext(&e)))? {
                let chunk_len = chunk.len();

                // Collect the first MAX_CHECKSUM_SIZE bytes
                if first_chunk.len() < MAX_CHECKSUM_SIZE && total_bytes_downloaded < size {
                    let needed = std::cmp::min(MAX_CHECKSUM_SIZE - first_chunk.len(), chunk_len);
                    first_chunk.extend_from_slice(&chunk[..needed]);
                }

                // Collect the last MAX_CHECKSUM_SIZE bytes
                if total_bytes_downloaded + chunk_len >= size - MAX_CHECKSUM_SIZE {
                    let start_index = if total_bytes_downloaded > size - MAX_CHECKSUM_SIZE {
                        0
                    } else {
                        size - MAX_CHECKSUM_SIZE - total_bytes_downloaded
                    };

                    if last_chunk.len() < MAX_CHECKSUM_SIZE {
                        let needed = std::cmp::min(MAX_CHECKSUM_SIZE - last_chunk.len(), chunk_len - start_index);
                        last_chunk.extend_from_slice(&chunk[start_index..start_index + needed]);
                    }
                }

                total_bytes_downloaded += chunk_len;
            }

            // Perform checksum validation if original data is provided
            if let Some(data) = original_data {
                // Use the minimum of the actual chunk length and MAX_CHECKSUM_SIZE for comparison
                let first_len = std::cmp::min(first_chunk.len(), MAX_CHECKSUM_SIZE);
                let last_len = std::cmp::min(last_chunk.len(), MAX_CHECKSUM_SIZE);

                if first_chunk.len() > 0 && &data[..first_len] != &first_chunk[..first_len] {
                    return Err("Checksum mismatch in the first chunk".to_string());
                }
                if last_chunk.len() > 0 && &data[data.len() - last_len..] != &last_chunk[..last_len] {
                    return Err("Checksum mismatch in the last chunk".to_string());
                }
            }

            if total_bytes_downloaded != size {
                Err(format!(
                    "Downloaded size mismatch: expected {}, got {}",
                    size, total_bytes_downloaded
                ))
            } else {
                Ok(total_bytes_downloaded)
            }
        },
        Err(e) => Err(format!("Download error: {}", DisplayErrorContext(&e))),
    }
}

// Function to delete a single object
async fn delete_object(
    client: Arc<Client>,
    bucket: String,
    key: String,
) -> Result<(), String> {
    let result = client.delete_object().bucket(bucket).key(key).send().await;

    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Delete error: {}", DisplayErrorContext(&e))),
    }
}

// Function to perform the large object throughput test (in-memory)
async fn run_large_object_test(
    client: Arc<Client>,
    args: Args,
) -> Result<(), String> {
    if args.large_object_size < MIN_MULTIPART_SIZE {
        return Err(format!("Large object size must be at least {} bytes", MIN_MULTIPART_SIZE));
    }

    // Define part size limits (8 MB to 4 GB)
    const MIN_PART_SIZE: usize = 8 * 1024 * 1024;
    const MAX_PART_SIZE: usize = 4 * 1024 * 1024 * 1024;

    // Calculate the ideal part size
    let mut part_size = args.large_object_size / MAX_PARTS;

    // Adjust part size to be within the allowed range
    part_size = part_size.clamp(MIN_PART_SIZE, MAX_PART_SIZE);

    // Check if the calculated part size is valid
    if args.large_object_size / part_size > MAX_PARTS {
        return Err(format!(
            "Large object size and part size combination exceeds maximum number of parts ({})",
            MAX_PARTS
        ));
    }

    println!("Running large object ({} MB) throughput test with part size {} MB (in-memory)...",
             args.large_object_size as f64 / 1024.0 / 1024.0,
             part_size as f64 / 1024.0 / 1024.0);

    // Generate static data in memory
    let static_data: Vec<u8> = (0..args.large_object_size).map(|i| (i % 256) as u8).collect();
    let large_object_data = static_data.clone();

    let key = format!("large_object_{}", Uuid::new_v4());

    // Upload
    let upload_start = Instant::now();
    let create_multipart_upload_result = client
        .create_multipart_upload()
        .bucket(&args.bucket)
        .key(&key)
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;

    let upload_id = match create_multipart_upload_result {
        Ok(response) => response.upload_id().unwrap().to_string(),
        Err(e) => {
            return Err(format!("Failed to create multipart upload: {}", DisplayErrorContext(&e)));
        }
    };

    let mut part_number = 1;
    let mut upload_parts = Vec::new();
    let mut remaining_bytes = args.large_object_size;
    let mut offset = 0;

    while remaining_bytes > 0 {
        let current_part_size = std::cmp::min(part_size, remaining_bytes);
        let body = aws_sdk_s3::primitives::ByteStream::from(
            large_object_data[offset..offset + current_part_size].to_vec(),
        );

        let upload_part_response = client
            .upload_part()
            .bucket(&args.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(body)
            .send()
            .await;

        match upload_part_response {
            Ok(response) => {
                upload_parts.push(CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(response.e_tag().unwrap_or_default().to_string())
                    .build());
            },
            Err(e) => {
                let abort_result = client.abort_multipart_upload()
                    .bucket(&args.bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .send()
                    .await;

                let abort_error_msg = match abort_result {
                    Ok(_) => "Multipart upload aborted successfully.".to_string(),
                    Err(abort_err) => format!("Failed to abort multipart upload: {}", DisplayErrorContext(&abort_err)),
                };

                return Err(format!("Failed to upload part: {}. {}", DisplayErrorContext(&e), abort_error_msg));
            }
        }

        part_number += 1;
        remaining_bytes -= current_part_size;
        offset += current_part_size;
    }

    let completed_multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let complete_multipart_upload_result = client
        .complete_multipart_upload()
        .bucket(&args.bucket)
        .key(&key)
        .upload_id(&upload_id)
        .multipart_upload(completed_multipart_upload)
        .send()
        .await;

    match complete_multipart_upload_result {
        Ok(_) => {},
        Err(e) => {
            return Err(format!("Failed to complete multipart upload: {}", DisplayErrorContext(&e)));
        }
    }

    let upload_duration = upload_start.elapsed();

    // Download
    let download_start = Instant::now();
    download_object(client.clone(), args.bucket.clone(), key.clone(), args.large_object_size, Some(&large_object_data)).await?;
    let download_duration = download_start.elapsed();

    // Cleanup
    if args.cleanup {
        delete_object(client.clone(), args.bucket.clone(), key.clone()).await.map_err(|e| e.to_string())?;
    }

    println!(
        "Upload Throughput: {:.2} MB/s",
        (args.large_object_size as f64 / 1024.0 / 1024.0) / upload_duration.as_secs_f64()
    );
    println!(
        "Download Throughput: {:.2} MB/s",
        (args.large_object_size as f64 / 1024.0 / 1024.0) / download_duration.as_secs_f64()
    );

    Ok(())
}

// Function to perform the small object IOPS test (in-memory)
async fn run_small_object_test(
    client: Arc<Client>,
    args: Args,
) -> Result<(), String> {
    println!("Running small object ({} bytes) IOPS test (in-memory)...", args.object_size);

    let total_objects = args.num_objects;

    // Progress bar setup
    let pb = ProgressBar::new(total_objects as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let start = Instant::now();

    // Use a channel to collect results from spawned tasks
    let (tx, mut rx) = mpsc::channel(args.concurrency);

    // Store generated keys and data for use in download and cleanup phases
    let mut keys = Vec::with_capacity(total_objects);
    // Wrap original_data in an Arc for sharing across threads
    let original_data = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(total_objects)));

    // Upload Phase
    for i in 0..total_objects {
        let client = client.clone();
        let bucket = args.bucket.clone();
        let object_size = args.object_size;
        let tx = tx.clone();
        let original_data_clone = original_data.clone();

        let key = format!("object_{}_{}", i, Uuid::new_v4());
        keys.push(key.clone());

        tokio::spawn(async move {
            // Generate static data in memory:
            let data: Vec<u8> = (0..object_size).map(|i| (i % 256) as u8).collect();

            // Store the generated data in the shared vector
            let mut original_data_guard = original_data_clone.lock().await;
            original_data_guard.push(data.clone());

            let result = upload_object(client, bucket, key, Bytes::from(data)).await;
            tx.send(result).await.unwrap(); // Handle send error if needed
        });
    };

    // Drop the original tx, so rx.recv() will eventually return None
    drop(tx);

    // Collect upload results
    let mut upload_errors = 0;
    while let Some(result) = rx.recv().await {
        match result {
            Ok(_) => pb.inc(1),
            Err(e) => {
                eprintln!("{}", e);
                upload_errors += 1;
            }
        }
    }

    pb.finish();
    let upload_duration = start.elapsed();

    if upload_errors > 0 {
        return Err(format!("{} uploads failed", upload_errors));
    }

    // Download Phase
    let download_start = Instant::now();
    let download_pb = ProgressBar::new(total_objects as u64);
    download_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let (tx_download, mut rx_download) = mpsc::channel(args.concurrency);

    // Lock the original_data vector for reading
    let original_data_guard = original_data.lock().await;

    for i in 0..total_objects {
        let key = keys[i].clone();
        let client = client.clone();
        let bucket = args.bucket.clone();
        let object_size = args.object_size;
        let tx_download = tx_download.clone();
        // Clone the data slice for this task
        let original_data_slice = original_data_guard[i].clone();

        tokio::spawn(async move {
            let result = download_object(client, bucket, key, object_size, Some(&original_data_slice)).await;
            tx_download.send(result).await.unwrap();
        });
    }

    drop(tx_download);

    // Collect download results
    let mut download_errors = 0;
    while let Some(result) = rx_download.recv().await {
        match result {
            Ok(_) => download_pb.inc(1),
            Err(e) => {
                eprintln!("{}", e);
                download_errors += 1;
            }
        }
    }

    download_pb.finish();
    let download_duration = download_start.elapsed();

    if download_errors > 0 {
        return Err(format!("{} downloads failed", download_errors));
    }

    // Cleanup Phase (conditional)
    if args.cleanup {
        let cleanup_pb = ProgressBar::new(total_objects as u64);
        cleanup_pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );

        let (tx_cleanup, mut rx_cleanup) = mpsc::channel(args.concurrency);

        for key in keys {
            let client = client.clone();
            let bucket = args.bucket.clone();
            let tx_cleanup = tx_cleanup.clone();
            tokio::spawn(async move {
                let result = delete_object(client, bucket, key).await;
                tx_cleanup.send(result).await.unwrap();
            });
        }

        drop(tx_cleanup);

        // Collect cleanup results
        let mut cleanup_errors = 0;
        while let Some(result) = rx_cleanup.recv().await {
            match result {
                Ok(_) => cleanup_pb.inc(1),
                Err(e) => {
                    eprintln!("{}", e);
                    cleanup_errors += 1;
                }
            }
        }

        cleanup_pb.finish();

        if cleanup_errors > 0 {
            return Err(format!("{} deletions failed", cleanup_errors));
        }
    }

    let elapsed = upload_duration + download_duration;

    println!("Total time elapsed: {:?}", elapsed);
    println!(
        "Upload IOPS: {:.2}",
        (total_objects - upload_errors) as f64 / upload_duration.as_secs_f64()
    );
    println!(
        "Download IOPS: {:.2}",
        (total_objects - download_errors) as f64 / download_duration.as_secs_f64()
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = std::sync::Arc::new(initialize_s3_client(&args.region, &args.endpoint_url).await);

    if args.large_object_test {
        run_large_object_test(client.clone(), args).await?;
    } else {
        run_small_object_test(client.clone(), args).await?;
    }

    Ok(())
}