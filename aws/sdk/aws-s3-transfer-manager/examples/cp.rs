use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::time;

use aws_s3_transfer_manager::download::Downloader;

use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use clap::{CommandFactory, Parser};
use tokio::fs;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const ONE_GIGABYTE: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "cp")]
#[command(about = "Copies a local file or S3 object to another location locally or in S3.")]
pub struct Args {
    /// Source to copy from <S3Uri | Local>
    #[arg(required = true)]
    source: TransferUri,

    /// Destination to copy to <S3Uri | Local>
    #[arg(required = true)]
    dest: TransferUri,
    /// Number of concurrent uploads/downloads to perform.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
}

#[derive(Clone, Debug)]
enum TransferUri {
    /// Local filesystem source/destination
    Local(PathBuf),

    /// S3 source/destination
    S3(S3Uri),
}

impl TransferUri {
    fn expect_s3(&self) -> &S3Uri {
        match self {
            TransferUri::S3(s3_uri) => s3_uri,
            _ => panic!("expected S3Uri"),
        }
    }

    fn expect_local(&self) -> &PathBuf {
        match self {
            TransferUri::Local(path) => path,
            _ => panic!("expected Local"),
        }
    }
}

impl FromStr for TransferUri {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uri = if s.starts_with("s3://") {
            TransferUri::S3(S3Uri(s.to_owned()))
        } else {
            let path = PathBuf::from_str(s).unwrap();
            TransferUri::Local(path)
        };
        Ok(uri)
    }
}

#[derive(Clone, Debug)]
struct S3Uri(String);

impl S3Uri {
    /// Split the URI into it's component parts '(bucket, key)'
    fn parts(&self) -> (&str, &str) {
        self.0
            .strip_prefix("s3://")
            .expect("valid s3 uri prefix")
            .split_once('/')
            .expect("invalid s3 uri, missing '/' between bucket and key")
    }
}

fn invalid_arg(message: &str) -> ! {
    Args::command()
        .error(clap::error::ErrorKind::InvalidValue, message)
        .exit();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    println!("{:#?}", args);

    match (&args.source, &args.dest) {
        (TransferUri::Local(_), TransferUri::S3(_)) => todo!("upload not implemented yet"),
        (TransferUri::Local(_), TransferUri::Local(_)) => {
            invalid_arg("local to local transfer not supported")
        }
        (TransferUri::S3(_), TransferUri::Local(_)) => (),
        (TransferUri::S3(_), TransferUri::S3(_)) => invalid_arg("s3 to s3 transfer not supported"),
    }

    let config = aws_config::from_env().load().await;

    let tm = Downloader::builder()
        .sdk_config(config)
        .concurrency(args.concurrency)
        .build();

    let (bucket, key) = args.source.expect_s3().parts();
    let input = GetObjectInputBuilder::default().bucket(bucket).key(key);

    println!("loading dest file");
    let mut dest = fs::File::create(args.dest.expect_local()).await?;
    println!("dest file opened, starting download");

    let start = time::Instant::now();
    let resp = tm.download(&mut dest, input.into()).await?;
    let elapsed_secs = start.elapsed().as_secs_f64();

    let obj_size = resp
        .object_meta
        .content_length
        .expect("Content-Length available") as u64;
    let obj_size_gigabytes = obj_size as f64 / ONE_GIGABYTE as f64;
    let obj_size_gigabits = obj_size_gigabytes * 8.0;

    println!(
        "downloaded {} bytes ({} GiB) in {} seconds; GiB/s: {}; Gbps: {}",
        obj_size,
        obj_size_gigabytes,
        elapsed_secs,
        obj_size_gigabytes / elapsed_secs,
        obj_size_gigabits / elapsed_secs
    );

    Ok(())
}
