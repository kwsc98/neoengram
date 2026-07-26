use std::process::ExitCode;

mod cli;

#[tokio::main]
async fn main() -> ExitCode {
    match cli::run().await {
        Ok(result) => {
            cli::render(&result);
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}
