// tests/cli_integration.rs
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::{Command, Stdio};
use tempfile::tempdir;
use std::thread;
use std::time::Duration;
use reqwest::blocking::Client;
use std::net::TcpListener;

// Helper function to kill process cleanly
fn kill_process(child: &mut std::process::Child) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;
        // Send SIGINT (like Ctrl+C) for graceful shutdown attempt on Unix
        signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)?;
    }
    #[cfg(windows)]
    {
        // Signal simulation on Windows is complex. Killing is less graceful but simpler for tests.
        // Note: This won't trigger the graceful shutdown logic in main.rs on Windows.
        child.kill()?;
    }
    Ok(())
}


fn get_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to random port")
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

#[test]
fn test_cli_help() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("dead_simple_db")?;
    cmd.arg("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Simple Key-Value Database Server"))
        .stdout(predicate::str::contains("--listen"))
        .stdout(predicate::str::contains("--data-file"))
        .stdout(predicate::str::contains("--index-file"))
        .stdout(predicate::str::contains("--sync"))
        .stdout(predicate::str::contains("--config")); // Added check for new config flag
    Ok(())
}

#[test]
fn test_cli_starts_and_responds() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let data_path = dir.path().join("cli_test.dblog");
    let port = get_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let url_base = format!("http://{}", addr);
    let client = Client::new();

    let mut cmd = Command::cargo_bin("dead_simple_db")?;
    cmd.arg("--listen")
       .arg(&addr)
       .arg("--data-file")
       .arg(data_path.to_str().expect("Path should be valid UTF-8"));

    // Spawn the server process
    let mut child = cmd.spawn()?;

    // Give it a moment to start up
    // Increased sleep slightly to reduce flakes in CI environments
    thread::sleep(Duration::from_millis(800));

    // Check if it's alive by trying a basic request (e.g., get non-existent key)
    let get_url = format!("{}/keys/startup_check", url_base);
    let resp = client.get(&get_url).send();

    // Assert that we got *some* response (likely 404, which is fine)
    assert!(resp.is_ok(), "Server did not respond to GET request");
    if let Ok(response) = resp {
         // We expect 404 for a non-existent key, confirming the server is running
         assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    }

    // Cleanly shut down the server process
    kill_process(&mut child)?;

    // Wait for the process to exit and check status
    let status = child.wait()?;
    // SIGINT might cause non-zero exit status, so don't assert success strictly here
    println!("test_cli_starts_and_responds exit status: {:?}", status.code());

    Ok(())
}


#[test]
fn test_cli_invalid_listen_address() -> Result<(), Box<dyn std::error::Error>> {
     // Try to bind to an address that is invalid format
     let mut cmd = Command::cargo_bin("dead_simple_db")?;
     // Provide an address clap cannot parse as SocketAddr
     cmd.arg("--listen").arg("not-a-valid-address");

     cmd.assert()
         .failure() // Expect the process to fail during argument parsing
         .stderr(predicate::str::contains("invalid value 'not-a-valid-address'")); // Check clap's error output

     Ok(())
}

#[test]
fn test_config_precedence() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config_path = dir.path().join("config_precedence.toml");
    let data_path_cli = dir.path().join("data_cli.dblog"); // Specific path for CLI arg
    let port_cli = get_free_port();
    let addr_cli = format!("127.0.0.1:{}", port_cli); // Specific addr for CLI arg

    // Write a temporary TOML config file with DIFFERENT values
    std::fs::write(
        &config_path,
        "listen = '127.0.0.1:1234'\ndata_file = 'data_from_config.dblog'\nsync = 'always'\n"
    )?;

    let mut cmd = Command::cargo_bin("dead_simple_db")?;
    cmd.arg("--config").arg(config_path.to_str().expect("Config path should be valid UTF-8"))
       .arg("--listen").arg(&addr_cli) // CLI should override TOML listen
       .arg("--data-file").arg(data_path_cli.to_str().expect("Data path should be valid UTF-8")) // CLI should override TOML data_file
       .arg("--sync").arg("never"); // CLI should override TOML sync

    // Capture output instead of using assert() directly on the command
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn()?;

    // Give it time to print startup logs
    thread::sleep(Duration::from_millis(800));

    // Shut it down
    kill_process(&mut child)?;

    // Wait and capture output
    let output = child.wait_with_output()?;

    // Convert stdout bytes to string for checking
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    println!("--- test_config_precedence stdout ---");
    println!("{}", stdout_str);
    println!("--- test_config_precedence stderr ---");
    println!("{}", String::from_utf8_lossy(&output.stderr));
    println!("--- End Output ---");

    // Assert that the *final* configuration logged uses the CLI arguments
    assert!(stdout_str.contains(&format!("Listen address: {}", addr_cli)));
    // Use Debug format for paths to handle potential escaping on Windows
    assert!(stdout_str.contains(&format!("Data file: {:?}", data_path_cli)));
    assert!(stdout_str.contains("Sync strategy: Never"));
    // Also check that the config file loading message appears
    assert!(stdout_str.contains(&format!("Attempting to load configuration from: {:?}", config_path)));

    Ok(())
}

#[test]
fn test_config_defaults() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?; // Use tempdir to ensure we don't accidentally load a real config.toml
    let cwd = dir.path().to_path_buf(); // Run the command inside the temp dir

    // We expect these defaults if no config file is found and no CLI args are given
    let default_addr = "127.0.0.1:7878";
    let default_data_file_log_output = "\"data.dblog\""; // Match the logged output including quotes
    let default_sync = "Sync strategy: Never";

    let mut cmd = Command::cargo_bin("dead_simple_db")?;
    cmd.current_dir(&cwd); // Run in the empty temp directory

    // Capture output
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn()?;

    // Give it time to print startup logs
    thread::sleep(Duration::from_millis(800));

    // Shut it down
    kill_process(&mut child)?;

    // Wait and capture output
    let output = child.wait_with_output()?;

    // Convert stdout bytes to string for checking
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    println!("--- test_config_defaults stdout ---");
    println!("{}", stdout_str);
    println!("--- test_config_defaults stderr ---");
    println!("{}", String::from_utf8_lossy(&output.stderr));
    println!("--- End Output ---");


    // Assert that the final configuration logged uses the default values
    assert!(stdout_str.contains(&format!("Listen address: {}", default_addr)));
    // Adjusted assertion to match the logged relative path string
    assert!(stdout_str.contains(&format!("Data file: {}", default_data_file_log_output)));
    assert!(stdout_str.contains(default_sync));
    // Check that it *tried* to load config.toml optionally
    assert!(stdout_str.contains("attempting to load 'config.toml' optionally"));
    // config crate doesn't log specific "not found" for optional files, so don't check for that.

    Ok(())
}