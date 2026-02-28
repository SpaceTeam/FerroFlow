# Ferroflow
Ferroflow is the new control software for all Liquid Rocketry projects at the TU Wien Space Team. 
It interfaces with our custom Engine Control Units ECUs, through our custom [LiquidCAN protocol](https://github.com/SpaceTeam/LiquidCAN/).
On the other end, it provides a high-level API for our [ECUI](https://github.com/SpaceTeam/web_ecui_houbolt), which is the user interface for our ECUs.

# Setup
TODO


## Development

### Running CI Checks

The repository includes a CI script (`ci-rust.sh`) that runs all quality checks on the Rust implementation. This script is used both locally and in GitHub Actions

**Run all checks:**
```bash
./ci-rust.sh
# or explicitly
./ci-rust.sh all
```

**Run individual checks:**
```bash
./ci-rust.sh build         # Build the project
./ci-rust.sh test          # Run tests
./ci-rust.sh fmt           # Check code formatting
./ci-rust.sh clippy        # Run clippy linter
```
You can fix formatting or linter issues by adding the -fix suffix to the command. e.g: `./ci-rust.sh clippy-fix`