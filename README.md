# DataPulse - Data Pipeline Monitor

DataPulse is a desktop application designed for data engineers to monitor ETL pipelines, verify data source connections, inspect data quality test results, examine database schemas, and view real-time system health metrics.

The project is pre-configured with a Jenkins CI/CD pipeline that automates linting, runs unit tests, and deploys the application inside a Jenkins container environment.

## Features

* Dashboard: High-level overview of pipeline success/failure metrics, total processed rows, recent execution logs, and system health monitors (CPU, Memory, Disk IO).
* Pipeline Jobs: Detailed view of recent ETL job statuses (Success, Failed, Running, Queued) with options to trigger manual pipeline runs.
* Data Source Connections: Connection validation tool supporting PostgreSQL, MySQL, MongoDB, Redis, Kafka, AWS S3, Snowflake, and Elasticsearch.
* Data Quality Checks: View test outcomes (Passed, Failed) for primary keys, null values, freshness metrics, referential integrity, and schema drift.
* Schema Explorer: Browse table details, column types, and structural relationship markers (Primary Keys and Foreign Keys) for primary data warehouse tables.
* CSV Export: Export current pipeline status reports directly to CSV.

## Project Structure

```
python-gui-jenkins/
├── app/
│   ├── __init__.py
│   └── main.py          # DataPulse GUI application (Tkinter)
├── tests/
│   ├── __init__.py
│   └── test_app.py      # 27 Headless Unit Tests
├── Jenkinsfile           # Declarative CI/CD pipeline
├── README.md             # Project documentation
└── requirements.txt      # Python dependencies
```

## Local Installation and Execution

DataPulse uses Tkinter for its user interface, which is built into standard Python installations.

### Prerequisites

* Python 3.8 or higher

### Running the Application

1. Clone or navigate to the repository directory:
   ```bash
   cd python-gui-jenkins
   ```

2. Run the application:
   ```bash
   python app/main.py
   ```

### Running Unit Tests

Run the test suite locally using the unittest discover tool:
```bash
python -m unittest discover -s tests -v
```

All 27 test cases run headlessly, meaning they do not require a display window to execute, making them ideal for server and CI/CD environments.

## CI/CD Pipeline

The project includes a `Jenkinsfile` configuring a declarative pipeline with the following stages:

1. Checkout: Retrieves the codebase from the SCM repository.
2. Setup: Prepares the Python environment and installs dependencies.
3. Lint: Inspects Python files for standard style guidelines using pycodestyle.
4. Test: Runs all unit tests and publishes test result reports (JUnit format).
5. Deploy: Copies the active application components to a persistent deployment path (`/var/jenkins_home/deployed/datapulse`) within the Jenkins container.

## Running in a Dockerized Jenkins Environment

Because Jenkins is running inside a Docker container, the GUI application will execute tests headlessly but cannot render the graphical user interface directly inside the container. 

To fetch the deployed code from your Jenkins Docker container to your host machine after a successful pipeline run:

1. Identify your running Jenkins container name:
   ```bash
   docker ps
   ```

2. Copy the deployed application from the container to your local Windows Downloads folder:
   ```bash
   docker cp jenkins-container:/var/jenkins_home/deployed/datapulse %USERPROFILE%\Downloads\datapulse_deployed
   ```
   *(Note: Replace "jenkins-container" with your actual container name if different)*

3. Run the deployed GUI application on your local machine:
   ```bash
   python %USERPROFILE%\Downloads\datapulse_deployed\app\main.py
   ```
