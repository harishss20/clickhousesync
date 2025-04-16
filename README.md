# Data Ingestion Tool

This project is a Data Ingestion Tool that allows users to connect to ClickHouse, ingest flat files, and perform data operations such as exporting and previewing data.

## Prerequisites

- **Node.js**: Ensure you have Node.js installed (v14 or later).
- **Java**: Install Java Development Kit (JDK) 11 or later.
- **Maven**: Ensure Apache Maven is installed.
- **ClickHouse**: A running instance of ClickHouse is required.

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd Assignment2
```

### 2. Client Setup

Navigate to the `client` directory and install dependencies:

```bash
cd client
npm install
```

### 3. Server Setup

Navigate to the `server` directory and build the project:

```bash
cd server
mvn clean install
```

## Configuration

### Client Configuration

- Update any necessary API endpoints in `client/api/clickhouse.js`.

### Server Configuration

- Update the `application.properties` file in `server/src/main/resources` with your ClickHouse connection details:
  ```properties
  spring.datasource.url=jdbc:clickhouse://<host>:<port>/<database>
  spring.datasource.username=<username>
  spring.datasource.password=<password>
  ```

## Running the Application

### 1. Start the Server

Navigate to the `server` directory and run the server:

```bash
cd server
mvn spring-boot:run
```

### 2. Start the Client

Navigate to the `client` directory and start the development server:

```bash
cd client
npm run dev
```

The client will be available at `http://localhost:3000`.

## Features

- **Connect to ClickHouse**: Establish a connection to a ClickHouse database.
- **Ingest Flat Files**: Upload and ingest flat files into ClickHouse.
- **Export Data**: Export data from ClickHouse to flat files.
- **Preview Data**: Preview data from ClickHouse tables.

## Troubleshooting

- Ensure ClickHouse is running and accessible.
- Verify the `application.properties` file has correct connection details.
- Check logs for errors in the terminal where the server or client is running.
