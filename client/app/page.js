"use client";

import { useState } from "react";
import { exportClickHouseToFlatFile } from "../api/clickhouse";

export default function DataIngestionTool() {
  const [source, setSource] = useState("");
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [database, setDatabase] = useState("");
  const [user, setUser] = useState("");
  const [password, setPassword] = useState("");
  const [jwtToken, setJwtToken] = useState("");
  const [filePath, setFilePath] = useState("");
  const [delimiter, setDelimiter] = useState(",");
  const [status, setStatus] = useState("");
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [tableName, setTableName] = useState("");

  const handleConnectClickHouse = async () => {
    try {
      const response = await fetch(
        "http://localhost:8080/api/connect-clickhouse",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ host, port, database, user, password }),
        }
      );
      const result = await response.text();
      setStatus(result);
    } catch (error) {
      setStatus("Error: " + error.message);
    }
  };

  const handleLoadClickHouseSchema = async () => {
    try {
      const params = new URLSearchParams({
        host,
        port,
        database,
        user,
        password,
        tableName,
      });
      const response = await fetch(
        `http://localhost:8080/api/clickhouse-schema?${params.toString()}`,
        {
          method: "GET",
        }
      );
      const result = await response.json();
      if (Array.isArray(result)) {
        setColumns(result);
      } else {
        setColumns([]);
        setStatus("Error: Unexpected response format");
      }
    } catch (error) {
      setStatus("Error: " + error.message);
    }
  };

  const handleLoadColumns = async () => {
    try {
      const response = await fetch(
        "http://localhost:8080/api/flatfile-schema",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ filePath, delimiter }),
        }
      );
      const result = await response.json();
      setColumns(result.columns || []);
    } catch (error) {
      setStatus("Error: " + error.message);
    }
  };

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      setFilePath(file.path || file.name);
    }
  };

  const handleUploadFile = async () => {
    if (!filePath) {
      setStatus("Error: Please select a file.");
      return;
    }

    try {
      const fileInput = document.querySelector('input[type="file"]');
      const file = fileInput.files[0];
      const formData = new FormData();
      formData.append("file", file);

      const response = await fetch("http://localhost:8080/api/upload-file", {
        method: "POST",
        body: formData,
      });

      const result = await response.text();
      setStatus(result);
    } catch (error) {
      setStatus("Error: " + error.message);
    }
  };

  const handleStartIngestion = async () => {
    if (!filePath) {
      setStatus("Error: Please select a file.");
      return;
    }
    try {
      const params = new URLSearchParams({
        source,
        target: filePath,
        columns: selectedColumns.join(","),
      });
      const response = await fetch(
        `http://localhost:8080/api/ingest-data?${params.toString()}`,
        {
          method: "POST",
        }
      );
      const result = await response.text();
      setStatus(result);
    } catch (error) {
      setStatus("Error: " + error.message);
    }
  };

  const handleExportClickHouseToFlatFile = async () => {
    if (!tableName || !filePath) {
      setStatus("Error: Please provide both table name and file name.");
      return;
    }
    try {
      const result = await exportClickHouseToFlatFile(
        tableName.trim(),
        filePath.trim()
      );
      setStatus(result);
    } catch (error) {
      setStatus("Error: " + error.message);
    }
  };

  return (
    <div className="p-6 bg-gray-100 min-h-screen">
      <h1 className="text-2xl font-bold mb-4">Data Ingestion Tool</h1>

      <div className="mb-6">
        <h2 className="text-lg font-semibold mb-2">Source Selection</h2>
        <select
          value={source}
          onChange={(e) => setSource(e.target.value)}
          className="p-2 border rounded w-full"
        >
          <option value="">Select Source</option>
          <option value="clickhouse">ClickHouse</option>
          <option value="flatfile">Flat File</option>
        </select>
      </div>

      {source === "clickhouse" && (
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-2">ClickHouse Connection</h3>
          <input
            placeholder="Host"
            value={host}
            onChange={(e) => setHost(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <input
            placeholder="Port"
            value={port}
            onChange={(e) => setPort(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <input
            placeholder="Database"
            value={database}
            onChange={(e) => setDatabase(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <input
            placeholder="User"
            value={user}
            onChange={(e) => setUser(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <input
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <button
            onClick={handleConnectClickHouse}
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Connect to ClickHouse
          </button>

          <h3 className="text-lg font-semibold mb-2 mt-4">
            ClickHouse Table Schema
          </h3>
          <input
            placeholder="Table Name"
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <button
            onClick={handleLoadClickHouseSchema}
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Load Schema
          </button>
          <div className="mt-4">
            <h3 className="text-lg font-semibold mb-2">Columns</h3>
            {columns.map((col) => (
              <div key={col} className="flex items-center mb-2">
                <input
                  type="checkbox"
                  value={col}
                  onChange={(e) => {
                    if (e.target.checked) {
                      setSelectedColumns([...selectedColumns, col]);
                    } else {
                      setSelectedColumns(
                        selectedColumns.filter((c) => c !== col)
                      );
                    }
                  }}
                  className="mr-2"
                />
                {col}
              </div>
            ))}
          </div>

          <h3 className="text-lg font-semibold mb-2 mt-4">ClickHouse Export</h3>
          <input
            placeholder="Table Name"
            value={tableName}
            onChange={(e) => setTableName(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <input
            placeholder="File Name"
            value={filePath}
            onChange={(e) => setFilePath(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <button
            onClick={handleExportClickHouseToFlatFile}
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Export to Flat File
          </button>
        </div>
      )}

      {source === "flatfile" && (
        <div className="mb-6">
          <h3 className="text-lg font-semibold mb-2">
            Flat File Configuration
          </h3>
          <input
            type="file"
            onChange={handleFileChange}
            className="p-2 border rounded w-full mb-2"
          />
          <p className="text-sm text-gray-600">Selected File: {filePath}</p>
          <input
            placeholder="Delimiter"
            value={delimiter}
            onChange={(e) => setDelimiter(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
          <button
            onClick={handleLoadColumns}
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Load Columns
          </button>
          <button
            onClick={handleUploadFile}
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Upload File
          </button>
          <div className="mt-4">
            <h3 className="text-lg font-semibold mb-2">Columns</h3>
            {columns.map((col) => (
              <div key={col} className="flex items-center mb-2">
                <input
                  type="checkbox"
                  value={col}
                  onChange={(e) => {
                    if (e.target.checked) {
                      setSelectedColumns([...selectedColumns, col]);
                    } else {
                      setSelectedColumns(
                        selectedColumns.filter((c) => c !== col)
                      );
                    }
                  }}
                  className="mr-2"
                />
                {col}
              </div>
            ))}
          </div>
        </div>
      )}

      <button
        onClick={handleStartIngestion}
        className="bg-green-500 text-white py-2 px-4 rounded hover:bg-green-600"
      >
        Start Ingestion
      </button>

      <div className="mt-6">
        <h3 className="text-lg font-semibold mb-2">Status</h3>
        <p className="text-gray-800">{status}</p>
      </div>
    </div>
  );
}
