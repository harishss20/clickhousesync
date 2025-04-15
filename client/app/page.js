"use client";

import { useState } from "react";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";

export default function DataIngestionTool() {
  const [source, setSource] = useState("");
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [database, setDatabase] = useState("");
  const [user, setUser] = useState("");
  const [password, setPassword] = useState("");
  const [filePath, setFilePath] = useState("");
  const [delimiter, setDelimiter] = useState(",");
  const [status, setStatus] = useState("");
  const [columns, setColumns] = useState([]);
  const [selectedColumns, setSelectedColumns] = useState([]);
  const [tableName, setTableName] = useState("");
  const [tables, setTables] = useState([]);
  const [table1, setTable1] = useState("");
  const [table2, setTable2] = useState("");
  const [joinCondition, setJoinCondition] = useState("");
  const [targetTable, setTargetTable] = useState("");
  const [progress, setProgress] = useState(0);
  const [previewData, setPreviewData] = useState([]);

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

  const handleLoadClickHouseTables = async () => {
    try {
      const params = new URLSearchParams({
        host,
        port,
        database,
        user,
        password,
      });
      const response = await fetch(
        `http://localhost:8080/api/clickhouse-tables?${params.toString()}`,
        {
          method: "GET",
        }
      );
      const result = await response.json();
      setTables(result);
      toast.success("Tables loaded successfully");
    } catch (error) {
      toast.error("Error: " + error.message);
    }
  };

  const handleTableSelection = async (tableName) => {
    setTableName(tableName);
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
      setColumns(result);
      toast.success("Schema loaded successfully");
    } catch (error) {
      toast.error("Error: " + error.message);
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
    if (!tableName || !filePath || selectedColumns.length === 0) {
      toast.error(
        "Error: Please provide table name, file name, and select at least one column."
      );
      return;
    }

    try {
      const response = await fetch(
        "http://localhost:8080/api/export-clickhouse-to-flatfile",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            tableName: tableName.trim(),
            fileName: filePath.trim(),
            columns: selectedColumns, // Pass selected columns as an array
          }),
        }
      );

      const result = await response.text();
      toast.success(result);
    } catch (error) {
      toast.error("Error: " + error.message);
    }
  };

  const handleJoinTables = async () => {
    if (
      !table1 ||
      !table2 ||
      !joinCondition ||
      !targetTable ||
      selectedColumns.length === 0
    ) {
      toast.error(
        "Error: Please provide both tables, join condition, target table name, and select at least one column."
      );
      return;
    }

    try {
      const response = await fetch(
        "http://localhost:8080/api/join-clickhouse-tables",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            table1: table1.trim(),
            table2: table2.trim(),
            joinCondition: joinCondition.trim(),
            targetTable: targetTable.trim(),
            columns: selectedColumns, // Pass selected columns
          }),
        }
      );

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText);
      }

      const result = await response.text();
      toast.success(result);
    } catch (error) {
      toast.error("Error: " + error.message);
    }
  };

  const handlePreviewData = async () => {
    if (!tableName || selectedColumns.length === 0) {
      toast.error("Error: Please select a table and at least one column.");
      return;
    }

    try {
      const response = await fetch("http://localhost:8080/api/preview-data", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tableName: tableName.trim(),
          columns: selectedColumns,
        }),
      });

      const result = await response.json();
      setPreviewData(result);
      toast.success("Preview data loaded successfully.");
    } catch (error) {
      toast.error("Error: " + error.message);
    }
  };

  const handleIngestionWithProgress = async () => {
    if (!filePath || selectedColumns.length === 0) {
      toast.error(
        "Error: Please provide a file path and select at least one column."
      );
      return;
    }

    try {
      const response = await fetch(
        "http://localhost:8080/api/ingest-with-progress",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            filePath: filePath.trim(),
            columns: selectedColumns,
          }),
        }
      );

      const reader = response.body.getReader();
      const contentLength = +response.headers.get("Content-Length");
      let receivedLength = 0;
      let chunks = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        chunks.push(value);
        receivedLength += value.length;
        setProgress(Math.round((receivedLength / contentLength) * 100));
      }

      toast.success("Ingestion completed successfully.");
    } catch (error) {
      toast.error("Error: " + error.message);
    }
  };

  const handleIngestFlatFile = async () => {
    if (!filePath || !tableName) {
      toast.error("Error: Please select a file and specify a target table.");
      return;
    }

    try {
      const formData = new FormData();
      formData.append("file", filePath);
      formData.append("target", tableName);

      const response = await fetch("http://localhost:8080/api/ingest-data", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(errorText);
      }

      const result = await response.text();
      toast.success(result);
    } catch (error) {
      toast.error("Error: " + error.message);
    }
  };

  return (
    <div className="p-6 bg-gray-100 min-h-screen">
      <ToastContainer />
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

          <h3 className="text-lg font-semibold mb-2 mt-4">ClickHouse Tables</h3>
          <button
            onClick={handleLoadClickHouseTables}
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Load Tables
          </button>
          <ul className="mt-4">
            {tables.map((table) => (
              <li
                key={table}
                className="cursor-pointer text-blue-600 hover:underline"
                onClick={() => handleTableSelection(table)}
              >
                {table}
              </li>
            ))}
          </ul>
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

      <div className="mb-6">
        <h2 className="text-lg font-semibold mb-2">Flat File Ingestion</h2>
        <input
          type="file"
          onChange={(e) => setFilePath(e.target.files[0])}
          className="p-2 border rounded w-full mb-2"
        />
        <input
          placeholder="Target Table Name"
          value={tableName}
          onChange={(e) => setTableName(e.target.value)}
          className="p-2 border rounded w-full mb-2"
        />
        <button
          onClick={handleIngestFlatFile}
          className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
        >
          Ingest Flat File
        </button>
      </div>

      <div className="mb-6">
        <h2 className="text-lg font-semibold mb-2">Join ClickHouse Tables</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700">
              Table 1
            </label>
            <input
              placeholder="Enter first table name"
              value={table1}
              onChange={(e) => setTable1(e.target.value)}
              className="p-2 border rounded w-full mb-2"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700">
              Table 2
            </label>
            <input
              placeholder="Enter second table name"
              value={table2}
              onChange={(e) => setTable2(e.target.value)}
              className="p-2 border rounded w-full mb-2"
            />
          </div>
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700">
            Join Condition
          </label>
          <input
            placeholder="e.g., table1.id = table2.id"
            value={joinCondition}
            onChange={(e) => setJoinCondition(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700">
            Target Table Name
          </label>
          <input
            placeholder="Enter target table name"
            value={targetTable}
            onChange={(e) => setTargetTable(e.target.value)}
            className="p-2 border rounded w-full mb-2"
          />
        </div>
        <div className="mt-4">
          <h3 className="text-lg font-semibold mb-2">Select Columns</h3>
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
        <button
          onClick={handleJoinTables}
          className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600 mt-4"
        >
          Join Tables
        </button>
      </div>

      <div className="mb-6">
        <h2 className="text-lg font-semibold mb-2">Data Preview</h2>
        <button
          onClick={handlePreviewData}
          className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
        >
          Preview Data
        </button>
        <div className="mt-4">
          {previewData.length > 0 && (
            <table className="table-auto border-collapse border border-gray-300 w-full">
              <thead>
                <tr>
                  {Object.keys(previewData[0]).map((key) => (
                    <th key={key} className="border border-gray-300 px-4 py-2">
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {previewData.map((row, index) => (
                  <tr key={index}>
                    {Object.values(row).map((value, i) => (
                      <td key={i} className="border border-gray-300 px-4 py-2">
                        {value}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      <div className="mb-6">
        <h2 className="text-lg font-semibold mb-2">Ingestion Progress</h2>
        <button
          onClick={handleIngestionWithProgress}
          className="bg-green-500 text-white py-2 px-4 rounded hover:bg-green-600"
        >
          Start Ingestion
        </button>
        <div className="mt-4">
          <div className="w-full bg-gray-200 rounded-full h-4">
            <div
              className="bg-blue-500 h-4 rounded-full"
              style={{ width: `${progress}%` }}
            ></div>
          </div>
          <p className="text-sm text-gray-600 mt-2">{progress}% completed</p>
        </div>
      </div>

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

      <div>
        <p>Ensure all JSX elements are properly closed and nested.</p>
      </div>
    </div>
  );
}
