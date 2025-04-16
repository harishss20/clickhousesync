"use client";

import { useState } from "react";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import {
  handleConnectClickHouse,
  handleLoadClickHouseSchema,
  handleLoadClickHouseTables,
  handleExportClickHouseToFlatFile,
  handleTableSelection,
  handlePreviewData,
  handleIngestFlatFile,
} from "@/api/clickhouse";
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
  const [progress, setProgress] = useState(0);
  const [previewData, setPreviewData] = useState([]);

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    if (file) {
      setFilePath(file.path || file.name);
    }
  };

  const handleIngestionWithProgress = async () => {
    if (!filePath) {
      toast.error("Error: Please provide a file path.");
      return;
    }

    if (selectedColumns.length === 0) {
      toast.error("Error: Please select at least one column.");
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
      const decoder = new TextDecoder("utf-8");
      let totalRecords = 0;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const chunk = decoder.decode(value, { stream: true });
        const match = chunk.match(/data: (\d+)/); // Extract progress value
        if (match) {
          setProgress(parseInt(match[1]));
        }

        const recordMatch = chunk.match(/totalRecords: (\d+)/); // Extract total records
        if (recordMatch) {
          totalRecords = parseInt(recordMatch[1]);
        }
      }

      toast.success(
        `Ingestion completed successfully. Total records: ${totalRecords}`
      );
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
            onClick={() =>
              handleConnectClickHouse(
                host,
                port,
                database,
                user,
                password,
                setStatus
              )
            }
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
            onClick={() =>
              handleLoadClickHouseSchema(
                host,
                port,
                database,
                user,
                password,
                tableName,
                setColumns,
                setStatus
              )
            }
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
            onClick={() =>
              handleExportClickHouseToFlatFile(
                tableName,
                filePath,
                selectedColumns
              )
            }
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Export to Flat File
          </button>

          <h3 className="text-lg font-semibold mb-2 mt-4">ClickHouse Tables</h3>
          <button
            onClick={() =>
              handleLoadClickHouseTables(
                host,
                port,
                database,
                user,
                password,
                setTables
              )
            }
            className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
          >
            Load Tables
          </button>
          <ul className="mt-4">
            {tables.map((table) => (
              <li
                key={table}
                className="cursor-pointer text-blue-600 hover:underline"
                onClick={() =>
                  handleTableSelection(
                    host,
                    port,
                    database,
                    user,
                    password,
                    table,
                    setTableName,
                    setColumns
                  )
                }
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
          onClick={() =>
            handleIngestFlatFile(filePath, selectedColumns, tableName)
          }
          className="bg-blue-500 text-white py-2 px-4 rounded hover:bg-blue-600"
        >
          Ingest Flat File
        </button>
      </div>

      <div className="mb-6">
        <h2 className="text-lg font-semibold mb-2">Data Preview</h2>
        <button
          onClick={() =>
            handlePreviewData(tableName, selectedColumns, setPreviewData)
          }
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
    </div>
  );
}
