import React, { useEffect, useState } from "react";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function LogViewer({ symbol }) {
  const [logs, setLogs] = useState("Loading...");

  useEffect(() => {
    if (!symbol) {
      console.warn("⚠️ LogViewer: no symbol yet, skipping fetch");
      return;
    }

    const u = localStorage.getItem("u");
    const p = localStorage.getItem("p");
    const auth = "Basic " + btoa(`${u}:${p}`);

    async function fetchLogs() {
      const url = `${API_BASE}/api/logs/${symbol}?lines=300`;
      console.log("📡 Fetching logs from:", url);
      try {
        const res = await fetch(url, {
          headers: { Authorization: auth },
        });
        console.log("🔵 Response status:", res.status);
        const text = await res.text();
        setLogs(text);
      } catch (e) {
        console.error("❌ Fetch error:", e);
        setLogs("Error loading logs.");
      }
    }

    fetchLogs();
    const interval = setInterval(fetchLogs, 3000);
    return () => clearInterval(interval);
  }, [symbol]);

  return (
    <div
      style={{
        background: "#0d1117",
        color: "#e5e7eb",
        fontFamily: "monospace",
        padding: "12px",
        borderRadius: "8px",
        height: "400px",
        overflowY: "auto",
        whiteSpace: "pre-wrap",
      }}
    >
      {logs}
    </div>
  );
}
