import React, { useEffect, useState } from "react";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function LiveTicker({ symbol }) {
  const [line, setLine] = useState("Waiting...");

  useEffect(() => {
    const u = localStorage.getItem("u");
    const p = localStorage.getItem("p");
    const auth = btoa(`${u}:${p}`);

    const url = `${API_BASE}/api/live_stream/${symbol}?auth=${encodeURIComponent(auth)}`;
    const eventSource = new EventSource(url);

    eventSource.onmessage = (e) => {
    // Decode the encoded \n back to real newlines
    setLine((e.data || "No data").replace(/\\n/g, "\n"));
    };


    eventSource.onerror = (err) => {
      console.error("SSE error:", err);
      setLine("Connection lost (retrying...)");
    };

    return () => eventSource.close();
  }, [symbol]);

  return (
    <div
      style={{
        background: "#0d1117",
        color: "#00ff99",
        fontFamily: "monospace",
        padding: "12px",
        borderRadius: "8px",
        height: "300px",
        overflowY: "auto",
        whiteSpace: "pre-wrap", // 👈 important
        lineHeight: "1.4em",
      }}
    >
      {line}
    </div>
  );
}
