import React, { useEffect, useState } from "react";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function LiveTicker({ symbolL, symbolE }) {
  const [line, setLine] = useState("Waiting...");

  useEffect(() => {
    const u = localStorage.getItem("u");
    const p = localStorage.getItem("p");
    const auth = btoa(`${u}:${p}`);

    const url = `${API_BASE}/api/live_stream/${symbolL}/${symbolE}?auth=${encodeURIComponent(auth)}`;
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
  }, [symbolL, symbolE]);

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
        whiteSpace: "pre", 
        lineHeight: "1.4em",
      }}
    >
      {line}
    </div>
  );
}
