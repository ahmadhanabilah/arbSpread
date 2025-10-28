// src/App.jsx
import React, { useState, useEffect } from "react";
import Login from "./components/Login";
import Dashboard from "./components/Dashboard";
import "./styles/App.css";

export default function App() {
  const [authed, setAuthed] = useState(false);

  useEffect(() => {
    const u = localStorage.getItem("u");
    const p = localStorage.getItem("p");
    if (u && p) setAuthed(true);
  }, []);

  return authed ? <Dashboard /> : <Login onLogin={() => setAuthed(true)} />;
}
