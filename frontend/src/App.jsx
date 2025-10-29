// src/App.jsx
import React, { useState, useEffect } from "react";
import Login from "./components/Login";
import Dashboard from "./components/Dashboard";
import "./styles/App.css";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function App() {
    const [authed, setAuthed] = useState(false);
    const [checking, setChecking] = useState(true);

    // Auto-login only if credentials are valid
    useEffect(() => {
        const u = localStorage.getItem("u");
        const p = localStorage.getItem("p");

        if (!u || !p) {
            setChecking(false);
            return;
        }

        const token = btoa(`${u}:${p}`);
        fetch(`${API_BASE}/api/auth_check`, {
            headers: { Authorization: `Basic ${token}` },
        })
            .then((res) => {
                if (res.ok) {
                    setAuthed(true);
                } else {
                    localStorage.clear();
                    setAuthed(false);
                }
            })
            .catch(() => {
                localStorage.clear();
                setAuthed(false);
            })
            .finally(() => setChecking(false));
    }, []);

    if (checking) {
        return (
            <div style={{ textAlign: "center", marginTop: "30vh", fontSize: "1.2rem" }}>
                Checking credentials...
            </div>
        );
    }

    return authed ? (
        <Dashboard />
    ) : (
        <Login onLogin={() => setAuthed(true)} />
    );
}
