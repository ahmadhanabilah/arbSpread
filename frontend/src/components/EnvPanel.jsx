import React, { useState, useEffect } from "react";
import "../styles/EnvPanel.css";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function EnvPanel() {
    const [envText, setEnvText] = useState("");
    const [user, setUser] = useState(localStorage.getItem("u") || "");
    const [pass, setPass] = useState(localStorage.getItem("p") || "");
    const [loggedIn, setLoggedIn] = useState(user && pass);
    const [saving, setSaving] = useState(false);
    const [loading, setLoading] = useState(false);
    const [message, setMessage] = useState("");

    const authHeader = {
        Authorization: "Basic " + btoa(unescape(encodeURIComponent(`${user}:${pass}`))),
        "Content-Type": "application/json",
    };

    async function fetchEnv() {
        setLoading(true);
        try {
            const res = await fetch(`${API_BASE}/api/env`, { headers: authHeader });
            if (!res.ok) throw new Error("Failed to fetch .env");
            const text = await res.text();
            setEnvText(text);
        } catch (e) {
            console.error(e);
            setMessage("⚠️ Failed to load environment file.");
        } finally {
            setLoading(false);
        }
    }

    async function saveEnv() {
        setSaving(true);
        setMessage("");
        try {
            const res = await fetch(`${API_BASE}/api/env`, {
                method: "PUT",
                headers: authHeader,
                body: JSON.stringify({ text: envText }),
            });
            if (res.ok) {
                setMessage("✅ .env saved successfully!");
            } else {
                setMessage("❌ Failed to save .env");
            }
        } catch (e) {
            console.error(e);
            setMessage("⚠️ Error saving .env file");
        } finally {
            setSaving(false);
        }
    }

    useEffect(() => {
        if (loggedIn) fetchEnv();
    }, [loggedIn]);

    if (!loggedIn) {
        return (
            <div className="card centered-card">
                <h2>Login</h2>
                <input
                    value={user}
                    onChange={(e) => setUser(e.target.value)}
                    placeholder="Username"
                />
                <input
                    type="password"
                    value={pass}
                    onChange={(e) => setPass(e.target.value)}
                    placeholder="Password"
                />
                <button
                    className="btn"
                    onClick={() => {
                        localStorage.setItem("u", user);
                        localStorage.setItem("p", pass);
                        setLoggedIn(true);
                    }}
                >
                    Login
                </button>
            </div>
        );
    }

    return (
        <div className="env-container">
                <div className="env-actions">
                    <button className="btn" onClick={fetchEnv}>
                        🔄 Reload
                    </button>
                    <button className="btn green" onClick={saveEnv} disabled={saving}>
                        💾 {saving ? "Saving..." : "Save"}
                    </button>
                </div>

                {loading ? (
                    <div className="loading-text">Loading...</div>
                ) : (
                    <textarea
                        value={envText}
                        onChange={(e) => setEnvText(e.target.value)}
                        spellCheck={false}
                    />
                )}

                {message && <div className="status-msg">{message}</div>}
        </div>
    );
}
