// src/components/Login.jsx
import React, { useState } from "react";
import "../styles/Login.css";
import { FaEye, FaEyeSlash } from "react-icons/fa";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;


export default function Login({ onLogin }) {
    const [user, setUser] = useState("");
    const [pass, setPass] = useState("");
    const [show, setShow] = useState(false);
    const [err, setErr] = useState("");
    const [loading, setLoading] = useState(false);

async function handleLogin(e) {
    e.preventDefault();
    setErr("");
    setLoading(true);
    try {
        const token = btoa(`${user}:${pass}`);
        const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

        const res = await fetch(`${API_BASE}/api/auth_check`, {
            headers: { Authorization: `Basic ${token}` },
        });

        if (res.ok) {
            localStorage.setItem("u", user);
            localStorage.setItem("p", pass);
            onLogin?.();
        } else {
            setErr("❌ Invalid username or password");
            localStorage.clear();
        }
    } catch (e) {
        setErr("⚠️ Cannot connect to backend (check port 8000).");
    } finally {
        setLoading(false);
    }
}


    return (
        <div className="login-page">
            <form className="login-card" onSubmit={handleLogin}>
                <h2>🔐 Login</h2>
                <input
                    type="text"
                    placeholder="Username"
                    value={user}
                    onChange={(e) => setUser(e.target.value)}
                    autoFocus
                    required
                />
                <div className="password-wrapper">
                    <input
                        type={show ? "text" : "password"}
                        placeholder="Password"
                        value={pass}
                        onChange={(e) => setPass(e.target.value)}
                        required
                    />
                    <button
                        type="button"
                        className="toggle-btn"
                        onClick={() => setShow(!show)}
                        aria-label="toggle password visibility"
                    >
                        {show ? <FaEyeSlash /> : <FaEye />}
                    </button>

                </div>
                <button
                    type="submit"
                    className="login-btn"
                    disabled={loading || !user || !pass}
                >
                    {loading ? "Checking..." : "Login"}
                </button>
                {err && <p className="error-msg">{err}</p>}
            </form>
        </div>
    );
}
