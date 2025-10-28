import React, { useEffect, useState } from "react";
import "../styles/BotPanel.css";

const API_BASE = "";

export default function BotPanel() {
    const [symbols, setSymbols] = useState([]);
    const [running, setRunning] = useState([]);
    const [user, setUser] = useState(localStorage.getItem("u") || "");
    const [pass, setPass] = useState(localStorage.getItem("p") || "");
    const [loggedIn, setLoggedIn] = useState(user && pass);
    const [config, setConfig] = useState({ symbols: [] });
    const [loading, setLoading] = useState(false);

    const authHeader = {
        Authorization:
            "Basic " + btoa(unescape(encodeURIComponent(`${user}:${pass}`))),
        "Content-Type": "application/json",
    };

    function login() {
        localStorage.setItem("u", user);
        localStorage.setItem("p", pass);
        setLoggedIn(true);
    }

    function logout() {
        localStorage.removeItem("u");
        localStorage.removeItem("p");
        setUser("");
        setPass("");
        setLoggedIn(false);
    }

    async function fetchAuth(url, opts = {}) {
        opts.headers = { ...(opts.headers || {}), ...authHeader };
        const r = await fetch(url, opts);
        if (r.status === 401) {
            alert("❌ Login failed");
            logout();
        }
        return r;
    }

    async function loadSymbols() {
        setLoading(true);
        try {
            const symRes = await fetchAuth(`${API_BASE}/api/symbols`);
            const cfgRes = await fetchAuth(`${API_BASE}/api/config`);

            if (!symRes.ok || !cfgRes.ok) throw new Error("API error");

            const symJson = await symRes.json();
            const cfgJson = await cfgRes.json();

            setSymbols(cfgJson.symbols || []);
            setRunning(symJson.running || []);
            setConfig(cfgJson);
        } catch (e) {
            console.error("Error loading symbols:", e);
            alert("Failed to load config or symbols.");
        } finally {
            setLoading(false);
        }
    }

    async function saveConfig() {
        const payload = { data: { symbols } };
        const res = await fetchAuth(`${API_BASE}/api/config`, {
            method: "PUT",
            body: JSON.stringify(payload),
        });
        if (res.ok) {
            alert("✅ Config saved!");
            loadSymbols();
        } else {
            alert("❌ Failed to save config");
        }
    }

    async function startBot(symbol) {
        await fetchAuth(`${API_BASE}/api/start/${symbol}`, { method: "POST" });
        loadSymbols();
    }

    async function stopBot(symbol) {
        await fetchAuth(`${API_BASE}/api/stop/${symbol}`, { method: "POST" });
        loadSymbols();
    }

    function addSymbol() {
        setSymbols([
            ...symbols,
            {
                symbol: "NEW",
                MIN_SPREAD: 0.3,
                SPREAD_TP: 0.2,
                MIN_TRADE_VALUE: 50,
                MAX_TRADE_VALUE: 500,
                MAX_INVENTORY_VALUE: 1000,
                PERC_OF_OB: 50,
                CHECK_SPREAD_INTERVAL: 1,
                SHOW_LIVE_SPREAD: false,
            },
        ]);
    }

    function removeSymbol(i) {
        const updated = [...symbols];
        updated.splice(i, 1);
        setSymbols(updated);
    }

    function updateValue(i, key, val) {
        const updated = [...symbols];
        updated[i][key] = val;
        setSymbols(updated);
    }

    useEffect(() => {
        if (loggedIn) loadSymbols();
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
                <button className="btn" onClick={login}>
                    Login
                </button>
            </div>
        );
    }

    return (
        <div className="main-container">

            <div className="card">
            
                <h3 className="table-title">Symbols Management</h3>
            
                <div className="table-actions">
                    <button className="btn" onClick={loadSymbols}>
                        🔄 Reload
                    </button>
                    <button className="btn green" onClick={addSymbol}>
                        ➕ Add Symbol
                    </button>
                    <button className="btn" onClick={saveConfig}>
                        💾 Save Config
                    </button>
                    <button className="btn" onClick={logout}>
                        🚪 Logout
                    </button>
                </div>

                <div className="table-container">
                    <table id="symbolsTable">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Status</th>
                                <th>MIN SPREAD</th>
                                <th>SPREAD TP</th>
                                <th>MIN TRADE VALUE</th>
                                <th>MAX TRADE VALUE</th>
                                <th>MAX INVENTORY VALUE</th>
                                <th>PERC OF OB</th>
                                <th>Action</th>
                            </tr>
                        </thead>

                        <tbody>
                            {symbols.length === 0 ? (
                                <tr>
                                    <td colSpan="9">Loading...</td>
                                </tr>
                            ) : (
                                symbols.map((row, i) => {
                                    const isRunning = running.includes(`arb_${row.symbol}`);
                                    return (
                                        <tr key={i}>
                                            <td>
                                                <input
                                                    value={row.symbol}
                                                    onChange={(e) =>
                                                        updateValue(i, "symbol", e.target.value.toUpperCase())
                                                    }
                                                />
                                            </td>
                                            <td>{isRunning ? "🟢" : "⚫"}</td>
                                            <td>
                                                <input
                                                    type="number"
                                                    value={row.MIN_SPREAD}
                                                    onChange={(e) =>
                                                        updateValue(i, "MIN_SPREAD", parseFloat(e.target.value))
                                                    }
                                                />
                                            </td>
                                            <td>
                                                <input
                                                    type="number"
                                                    value={row.SPREAD_TP}
                                                    onChange={(e) =>
                                                        updateValue(i, "SPREAD_TP", parseFloat(e.target.value))
                                                    }
                                                />
                                            </td>
                                            <td>
                                                <input
                                                    type="number"
                                                    value={row.MIN_TRADE_VALUE}
                                                    onChange={(e) =>
                                                        updateValue(i, "MIN_TRADE_VALUE", parseFloat(e.target.value))
                                                    }
                                                />
                                            </td>
                                            <td>
                                                <input
                                                    type="number"
                                                    value={row.MAX_TRADE_VALUE}
                                                    onChange={(e) =>
                                                        updateValue(i, "MAX_TRADE_VALUE", parseFloat(e.target.value))
                                                    }
                                                />
                                            </td>
                                            <td>
                                                <input
                                                    type="number"
                                                    value={row.MAX_INVENTORY_VALUE}
                                                    onChange={(e) =>
                                                        updateValue(
                                                            i,
                                                            "MAX_INVENTORY_VALUE",
                                                            parseFloat(e.target.value)
                                                        )
                                                    }
                                                />
                                            </td>
                                            <td>
                                                <input
                                                    type="number"
                                                    value={row.PERC_OF_OB}
                                                    onChange={(e) =>
                                                        updateValue(i, "PERC_OF_OB", parseFloat(e.target.value))
                                                    }
                                                />
                                            </td>
                                            <td>
                                                <div className="action-buttons">
                                                    {!isRunning && (
                                                        <button
                                                            className="btn start"
                                                            onClick={() => startBot(row.symbol)}
                                                        >
                                                            ▶
                                                        </button>
                                                    )}
                                                    {isRunning && (
                                                        <button
                                                            className="btn stop"
                                                            onClick={() => stopBot(row.symbol)}
                                                        >
                                                            ⏹
                                                        </button>
                                                    )}
                                                    <button
                                                        className="btn delete"
                                                        onClick={() => removeSymbol(i)}
                                                    >
                                                        🗑
                                                    </button>
                                                </div>
                                            </td>
                                        </tr>
                                    );
                                })
                            )}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}
