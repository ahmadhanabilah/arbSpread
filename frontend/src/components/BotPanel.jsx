import React, { useEffect, useState } from "react";
import "../styles/BotPanel.css";
import LiveTicker from "./LiveTicker"; // 👈 added
import LogViewer from "./LogViewer";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;


export default function BotPanel() {
    const [symbols, setSymbols] = useState([]);
    const [running, setRunning] = useState([]);
    const [user, setUser] = useState(localStorage.getItem("u") || "");
    const [pass, setPass] = useState(localStorage.getItem("p") || "");
    const [loggedIn, setLoggedIn] = useState(user && pass);
    const [config, setConfig] = useState({ symbols: [] });
    const [loading, setLoading] = useState(false);
    const [notification, setNotification] = useState(null);

    const showNotification = (message, type = "info", timeout = 2500) => {
        setNotification({ message, type });
        setTimeout(() => setNotification(null), timeout);
    };

    const closeNotification = () => setNotification(null);

    // 👇 Live popup state
    const [showLive, setShowLive] = useState(false);
    const [liveSymbol, setLiveSymbol] = useState(null);

    const openLive = (pair) => {
        setLiveSymbol(pair);
        setShowLive(true);
    };


    const closeLive = () => {
        setShowLive(false);
        setLiveSymbol(null);
    };
    
    const [showLog, setShowLog] = useState(false);
    const [logSymbol, setLogSymbol] = useState(null);


    const openLog = (pair) => {
        console.log("🟢 openLog called with:", pair);
        setLogSymbol(pair);
        setShowLog(true);
    };


    const closeLog = () => {
        setShowLog(false);
        setLogSymbol(null);
    };


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
        // ✅ Before saving, normalize numeric fields
        const numericKeys = [
            "TRADES_INTERVAL",
            "MIN_SPREAD",
            "SPREAD_MULTIPLIER",
            "SPREAD_TP",
            "MIN_TRADE_VALUE",
            "MAX_TRADE_VALUE_ENTRY",
            "MAX_TRADE_VALUE_EXIT",
            "PERC_OF_OB",
            "INV_LEVEL_TO_MULT",
            "MAX_INVENTORY_VALUE",
        ];

        const cleanedSymbols = symbols.map((sym) => {
            const cleaned = { ...sym };
            for (const key of numericKeys) {
                let v = cleaned[key];
                if (typeof v === "string") v = v.replace(",", ".");
                const num = parseFloat(v);
                if (!isNaN(num)) cleaned[key] = num;
            }
            return cleaned;
        });

        const payload = { data: { symbols: cleanedSymbols } };

        const res = await fetchAuth(`${API_BASE}/api/config`, {
            method: "PUT",
            body: JSON.stringify(payload),
        });

        if (res.ok) {
            showNotification("✅ Config saved!", "success");
            loadSymbols();
        } else {
            showNotification("❌ Failed to save config", "error");
        }

    }

    async function startBot(row) {
        await fetchAuth(`${API_BASE}/api/start?symbolL=${row.symbolL}&symbolE=${row.symbolE}`, { method: "POST" });
        loadSymbols();
    }

    async function stopBot(row) {
        await fetchAuth(`${API_BASE}/api/stop?symbolL=${row.symbolL}&symbolE=${row.symbolE}`, { method: "POST" });
        loadSymbols();
    }


    function addSymbol() {
        setSymbols([
            ...symbols,
            {
                symbolL: "NEW",
                symbolE: "NEW-USD",
                MIN_SPREAD: 0.3,
                SPREAD_MULTIPLIER: 1.1,
                SPREAD_TP: 0.2,
                MIN_TRADE_VALUE: 100,
                MAX_TRADE_VALUE_ENTRY: 200,
                MAX_TRADE_VALUE_EXIT: 200,
                MAX_INVENTORY_VALUE: 1000,
                PERC_OF_OB: 30,
                INV_LEVEL_TO_MULT: 5,
                TRADES_INTERVAL: 1,
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

    function handleNumericChange(i, key, val) {
        // Always treat as string for controlled input
        if (typeof val !== "string") val = String(val ?? "");

        // Convert commas to dots for internal consistency
        val = val.replace(",", ".");

        // Allow digits, one dot, and one leading minus
        val = val.replace(/[^0-9.\-]/g, "");

        // Only one dot allowed
        const dotCount = (val.match(/\./g) || []).length;
        if (dotCount > 1) {
            // Keep first dot, remove later ones
            const firstDot = val.indexOf(".");
            val = val.slice(0, firstDot + 1) + val.slice(firstDot + 1).replace(/\./g, "");
        }

        // Only one minus sign at the start
        if (val.includes("-") && !val.startsWith("-")) {
            val = "-" + val.replace(/-/g, "");
        }

        // ✅ Allow partial inputs like "-", "1.", or "-0."
        updateValue(i, key, val);
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
        <div className="botpanel-main-container">
            <div className="buttons-container">
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

            <div className="botpanel-table-container">
                <table id="symbolsTable">
                    <thead>
                        <tr>
                            <th>Action</th>
                            <th>Symbol Lighter</th>
                            <th>Symbol Extended</th>
                            <th>Status</th>
                            <th>TRADES INTERVAL</th>
                            <th>MIN SPREAD ENTRY</th>
                            <th>SPREAD MULTIPLIER</th>
                            <th>MIN SPREAD EXIT DIFF</th>
                            <th>MIN TRADE VALUE</th>
                            <th>MAX TRADE VALUE ENTRY</th>
                            <th>MAX TRADE VALUE EXIT</th>
                            <th>PERC OF OB</th>
                            <th>MAX INVENTORY VALUE</th>
                            <th>INV LEVEL TO MULT SPREAD</th>
                        </tr>
                    </thead>

                    <tbody>
                        {symbols.length === 0 ? (
                            <tr>
                                <td colSpan="9">Loading...</td>
                            </tr>
                        ) : (
                            symbols.map((row, i) => {
                                const isRunning = running.includes(`arb_${row.symbolL}_${row.symbolE}`);
                                return (
                                    <tr key={i}>
                                        <td>
                                            <div className="action-buttons">
                                                {!isRunning && (
                                                    <button
                                                        className="btn start"
                                                        onClick={() => startBot(row)}
                                                    >
                                                        ▶ Start
                                                    </button>
                                                )}
                                                {isRunning && (
                                                    <button
                                                        className="btn stop"
                                                        onClick={() => stopBot(row)}
                                                    >
                                                        ⏹ Stop
                                                    </button>
                                                )}
                                                <button
                                                    className="btn delete"
                                                    onClick={() => removeSymbol(i)}
                                                >
                                                    🗑 Delete
                                                </button>
                                                <button
                                                    className="btn live"
                                                    onClick={() => openLive({ symbolL: row.symbolL, symbolE: row.symbolE })}
                                                >
                                                    📡 Live
                                                </button>

                                                <button
                                                    className="btn log"
                                                    onClick={() => openLog({ symbolL: row.symbolL, symbolE: row.symbolE })}
                                                >
                                                    📜 Logs
                                                </button>
                                            </div>
                                        </td>
                                        <td>
                                            <input
                                                value={row.symbolL}
                                                onChange={(e) =>
                                                    updateValue(i, "symbolL", e.target.value.toUpperCase())
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                value={row.symbolE}
                                                onChange={(e) =>
                                                    updateValue(i, "symbolE", e.target.value.toUpperCase())
                                                }
                                            />
                                        </td>
                                        <td>{isRunning ? "🟢" : "⚫"}</td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.TRADES_INTERVAL}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "TRADES_INTERVAL", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.MIN_SPREAD}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "MIN_SPREAD", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.SPREAD_MULTIPLIER}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "SPREAD_MULTIPLIER", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.SPREAD_TP}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "SPREAD_TP", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.MIN_TRADE_VALUE}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "MIN_TRADE_VALUE", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.MAX_TRADE_VALUE_ENTRY}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "MAX_TRADE_VALUE_ENTRY", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.MAX_TRADE_VALUE_EXIT}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "MAX_TRADE_VALUE_EXIT", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.PERC_OF_OB}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "PERC_OF_OB", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.MAX_INVENTORY_VALUE}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "MAX_INVENTORY_VALUE", (e.target.value))
                                                }
                                            />
                                        </td>
                                        <td>
                                            <input
                                                type="text"
                                                value={row.INV_LEVEL_TO_MULT}
                                                onChange={(e) =>
                                                    handleNumericChange(i, "INV_LEVEL_TO_MULT", (e.target.value))
                                                }
                                            />
                                        </td>
                                    </tr>
                                );
                            })
                        )}
                    </tbody>
                </table>
            </div>


            {/* 👇 LiveTicker popup modal */}
            {showLive && (
                <div
                    className="modal-overlay"
                    onClick={closeLive}
                    style={{
                        position: "fixed",
                        top: 0,
                        left: 0,
                        right: 0,
                        bottom: 0,
                        background: "rgba(0,0,0,0.6)",
                        display: "flex",
                        justifyContent: "center",
                        alignItems: "center",
                        zIndex: 9999,
                    }}
                >
                    <div
                        className="modal-content"
                        onClick={(e) => e.stopPropagation()}
                        style={{
                            background: "#1e1e1e",
                            padding: "20px",
                            borderRadius: "10px",
                            width: "600px",
                            maxWidth: "80%",
                            color: "#fff",
                        }}
                    >
                        <LiveTicker symbolL={liveSymbol?.symbolL} symbolE={liveSymbol?.symbolE} />
                        <button
                            onClick={closeLive}
                            style={{
                                marginTop: "15px",
                                padding: "6px 12px",
                                background: "#444",
                                color: "#fff",
                                border: "none",
                                borderRadius: "6px",
                                cursor: "pointer",
                            }}
                        >
                            Close
                        </button>
                    </div>
                </div>
            )}

            {/* 👇 LogViewer popup modal (must be OUTSIDE of showLive block) */}
            {showLog && (
                <div
                    className="modal-overlay"
                    onClick={closeLog}
                    style={{
                        position: "fixed",
                        top: 0,
                        left: 0,
                        right: 0,
                        bottom: 0,
                        background: "rgba(0,0,0,0.6)",
                        display: "flex",
                        justifyContent: "center",
                        alignItems: "center",
                        zIndex: 9999,
                    }}
                >
                    <div
                        className="modal-content"
                        onClick={(e) => e.stopPropagation()}
                        style={{
                            background: "#1e1e1e",
                            padding: "20px",
                            borderRadius: "10px",
                            width: "600px",
                            maxWidth: "80%",
                            color: "#fff",
                        }}
                    >
                        <LogViewer symbolL={logSymbol?.symbolL} symbolE={logSymbol?.symbolE} />
                        <button
                            onClick={closeLog}
                            style={{
                                marginTop: "15px",
                                padding: "6px 12px",
                                background: "#444",
                                color: "#fff",
                                border: "none",
                                borderRadius: "6px",
                                cursor: "pointer",
                            }}
                        >
                            Close
                        </button>
                    </div>
                </div>
            )}

            {/* 👇 Notification popup */}
            {notification && (
                <div
                    className="modal-overlay"
                    onClick={closeNotification}
                    style={{
                        position: "fixed",
                        bottom: "30px",
                        right: "30px",
                        color: "#fff",
                        zIndex: 9999,
                        cursor: "pointer",
                    }}
                >
                    <div
                        className="modal-content"
                        onClick={(e) => e.stopPropagation()}
                        style={{
                            background:
                                notification.type === "error"
                                    ? "#ff0000ff"
                                    : "#ffffffff",
                            color: "#000000ff",
                            padding: "16px 24px",
                            borderRadius: "8px",
                            boxShadow:
                                "0 4px 8px rgba(0, 0, 0, 0.2)",
                            textAlign: "center",
                            minWidth: "220px",
                            fontSize: "1rem",
                        }}
                    >
                        {notification.message}
                    </div>
                </div>
            )}


        </div>
    );
}
