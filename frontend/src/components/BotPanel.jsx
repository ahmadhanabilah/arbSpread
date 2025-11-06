// src/components/BotPanel.jsx
import React, { useEffect, useState } from "react";
import "../styles/BotPanel.css";
import LiveTicker from "./LiveTicker";
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

    const [showLive, setShowLive] = useState(false);
    const [liveSymbol, setLiveSymbol] = useState(null);

    const [showLog, setShowLog] = useState(false);
    const [logSymbol, setLogSymbol] = useState(null);

    const [showEdit, setShowEdit] = useState(false);
    const [editSymbol, setEditSymbol] = useState(null);
    const [tempConfig, setTempConfig] = useState(null);

    const [showDelete, setShowDelete] = useState(false);
    const [deleteSymbol, setDeleteSymbol] = useState(null);

    const showNotification = (message, type = "info", timeout = 1000) => {
        setNotification({ message, type });
        setTimeout(() => setNotification(null), timeout);
    };

    const authHeader = {
        Authorization: "Basic " + btoa(unescape(encodeURIComponent(`${user}:${pass}`))),
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

    async function saveConfig(newSymbols = symbols) {
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

        const cleanedSymbols = newSymbols.map((sym) => {
            const cleaned = { ...sym };
            for (const key of numericKeys) {
                let v = cleaned[key];
                if (typeof v === "string") v = v.replace(",", ".");
                const num = parseFloat(v);
                if (!isNaN(num)) cleaned[key] = num;
            }

            // Ensure keys are uppercase, consistent naming
            cleaned.SYMBOL_LIGHTER = cleaned.SYMBOL_LIGHTER?.toUpperCase?.() || cleaned.SYMBOL_LIGHTER;
            cleaned.SYMBOL_EXTENDED = cleaned.SYMBOL_EXTENDED?.toUpperCase?.() || cleaned.SYMBOL_EXTENDED;

            return cleaned;
        });

        const payload = { data: { symbols: cleanedSymbols } };

        const res = await fetchAuth(`${API_BASE}/api/config`, {
            method: "PUT",
            body: JSON.stringify(payload),
        });

        if (res.ok) {
            loadSymbols();
        }
    }


    async function startBot(row) {
        await fetchAuth(`${API_BASE}/api/start?symbolL=${row.SYMBOL_LIGHTER}&symbolE=${row.SYMBOL_EXTENDED}`, { method: "POST" });
        loadSymbols();
    }

    async function stopBot(row) {
        await fetchAuth(`${API_BASE}/api/stop?symbolL=${row.SYMBOL_LIGHTER}&symbolE=${row.SYMBOL_EXTENDED}`, { method: "POST" });
        loadSymbols();
    }

    function addSymbol() {
        setSymbols([
            ...symbols,
            {
                SYMBOL_LIGHTER: "NEW",
                SYMBOL_EXTENDED: "NEW-USD",
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

    // =============== MODALS ===================
    const openLive = (pair) => {
        setLiveSymbol(pair);
        setShowLive(true);
    };
    const closeLive = () => {
        setShowLive(false);
        setLiveSymbol(null);
    };

    const openLog = (pair) => {
        setLogSymbol(pair);
        setShowLog(true);
    };
    const closeLog = () => {
        setShowLog(false);
        setLogSymbol(null);
    };

    const openEditConfig = (row) => {
        setEditSymbol(row);
        setTempConfig({ ...row });
        setShowEdit(true);
    };
    const closeEdit = () => {
        setShowEdit(false);
        setEditSymbol(null);
        setTempConfig(null);
    };

    const openDeleteConfirm = (row) => {
        setDeleteSymbol(row);
        setShowDelete(true);
    };
    const closeDelete = () => {
        setDeleteSymbol(null);
        setShowDelete(false);
    };

    const confirmDelete = () => {
        const updated = symbols.filter(
            (s) => !(s.SYMBOL_LIGHTER === deleteSymbol.SYMBOL_LIGHTER && s.SYMBOL_EXTENDED === deleteSymbol.SYMBOL_EXTENDED)
        );
        saveConfig(updated);
        closeDelete();
        closeEdit(); // ✅ also close edit modal
    };


    const handleEditChange = (key, val) => {
        setTempConfig((prev) => ({ ...prev, [key]: val }));
    };

    const saveEditedConfig = () => {
        const updated = symbols.map((s) =>
            s.SYMBOL_LIGHTER === editSymbol.SYMBOL_LIGHTER && s.SYMBOL_EXTENDED === editSymbol.SYMBOL_EXTENDED ? tempConfig : s
        );
        saveConfig(updated);
        closeEdit();
    };

    useEffect(() => {
        if (loggedIn) loadSymbols();
    }, [loggedIn]);

    if (!loggedIn) {
        return (
            <div className="card centered-card">
                <h2>Login</h2>
                <input value={user} onChange={(e) => setUser(e.target.value)} placeholder="Username" />
                <input type="password" value={pass} onChange={(e) => setPass(e.target.value)} placeholder="Password" />
                <button className="btn" onClick={login}>
                    Login
                </button>
            </div>
        );
    }

    return (
        <div className="botpanel-main-container">
            {/* Top buttons */}
            <div className="buttons-container">
                <button className="btn" onClick={loadSymbols}>🔄 Reload</button>
                <button className="btn" onClick={addSymbol}>➕ Add Symbol</button>
                <button className="btn" onClick={logout}>🚪 Logout</button>
            </div>

            {/* Symbols Table */}
            <div className="botpanel-table-container">
                <table id="symbolsTable">
                    <thead>
                        <tr>
                            <th>Lighter</th>
                            <th>Extended</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {symbols.length === 0 ? (
                            <tr><td colSpan="3">Loading...</td></tr>
                        ) : (
                            symbols.map((row, i) => {
                                const isRunning = running.includes(`arb_${row.SYMBOL_LIGHTER}_${row.SYMBOL_EXTENDED}`);
                                return (
                                    <tr key={i}>
                                        <td>{row.SYMBOL_LIGHTER}</td>
                                        <td>{row.SYMBOL_EXTENDED}</td>
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
                                                    <button className="btn stop" onClick={() => stopBot(row)}>⏹ Stop</button>
                                                )}
                                                <button className="btn " onClick={() => openEditConfig(row)}>⚙️ Edit</button>
                                                <button className="btn" onClick={() => openLive(row)}>📡 Live</button>
                                                <button className="btn" onClick={() => openLog(row)}>📜 Log</button>
                                            </div>
                                        </td>
                                    </tr>
                                );
                            })
                        )}
                    </tbody>
                </table>
            </div>

            {/* Live Modal */}
            {showLive && (
                <div className="modal-overlay" onClick={closeLive}>
                    <div className="modal-content" onClick={(e) => e.stopPropagation()}>
                        <LiveTicker symbolL={liveSymbol.SYMBOL_LIGHTER} symbolE={liveSymbol.SYMBOL_EXTENDED} />
                        <button className="btn stop" onClick={closeLive}>Close</button>
                    </div>
                </div>
            )}

            {/* Log Modal */}
            {showLog && (
                <div className="modal-overlay" onClick={closeLog}>
                    <div className="modal-content" onClick={(e) => e.stopPropagation()}>
                        <LogViewer symbolL={logSymbol.SYMBOL_LIGHTER} symbolE={logSymbol.SYMBOL_EXTENDED} />
                        <button className="btn stop" onClick={closeLog}>Close</button>
                    </div>
                </div>
            )}

            {/* Edit Config Modal */}
            {showEdit && tempConfig && (
                <div className="modal-overlay-config" onClick={closeEdit}>
                    <div className="modal-content-config" onClick={(e) => e.stopPropagation()}>
                        <div className="edit-buttons">
                            <button className="btn save" onClick={saveEditedConfig}>Save</button>
                            <button className="btn delete" onClick={() => openDeleteConfirm(editSymbol)}>Delete</button>
                            <button className="btn close" onClick={closeEdit}>Close</button>
                        </div>
                        <div className="edit-config-table-container">
                            <table className="edit-config-table">
                                <tbody>
                                    {Object.keys(tempConfig).map((key) => (
                                        <tr key={key}>
                                            <td className="config-key-cell">{key}</td>
                                            <td className="config-value-cell">
                                                <input
                                                    value={tempConfig[key]}
                                                    onChange={(e) => handleEditChange(key, e.target.value)}
                                                />
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            )}



            {/* Delete Confirmation Modal */}
            {showDelete && (
                <div className="modal-overlay-del" onClick={closeDelete}>
                    <div className="modal-content-del" onClick={(e) => e.stopPropagation()}>
                        <p>
                            Are you sure you want to delete{" "}
                            <strong>{deleteSymbol.SYMBOL_LIGHTER}-{deleteSymbol.SYMBOL_EXTENDED}</strong>?
                        </p>
                        <div style={{ display: "flex", justifyContent: "space-around", marginTop: "20px" }}>
                            <button className="btn yes" onClick={confirmDelete}>✅ Yes</button>
                            <button className="btn cancel" onClick={closeDelete}>❌ Cancel</button>
                        </div>
                    </div>
                </div>
            )}

            {/* Notifications */}
            {notification && (
                <div className="modal-overlay" style={{ position: "fixed", bottom: "30px", right: "30px" }}>
                    <div className="modal-content" style={{
                        background: notification.type === "error" ? "#ff4d4f" : "#4ade80",
                        color: "#000",
                        padding: "16px 24px",
                        borderRadius: "8px"
                    }}>
                        {notification.message}
                    </div>
                </div>
            )}
        </div>
    );
}
