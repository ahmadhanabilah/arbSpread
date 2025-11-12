import React, { useState, useEffect } from "react";
import "../styles/DailyStats.css";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function DailyStats() {
    const [mode, setMode] = useState("fifo"); // "fifo" or "cycle"
    const [pnlLig, setPnlLig] = useState([]);
    const [pnlExt, setPnlExt] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    const fetchData = async () => {
        try {
            setLoading(true);
            const [ligRes, extRes] = await Promise.all([fetch(`${API_BASE}/get_daily_lig`), fetch(`${API_BASE}/get_daily_ext`),
            ]);

            if (!ligRes.ok || !extRes.ok) throw new Error("API error");

            const [ligJson, extJson] = await Promise.all([
                ligRes.json(),
                extRes.json(),
            ]);

            setPnlLig(ligJson);
            setPnlExt(extJson);
            setError("");
        } catch (e) {
            console.error("Error fetching PnL data:", e);
            setError("Failed to load PnL data. Check backend connection.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
        const interval = setInterval(fetchData, 100000);
        return () => clearInterval(interval);
    }, [mode]);

    const formatValue = (value, decimals = 2) => {
        const num = parseFloat(value || 0);
        return isNaN(num) ? "0.00" : num.toFixed(decimals);
    };

    const dataLength = Math.min(pnlLig.length, pnlExt.length);

    if (loading) return <div className="loading">Loading...</div>;
    if (error) return <div className="error">{error}</div>;

    return (
            <div className="daily-stats-table-container">
                <table>
                    <thead>
                        <tr>
                            <th rowSpan="2" className="date-header">Date</th>
                            <th rowSpan="2" className="netpnl-header">Net PnL ($)</th>
                            <th colSpan="2" className="ext-header">Extended Stats</th>
                            <th colSpan="2" className="lig-header">Lighter Stats</th>
                        </tr>
                        <tr>
                            <th className="ext-pnl-header">PnL ($)</th>
                            <th className="ext-vol-header">Volume ($)</th>
                            <th className="lig-pnl-header">PnL ($)</th>
                            <th className="lig-vol-header">Volume ($)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {dataLength > 0 ? (
                            [...Array(dataLength)].map((_, i) => {
                                const lig = pnlLig[i] || {};
                                const ext = pnlExt[i] || {};
                                const net =
                                    parseFloat(ext.PNL) +
                                    parseFloat(lig.PNL);

                                return (
                                    <tr key={lig.date || i}>
                                        <td className="date-col">{lig.Date || "N/A"}</td>
                                        <td className={`netpnl-col ${net >= 0 ? "pnl-positive" : "pnl-negative"}`}>
                                            {formatValue(net)}
                                        </td>
                                        <td className="ext-pnl-col">{formatValue(ext.PNL)}</td>
                                        <td className="ext-vol-col">{formatValue(ext.Volume)}</td>
                                        <td className="ext-pnl-col">{formatValue(lig.PNL)}</td>
                                        <td className="ext-vol-col">{formatValue(lig.Volume)}</td>
                                    </tr>
                                );
                            })
                        ) : (
                            <tr>
                                <td colSpan="6" className="no-data">No PnL data found.</td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
    );
}
