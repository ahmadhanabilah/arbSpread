import React, { useState, useEffect } from "react";
import "../styles/RecentTrades.css";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function RecentTrades() {
    const [ligData, setLigData] = useState([]);
    const [extData, setExtData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    const fetchData = async () => {
        try {
            const [ligRes, extRes] = await Promise.all([
                fetch(`${API_BASE}/get_lig`),
                fetch(`${API_BASE}/get_ext`),
            ]);
            if (!ligRes.ok || !extRes.ok) throw new Error("API error");

            const [ligJson, extJson] = await Promise.all([
                ligRes.json(),
                extRes.json(),
            ]);

            setLigData(ligJson.slice(0, 200));
            setExtData(extJson.slice(0, 200));
            setError("");
        } catch (e) {
            console.error("Error fetching trades:", e);
            setError("Failed to load trade data. Check backend.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, []);

    // 🧾 Helper to download CSV from JS array
    const downloadCSV = (data, filename) => {
        if (!data || data.length === 0) {
            alert("No data available to download.");
            return;
        }
        const headers = Object.keys(data[0]);
        const csvRows = [
            headers.join(","), // header row
            ...data.map((row) =>
                headers.map((h) => JSON.stringify(row[h] ?? "")).join(",")
            ),
        ];
        const csvContent = csvRows.join("\n");
        const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = filename;
        link.click();
    };

    if (loading) return <div className="p-4 text-center">Loading...</div>;
    if (error) return <div className="p-4 text-center text-red-500">{error}</div>;

    return (
        <div className="trades-row-main-container">

            {/* ✅ Lighter Trades */}
            <div className="trades-row">
                <div className="trades-table-header">
                    <h2 className="trades-table-title">Lighter</h2>
                    <button
                        className="trades-download-btn"
                        onClick={() => downloadCSV(ligData, "lighter_trades.csv")}
                    >
                        ⬇️ CSV
                    </button>
                </div>

                <div className="trades-table">
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>PnL ($)</th>
                                <th>Qty</th>
                                <th>Value ($)</th>
                                <th>Entry Time</th>
                                <th>Exit Time</th>
                            </tr>
                        </thead>
                        <tbody>
                            {ligData.map((row, i) => (
                                <tr key={i}>
                                    <td>{row.symbol}</td>
                                    <td>{row.side}</td>
                                    <td className={parseFloat(row.net_pnl) > 0 ? "pnl-positive" : "pnl-negative"}>
                                        {parseFloat(row.net_pnl || 0).toFixed(4)}
                                    </td>
                                    <td>{parseFloat(row.qty_opened || 0) + parseFloat(row.qty_closed || 0)}</td>
                                    <td>
                                        {(
                                            (parseFloat(row.qty_opened || 0) * parseFloat(row.avg_entry_price || 0)) +
                                            (parseFloat(row.qty_closed || 0) * parseFloat(row.avg_exit_price || 0))
                                        ).toFixed(2)}
                                    </td>
                                    <td>{row.entry_time}</td>
                                    <td>{row.exit_time || "-"}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* ✅ Extended Trades */}
            <div className="trades-row">
                <div className="trades-table-header">
                    <h2 className="trades-table-title">Extended</h2>
                    <button
                        className="trades-download-btn"
                        onClick={() => downloadCSV(extData, "extended_trades.csv")}
                    >
                        ⬇️ CSV
                    </button>
                </div>

                <div className="trades-table">
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>PnL ($)</th>
                                <th>Qty</th>
                                <th>Value ($)</th>
                                <th>Entry Time</th>
                                <th>Exit Time</th>
                            </tr>
                        </thead>
                        <tbody>
                            {extData.map((row, i) => (
                                <tr key={i}>
                                    <td>{row.market}</td>
                                    <td>{row.side}</td>
                                    <td className={parseFloat(row.net_pnl) > 0 ? "pnl-positive" : "pnl-negative"}>
                                        {parseFloat(row.net_pnl || 0).toFixed(4)}
                                    </td>
                                    <td>{parseFloat(row.qty_opened || 0) + parseFloat(row.qty_closed || 0)}</td>
                                    <td>
                                        {(
                                            (parseFloat(row.qty_opened || 0) * parseFloat(row.avg_entry_price || 0)) +
                                            (parseFloat(row.qty_closed || 0) * parseFloat(row.avg_exit_price || 0))
                                        ).toFixed(2)}
                                    </td>
                                    <td>{row.entry_time}</td>
                                    <td>{row.exit_time || "-"}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>

        </div>
    );
}
