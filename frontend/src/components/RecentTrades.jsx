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

            // ✅ Get the top 200 trades (first 200 items)
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

    if (loading) return <div className="p-4 text-center">Loading...</div>;
    if (error) return <div className="p-4 text-center text-red-500">{error}</div>;


    return (
        <div className="trades-row-container">

            {/* ✅ Lighter Trades */}
            <div className="table-block">
                <h2 className="table-title">Lighter</h2> {/* ✅ Fixed Title */}
                <div className="table-responsive">
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
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
                                    <td className={parseFloat(row.pnl_usd) > 0 ? "pnl-positive" : "pnl-negative"}>
                                        {parseFloat(row.pnl_usd || 0).toFixed(4)}
                                    </td>
                                    <td>{parseFloat(row.qty_opened || 0).toLocaleString()}</td>
                                    <td>{parseFloat(row.entry_value || 0).toFixed(2)}</td>
                                    <td>{row.start_time}</td>
                                    <td>{row.end_time || "-"}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* ✅ Extended Trades */}
            <div className="table-block">
                <h2 className="table-title">Extended</h2> {/* ✅ Fixed Title */}
                <div className="table-responsive">
                    <table>
                        <thead>
                            <tr>
                                <th>Symbol</th>
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
                                    <td className={parseFloat(row.realised_pnl) > 0 ? "pnl-positive" : "pnl-negative"}>
                                        {parseFloat(row.realised_pnl || 0).toFixed(4)}
                                    </td>
                                    <td>{parseFloat(row.size * (row.exit_price > 0 ? 2 : 1) || 0).toLocaleString()}</td>
                                    <td>
                                        {(
                                            (parseFloat(row.size || 0) * parseFloat(row.open_price || 0)) +
                                            (parseFloat(row.size || 0) * parseFloat(row.exit_price || 0))
                                        ).toFixed(2)}
                                    </td>

                                    <td>{row.created_at}</td>
                                    <td>{row.closed_at || "-"}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>


        </div>
    );
}
