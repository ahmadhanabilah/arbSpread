// src/components/DailyStats.jsx
import React, { useState, useEffect } from "react";
import "../styles/DailyStats.css";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;


export default function DailyStats() {
    const [pnlLig, setPnlLig] = useState([]);
    const [pnlExt, setPnlExt] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");


    const fetchData = async () => {
        try {
            const [ligRes, extRes] = await Promise.all([
                fetch(`${API_BASE}/get_pnl_lig`),
                fetch(`${API_BASE}/get_pnl_ext`),
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
        const interval = setInterval(fetchData, 10000); // refresh every 10s
        return () => clearInterval(interval);
    }, []);

    const formatValue = (value, decimals = 2) => {
        const numValue = parseFloat(value || 0);
        return numValue.toFixed(decimals);
    };

    const dataLength = Math.min(pnlLig.length, pnlExt.length);

    if (loading) return <div className="p-4 text-center">Loading...</div>;
    if (error) return <div className="p-4 text-center text-red-500">{error}</div>;

    return (
        <div className="p-4 sm:p-8 bg-gray-50 min-h-screen">
            <div className="table-container">
                <table>
                    <thead>
                        <tr>
                            <th rowSpan="2" className="date-header">Date</th>
                            <th rowSpan="2" className="date-header">Net PnL ($)</th>
                            <th colSpan="2" className="text-center extended-header">💎 Extended Stats</th>
                            <th colSpan="2" className="text-center lighter-header">🔥 Lighter Stats</th>
                        </tr>
                        <tr>
                            <th className="extended-header">PnL ($)</th>
                            <th className="extended-header">Volume ($)</th>
                            <th className="lighter-header">PnL ($)</th>
                            <th className="lighter-header">Volume ($)</th>
                        </tr>
                    </thead>

                    <tbody>
                        {[...Array(dataLength)].map((_, i) => {
                            const lig = pnlLig[i] || {};
                            const ext = pnlExt[i] || {};
                            return (
                                <tr key={lig.date || i}>
                                    <td className="date-col">{lig.date || "N/A"}</td>
                                    <td className="date-col">{ formatValue(parseFloat(ext.daily_net_pnl) + parseFloat(lig.daily_pnl_usd)) || "N/A"}</td>
                                    <td className="ext-col">{formatValue(ext.daily_net_pnl)}</td>
                                    <td className="ext-col">{formatValue(ext.daily_volume_usd)}</td>
                                    <td className="light-pnl-col">{formatValue(lig.daily_pnl_usd)}</td>
                                    <td className="light-pnl-col">{formatValue(lig.daily_volume_usd)}</td>
                                </tr>
                            );
                        })}
                    </tbody>


                    {dataLength === 0 && (
                        <tfoot>
                            <tr>
                                <td colSpan="7" className="text-center py-6 text-gray-500">
                                    No PnL data found.
                                </td>
                            </tr>
                        </tfoot>
                    )}
                </table>
            </div>
        </div>
    );
}
