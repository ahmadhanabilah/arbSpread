import React, { useState, useEffect, useMemo } from "react";
import "../styles/RecentTrades.css";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function RecentTrades() {
    const [ligData, setLigData] = useState([]);
    const [extData, setExtData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [ligFifoEnabled, setLigFifoEnabled] = useState(false);
    const [extFifoEnabled, setExtFifoEnabled] = useState(false);
    const [symbolFilter, setSymbolFilter] = useState("ALL"); // ⬅️ new

    const fetchData = async () => {
        try {
            const qs = symbolFilter && symbolFilter !== "ALL"
                ? `?symbol=${encodeURIComponent(symbolFilter)}&limit=200`
                : `?limit=200`;

            const [fifoExtRes, cycleExtRes, fifoLigRes, cycleLigRes] = await Promise.all([
                fetch(`${API_BASE}/get_trades_fifo_ext${qs}`),
                fetch(`${API_BASE}/get_trades_cycle_ext${qs}`),
                fetch(`${API_BASE}/get_trades_fifo_lig${qs}`),
                fetch(`${API_BASE}/get_trades_cycle_lig${qs}`),
            ]);


            if (!fifoExtRes.ok || !cycleExtRes.ok || !fifoLigRes.ok || !cycleLigRes.ok) {
                throw new Error("API response error");
            }

            const [fifoExt, cycleExt, fifoLig, cycleLig] = await Promise.all([
                fifoExtRes.json(),
                cycleExtRes.json(),
                fifoLigRes.json(),
                cycleLigRes.json(),
            ]);

            const ligMerged = fifoLig.map((row, i) => ({
                fifo: row,
                cycle: cycleLig[i] || {},
            }));
            const extMerged = fifoExt.map((row, i) => ({
                fifo: row,
                cycle: cycleExt[i] || {},
            }));

            // store full sets; slice later after filtering
            setLigData(ligMerged);
            setExtData(extMerged);
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

    // --- Helpers ---
    const getMarket = (row, fifoEnabled) => {
        const obj = fifoEnabled ? row?.fifo : row?.cycle;
        return (obj?.market || "").toUpperCase();
    };

    const filtered = (data, fifoEnabled) => {
        if (!data) return [];
        const upper = symbolFilter.toUpperCase();
        return data.filter((row) =>
            symbolFilter === "ALL" ? true : getMarket(row, fifoEnabled) === upper
        );
    };

    // Build symbol list from both Lighter & Extended (fifo+cycle)
    const allSymbols = useMemo(() => {
        const s = new Set();
        const grab = (rows, key) =>
            rows.forEach((r) => {
                const m = r?.[key]?.market;
                if (m) s.add(String(m).toUpperCase());
            });
        grab(ligData, "fifo");
        grab(ligData, "cycle");
        grab(extData, "fifo");
        grab(extData, "cycle");
        return ["ALL", ...Array.from(s).sort()];
    }, [ligData, extData]);

    // CSV Downloader (respects current filter & fifo toggle)
    const downloadCSV = (data, filename, fifoEnabled) => {
        const view = filtered(data, fifoEnabled);
        if (!view || view.length === 0) {
            alert("No data available to download for this filter/toggle.");
            return;
        }
        const flat = view.map((row) => (fifoEnabled ? row.fifo : row.cycle));
        const headers = Object.keys(flat[0] || {});
        const csvRows = [
            headers.join(","),
            ...flat.map((row) => headers.map((h) => JSON.stringify(row[h] ?? "")).join(",")),
        ];
        const blob = new Blob([csvRows.join("\n")], { type: "text/csv;charset=utf-8;" });
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = filename;
        link.click();
    };

    if (loading) return <div className="loading">Loading...</div>;
    if (error) return <div className="error">{error}</div>;

    const renderTable = (data, label, fifoEnabled, setFifoEnabled) => {
        const rows = filtered(data, fifoEnabled).slice(0, 200); // ⬅️ slice after filtering
        return (
            <div className="trades-row">
                <div className="trades-table-header">
                    <h2 className="trades-table-title">{label}</h2>
                    <div className="trades-table-buttons">
                        <button
                            className="trades-download-btn"
                            onClick={() => downloadCSV(rows, `${label.toLowerCase()}_trades.csv`, fifoEnabled)}
                            title="Download CSV"
                        >
                            ⬇️
                        </button>
                        <button
                            className={`fifo-toggle ${fifoEnabled ? "on" : "off"}`}
                            onClick={() => setFifoEnabled(!fifoEnabled)}
                            title={fifoEnabled ? "Show Aggregated" : "Show FIFO"}
                        >
                            {fifoEnabled ? "F" : "A"}
                        </button>
                    </div>
                </div>

                <div className="trades-table">
                    <table>
                        {!fifoEnabled ? (
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Entry Time</th>
                                    <th>Exit Time</th>
                                    <th>Side</th>
                                    <th>Realized PNL ($)</th>
                                    <th>Qty Opened</th>
                                    <th>Qty Closed</th>
                                    <th>Value Opened($)</th>
                                    <th>Value Closed($)</th>
                                    <th>Trade Pnl ($)</th>
                                    <th>Trading Fees ($)</th>
                                    <th>Funding Fees ($)</th>
                                    <th>Detailed FF ($)</th>
                                </tr>
                            </thead>
                        ) : (
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Time</th>
                                    <th>Realized PNL ($)</th>
                                    <th>Qty</th>
                                    <th>Price</th>
                                    <th>Type</th>
                                    <th>Trade PNL ($)</th>
                                    <th>Trading Fees ($)</th>
                                    <th>Funding Fees ($)</th>
                                    <th>Detailed FF ($)</th>
                                </tr>
                            </thead>
                        )}
                        <tbody>
                            {rows.map((row, i) => {
                                const r = fifoEnabled ? row.fifo : row.cycle;
                                return !fifoEnabled ? (
                                    <tr key={i}>
                                        <td>{r.market}</td>
                                        <td>{r.entry_time}</td>
                                        <td>{r.exit_time || "-"}</td>
                                        <td>{r.side}</td>
                                        <td className={parseFloat(r.realized_pnl) > 0 ? "pnl-positive" : "pnl-negative"}>
                                            {parseFloat(r.realized_pnl || 0).toFixed(4)}
                                        </td>
                                        <td>{parseFloat(r.qty_opened).toFixed(4)}</td>
                                        <td>{parseFloat(r.qty_closed).toFixed(4)}</td>
                                        <td>{parseFloat(r.qty_opened*r.entry_price).toFixed(4)}</td>
                                        <td>{parseFloat(r.qty_closed*r.exit_price).toFixed(4)} </td>
                                        <td className={parseFloat(r.trade_pnl) > 0 ? "pnl-positive" : "pnl-negative"}>
                                            {parseFloat(r.trade_pnl || 0).toFixed(4)}
                                        </td>
                                        <td>{parseFloat(r.trading_fees).toFixed(4)}</td>
                                        <td>{parseFloat(r.funding_fees).toFixed(4)}</td>
                                        <td>{r.funding_fee_details}</td>
                                    </tr>
                                ) : (
                                    <tr key={i}>
                                        <td>{r.market}</td>
                                        <td>{r.readable_time}</td>
                                        <td className={parseFloat(r.realized_pnl) > 0 ? "pnl-positive" : "pnl-negative"}>
                                            {parseFloat(r.realized_pnl || 0).toFixed(4)}
                                        </td>
                                        <td>{parseFloat(r.qty).toFixed(4)}</td>
                                        <td>{parseFloat(r.price).toFixed(4)}</td>
                                        <td>{r.trade_type}</td>
                                        <td className={parseFloat(r.trade_pnl) > 0 ? "pnl-positive" : "pnl-negative"}>
                                            {parseFloat(r.trade_pnl || 0).toFixed(4)}
                                        </td>
                                        <td>{parseFloat(r.trading_fees).toFixed(4)}</td>
                                        <td>{parseFloat(r.funding_fees).toFixed(4)}</td>
                                        <td>{r.funding_fee_details}</td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            </div>
        );
    };

    return (
        <div className="trades-main-container">
            {/* <div className="trades-filter-bar">
                <label htmlFor="symbolFilter">Pair:</label>
                <select
                    id="symbolFilter"
                    className="symbol-filter"
                    value={symbolFilter}
                    onChange={(e) => setSymbolFilter(e.target.value)}
                >
                    {allSymbols.map((s) => (
                        <option key={s} value={s}>
                            {s === "ALL" ? "All Symbols" : s}
                        </option>
                    ))}
                </select>
            </div> */}

            <div className="trades-row-main-container">
                {renderTable(ligData, "Lighter", ligFifoEnabled, setLigFifoEnabled)}
                {renderTable(extData, "Extended", extFifoEnabled, setExtFifoEnabled)}
            </div>
        </div>
    );
}
