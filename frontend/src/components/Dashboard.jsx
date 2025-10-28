// src/components/Dashboard.jsx
import EnvPanel from "./EnvPanel";
import React, { useState } from "react";
import "../styles/App.css";
import "../styles/Dashboard.css";
import DailyStats from "./DailyStats";
import RecentTrades from "./RecentTrades";
import BotPanel from "./BotPanel";

export default function Dashboard() {
    const [tab, setTab] = useState("stats");

    return (
        <div className="app-main-container">
            <header className="header-section">
                <h1 className="dashboard-title">Dashboard</h1>
                <div className="view-switch-box">
                    <button
                        className={tab === "stats" ? "active" : ""}
                        onClick={() => setTab("stats")}
                    >
                        📈 Daily Stats
                    </button>
                    <button
                        className={tab === "trades" ? "active" : ""}
                        onClick={() => setTab("trades")}
                    >
                        📊 Trades
                    </button>
                    <button
                        className={tab === "bot" ? "active" : ""}
                        onClick={() => setTab("bot")}
                    >
                        🤖 Bot Panel
                    </button>

                    <button
                        className={tab === "env" ? "active" : ""}
                        onClick={() => setTab("env")}
                    >
                        ⚙️ Env Panel
                    </button>


                </div>
            </header>

            <main className="content-section">
                {tab === "stats" && (
                    <div className="daily-stats-section">
                        <DailyStats />
                    </div>
                )}
                {tab === "trades" && (
                    <div className="recent-trades-section">
                        <RecentTrades />
                    </div>
                )}
                {tab === "bot" && (
                    <div className="bot-panel-section">
                        <BotPanel />
                    </div>
                )}

                {tab === "env" && (
                    <div className="env-panel-section">
                        <EnvPanel />
                    </div>
                )}


            </main>
        </div>
    );
}
