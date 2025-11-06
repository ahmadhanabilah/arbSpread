// src/components/Dashboard.jsx
import EnvPanel from "./EnvPanel";
import React, { useState } from "react";
import "../styles/App.css";
import "../styles/Dashboard.css";
import DailyStats from "./DailyStats";
import RecentTrades from "./RecentTrades";
import BotPanel from "./BotPanel";
import Readme from "./Readme";

export default function Dashboard() {
    const [tab, setTab] = useState("stats");

    return (
        <div classname="page-container">
            <div className="navigator">
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

                <button
                    className={tab === "Readme" ? "active" : ""}
                    onClick={() => setTab("Readme")}
                >
                    Readme
                </button>

            </div>

            <main className="content-section">
                {tab === "stats" && (
                    <div>
                        <DailyStats />
                    </div>
                )}
                {tab === "trades" && (
                    <div>
                        <RecentTrades />
                    </div>
                )}
                {tab === "bot" && (
                    <div>
                        <BotPanel />
                    </div>
                )}

                {tab === "env" && (
                    <div>
                        <EnvPanel />
                    </div>
                )}

                {tab === "Readme" && (
                    <div>
                        <Readme />
                    </div>
                )}

            </main>
        </div>
    );
}
