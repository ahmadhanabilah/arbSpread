import React, { useEffect, useState } from "react";
import "../styles/ConfigModal.css";

const API_BASE = `${window.location.protocol}//${window.location.hostname}:8000`;

export default function ConfigModal({ pair, onClose, showNotification }) {
    const [configText, setConfigText] = useState("");
    const [saving, setSaving] = useState(false);

    useEffect(() => {
        const fetchConfig = async () => {
            try {
                const res = await fetch(`${API_BASE}/api/config/${pair.symbolL}/${pair.symbolE}`);
                const text = await res.text();
                setConfigText(text);
            } catch (err) {
                console.error("Error fetching config:", err);
                showNotification("❌ Failed to load config", "error");
            }
        };
        fetchConfig();
    }, [pair]);

    const saveConfig = async () => {
        try {
            setSaving(true);
            const res = await fetch(`${API_BASE}/api/config/${pair.symbolL}/${pair.symbolE}`, {
                method: "POST",
                headers: { "Content-Type": "text/plain" },
                body: configText,
            });
            if (res.ok) {
                showNotification(`✅ Saved config for ${pair.symbolL}_${pair.symbolE}`);
                onClose();
            } else {
                showNotification("❌ Failed to save config", "error");
            }
        } catch (err) {
            console.error(err);
            showNotification("❌ Error saving config", "error");
        } finally {
            setSaving(false);
        }
    };

    return (
        <div className="modal-overlay">
            <div className="modal-box">
                <h3>
                    ⚙️ Editing Config: {pair.symbolL}_{pair.symbolE}
                </h3>
                <textarea
                    className="config-textarea"
                    value={configText}
                    onChange={(e) => setConfigText(e.target.value)}
                />
                <div className="modal-actions">
                    <button className="save-btn" onClick={saveConfig} disabled={saving}>
                        💾 Save
                    </button>
                    <button className="close-btn" onClick={onClose}>
                        ✖ Close
                    </button>
                </div>
            </div>
        </div>
    );
}
