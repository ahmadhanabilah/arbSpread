import React, { useEffect, useState } from "react";
import "../styles/Readme.css";

// You can place your README.md in the "public" folder
const README_PATH = "../USER_README.md";

export default function Readme() {
    const [content, setContent] = useState("");
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    useEffect(() => {
        const loadReadme = async () => {
            try {
                const res = await fetch(README_PATH);
                if (!res.ok) throw new Error(`Failed to load README.md (${res.status})`);
                const text = await res.text();
                setContent(text);
            } catch (err) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };
        loadReadme();
    }, []);

    if (loading) return <div className="readme-loading">Loading README...</div>;
    if (error) return <div className="readme-error">Error: {error}</div>;

    return (
        <div className="readme-container">
            <article
                className="markdown-body"
                dangerouslySetInnerHTML={{
                    __html: window.marked ? window.marked.parse(content) : content,
                }}
            />
        </div>
    );
}
