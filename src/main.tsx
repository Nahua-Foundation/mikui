import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";

// Стили (если используешь Tailwind/Vite)
import "./styles/fira_code.css";
import "./styles/globals.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
    <React.StrictMode>
        <App />
    </React.StrictMode>
);