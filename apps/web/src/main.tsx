import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles.css";

// Tailwind(shadcn)의 다크모드는 <html>에 .dark 클래스가 붙어야 발동하는데,
// 이 앱엔 수동 라이트/다크 토글이 없으므로 시스템 설정을 그대로 따라간다
// (next-themes의 "system" 모드와 동일한 방식).
const darkMediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
const syncDarkClass = (isDark: boolean) => document.documentElement.classList.toggle("dark", isDark);
syncDarkClass(darkMediaQuery.matches);
darkMediaQuery.addEventListener("change", (e) => syncDarkClass(e.matches));

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
