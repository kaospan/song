import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

function pickPython() {
  const candidates = [];
  if (process.platform === "win32") {
    candidates.push(path.join(repoRoot, ".venv", "Scripts", "python.exe"));
  } else {
    candidates.push(path.join(repoRoot, ".venv", "bin", "python"));
  }
  candidates.push("python");
  candidates.push("python3");

  for (const c of candidates) {
    try {
      if (c === "python" || c === "python3") return c;
      if (fs.existsSync(c)) return c;
    } catch {
      // ignore
    }
  }
  return "python";
}

const python = pickPython();
// Default to 8001 to avoid collisions with other local dev backends that often use 8000.
const port = Number.parseInt(process.env.API_PORT || process.env.PORT || "8001", 10);
const args = [
  "-m",
  "uvicorn",
  "api_server:app",
  "--host",
  "127.0.0.1",
  "--port",
  String(port),
  "--reload",
];

console.log(`[api] Starting FastAPI via ${python} on http://127.0.0.1:${port}`);

const child = spawn(python, args, {
  cwd: repoRoot,
  stdio: "inherit",
  env: process.env,
});

child.on("error", (err) => {
  console.error("[api] Failed to start:", err?.message || err);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  if (signal) process.exit(1);
  process.exit(code ?? 1);
});
