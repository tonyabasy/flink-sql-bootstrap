#!/usr/bin/env python3
"""
gen-report.py - 从原始 JSON 测试结果生成兼容性报告
输出：docs/flink-compat-test-<pom-version>.html
"""
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_CFG   = PROJECT_ROOT / "scripts" / "flink-compat-test" / "config.yaml"
VERSIONS_CFG = PROJECT_ROOT / "scripts" / "flink-compat-test" / "versions.yaml"


def html_escape(text: str) -> str:
    """对文本进行 HTML 转义，防止 XSS。"""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def load_config() -> dict[str, Any]:
    """读取 config.yaml，返回扁平化的配置字典。"""
    cfg: dict[str, Any] = {}
    try:
        with open(CONFIG_CFG) as f:
            content = f.read()
        # 解析顶层 key: value 对（无前导空白）
        for m in re.finditer(r'^(\w[\w_]*):\s*(.*)', content, re.MULTILINE):
            key, val = m.group(1), m.group(2).strip()
            val = re.sub(r'\s+#.*', '', val)
            if val:
                cfg[key] = val.replace('"', '').replace("'", "")
        # 解析 modes 块
        modes: list[tuple[str, str]] = []
        for m in re.finditer(r'key:\s*(\S+)\s*\n\s*label:\s*(\S+)', content):
            modes.append((m.group(1), m.group(2)))
        if modes:
            cfg["modes"] = modes
        # 解析 dependencies 列表
        deps_match = re.search(r'^dependencies:\n', content, re.MULTILINE)
        if deps_match:
            deps: list[str] = []
            rest = content[deps_match.end():]
            for line in rest.split('\n'):
                line = line.rstrip()
                if not line.startswith('  - '):
                    break
                val = line[4:].strip().strip('"').strip("'")
                if val:
                    deps.append(val)
            cfg["dependencies"] = deps
    except Exception as e:
        print(f"[WARN] Could not parse config.yaml: {e}", file=sys.stderr)
    return cfg


_CFG = load_config()

RESULTS_RAW = PROJECT_ROOT / _CFG.get("results_dir", "target/compat-test-results") / "raw"
REPORT_DIR  = PROJECT_ROOT / _CFG.get("report_dir", "docs")
MODES: list[tuple[str, str]] = _CFG.get("modes", [
    ("local", "Local"),
    ("yarn-application", "YARN-App"),
    ("yarn-session", "YARN-Session"),
    ("kubernetes-session", "K8s-Session"),
    ("kubernetes-application", "K8s-App"),
])

_NS = {"pom": "http://maven.apache.org/POM/4.0.0"}

def get_pom_version() -> str:
    """从 pom.xml 读取项目版本号。"""
    try:
        # Python 3.8+ 的 xml.etree.ElementTree 不支持 DTD，
        # 因此使用此 API 不可能发生 XXE 攻击。
        tree = ET.parse(str(PROJECT_ROOT / "pom.xml"))
        root = tree.getroot()
        el = root.find("pom:version", _NS)
        if el is not None and el.text:
            return el.text
    except Exception:
        pass
    return "unknown"

def load_versions() -> list[str]:
    """从 versions.yaml 加载 Flink 版本号列表。"""
    versions = []
    try:
        with open(VERSIONS_CFG) as f:
            for line in f:
                m = re.search(r'^\s*-\s+version:\s*"([^"]+)"', line)
                if m:
                    versions.append(m.group(1))
    except Exception as e:
        print(f"[WARN] Could not parse versions.yaml: {e}", file=sys.stderr)
        # 降级：从结果文件推断版本
        for p in RESULTS_RAW.glob("*.json"):
            parts = p.stem.rsplit("_", maxsplit=len(MODES))
            if parts:
                versions.append(parts[0])
        versions = sorted(set(versions))
    return versions

def load_result(version: str, mode: str) -> Optional[dict]:
    """加载某个版本和模式对应的测试结果 JSON 文件。"""
    path = RESULTS_RAW / f"{version}_{mode}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

def status_of(result: Optional[dict]) -> Optional[str]:
    """获取最新测试状态（PASS/FAIL/SKIP）。"""
    if result is None:
        return None
    return result.get("latest", {}).get("status")

def error_of(result: Optional[dict]) -> str:
    """获取最新测试的错误信息。"""
    if result is None:
        return ""
    return result.get("latest", {}).get("error", "")

def latest_timestamp(result: Optional[dict]) -> str:
    """返回最新测试的时间戳，格式化为本地可读格式。"""
    if result is None:
        return ""
    latest = result.get("latest", {})
    ts = latest.get("datetime", "")
    # 将 ISO 时间戳转换为本地可读格式：YYYY-MM-DD HH:MM
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%m-%d %H:%M")
        except Exception:
            return html_escape(ts)
    return ""

def classify_error(error: str) -> str:
    """根据错误文本分类错误类型。"""
    if not error:
        return "Unknown"
    e = error.lower()
    if "classnotfound" in e or "nosuchmethod" in e or "nosuchfield" in e:
        return "🔴 API Incompatibility (class/method not found)"
    if "incompatibleclasschange" in e or "abstractmethoderror" in e:
        return "🔴 API Incompatibility (binary incompatible change)"
    if "connection refused" in e or "timeout" in e or "unreachable" in e:
        return "🟡 Environment / Network issue"
    if "outofmemory" in e:
        return "🟡 Resource issue (OOM)"
    if "exit code" in e:
        return "🟠 Process exit (check logs)"
    return "🔴 Runtime Error"


def extract_exception_class(error: str) -> str:
    """从错误文本中提取异常类名，例如 'java.lang.NoSuchMethodError'。"""
    if not error:
        return ""
    return error.split(":")[0].strip()

# ── SVG 图标 ─────────────────────────────────────────────────────────────────
_ICON_SVG = {
    "PASS": (
        '<svg viewBox="0 0 16 16" fill="none">'
        '<circle cx="8" cy="8" r="7" fill="#1a7f37"/>'
        '<path d="M5 8l2 2 4-4" stroke="#fff" stroke-width="1.5" '
        'stroke-linecap="round" stroke-linejoin="round"/></svg>'
    ),
    "FAIL": (
        '<svg viewBox="0 0 16 16" fill="none">'
        '<circle cx="8" cy="8" r="7" fill="#cf222e"/>'
        '<path d="M5.5 5.5l5 5M10.5 5.5l-5 5" stroke="#fff" '
        'stroke-width="1.5" stroke-linecap="round"/></svg>'
    ),
    "SKIP": (
        '<svg viewBox="0 0 16 16" fill="none">'
        '<path d="M8 2L1.5 14h13z" fill="#9a6700"/>'
        '<path d="M8 6v4M8 11.5v.5" stroke="#fff" '
        'stroke-width="1.5" stroke-linecap="round"/></svg>'
    ),
    None: (
        '<svg viewBox="0 0 16 16" fill="none">'
        '<circle cx="8" cy="8" r="5.5" stroke="#8b949e" '
        'stroke-width="1.5"/></svg>'
    ),
}

_ICON_CLASS = {
    "PASS": "icon-pass",
    "FAIL": "icon-fail",
    "SKIP": "icon-skip",
    None:   "icon-nt",
}


def _icon(status: Optional[str], small: bool = False) -> str:
    """返回对应状态的图标 span HTML。"""
    svg = _ICON_SVG.get(status, _ICON_SVG[None])
    cls = _ICON_CLASS.get(status, "icon-nt")
    size = "icon-sm" if small else "icon"
    return f'<span class="{size} {cls}">{svg}</span>'


def _status_cell(status: Optional[str], timestamp: str) -> str:
    """返回表格单元格内容：图标 + 可选时间戳。"""
    icon = _icon(status)
    return f"{icon} {timestamp}" if timestamp else icon


def _format_cmd(version: str, mode: str, jar_name: Optional[str] = None,
                script: Optional[str] = None) -> str:
    """格式化测试命令，使用 $FLINK_HOME 和反斜杠续行。"""
    if jar_name is None:
        app_jar = _CFG.get("app_jar", "target/flink-sql-bootstrap.jar")
        jar_name = Path(app_jar).name
    if script is None:
        script = _CFG.get("test_script", "classpath:example-word-count.sql")
    lines = [
        "$FLINK_HOME/bin/flink run \\",
        f"    --target {mode} \\",
        f'    -Dpipeline.name="compat-test-{version}-{mode}" \\',
        f"    -Dexecution.attached=true \\",
        f"    {jar_name} \\",
        f"    --script-file {script} \\",
    ]
    # 追加 config.yaml 中的可选应用参数
    resource_file = _CFG.get("resource_file", "")
    catalog_file = _CFG.get("catalog_file", "")
    deps = _CFG.get("dependencies", [])
    run_mode = _CFG.get("run_mode", "")
    if resource_file:
        lines.append(f"    --resource-file {resource_file} \\")
    if catalog_file:
        lines.append(f"    --catalog-file {catalog_file} \\")
    for dep in deps:
        lines.append(f"    --dependency {dep} \\")
    if run_mode:
        lines.append(f"    --{run_mode} \\")
    # 去除最后一行的尾部反斜杠
    lines[-1] = lines[-1].rstrip(" \\")
    return "\n".join(lines)


def _load_sql_content() -> str:
    """从 config.yaml 配置的路径读取 SQL 测试脚本内容。"""
    script_cfg = _CFG.get("test_script", "classpath:example-word-count.sql")
    # 去掉 classpath: 前缀 → 对应 src/main/resources/
    if script_cfg.startswith("classpath:"):
        path = PROJECT_ROOT / "src" / "main" / "resources" / script_cfg[len("classpath:"):]
    else:
        path = PROJECT_ROOT / script_cfg
    try:
        return path.read_text().strip()
    except Exception:
        return ""


def generate_report(versions: list[str]) -> str:
    """生成完整的 HTML 报告字符串。"""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=PROJECT_ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        commit = "unknown"

    pom_version = get_pom_version()
    failures: list[tuple[str, str, str]] = []

    # 获取 Java 版本
    java_version = "unknown"
    try:
        out = subprocess.check_output(
            ["java", "-version"], stderr=subprocess.STDOUT, text=True
        )
        java_version = out.strip().split("\n")[0]
    except Exception:
        pass

    # 用于展示的 SQL 内容
    sql_content = _load_sql_content()

    test_script = _CFG.get("test_script", "classpath:example-word-count.sql")

    # ── 页头 ────────────────────────────────────────────────────────────────────
    lines = [
        "<!DOCTYPE html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>Flink SQL App - Compatibility Report</title>",
        "<style>",
        "  body {",
        "    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',",
        "      Roboto, Helvetica, Arial, sans-serif;",
        "    max-width: 960px; margin: 40px auto; padding: 0 24px;",
        "    color: #24292f; background: #fff; line-height: 1.6;",
        "  }",
        "  h1 { font-size: 28px; border-bottom: 1px solid #d0d7de;",
        "      padding-bottom: 10px; margin-bottom: 8px; }",
        "  h2 { font-size: 22px; margin-top: 32px;",
        "      border-bottom: 1px solid #d0d7de; padding-bottom: 8px; }",
        "  h3 { font-size: 16px; margin: 20px 0 8px; }",
        "  h4 { font-size: 14px; margin: 16px 0 4px; color: #57606a; }",
        "  .meta { font-size: 14px; margin-bottom: 24px; }",
        "  .meta p { margin: 2px 0; }",
        "  .meta code { background: #f6f8fa; padding: 2px 6px;",
        "      border-radius: 4px; font-size: 13px; }",
        "  pre, .code-block {",
        "    background: #f6f8fa; border: 1px solid #d0d7de;",
        "    border-radius: 6px; padding: 16px;",
        "    font-size: 12px; line-height: 1.5;",
        "    overflow-x: auto; white-space: pre-wrap;",
        "    overflow-wrap: break-word;",
        "    font-family: 'SFMono-Regular', Consolas,",
        "      'Liberation Mono', Menlo, monospace;",
        "  }",
        "  .icon svg { width: 16px; height: 16px; }",
        "  .icon-sm svg { width: 14px; height: 14px; }",
        "  .icon { display: inline-flex; align-items: center; vertical-align: middle; }",
        "  .icon-pass { color: #1a7f37; }",
        "  .icon-fail { color: #cf222e; }",
        "  .icon-skip { color: #9a6700; }",
        "  .icon-nt   { color: #8b949e; }",
        "  table { width: 100%; border-collapse: collapse;",
        "      margin: 16px 0; font-size: 14px; }",
        "  th { background: #f6f8fa; padding: 10px 12px;",
        "      border: 1px solid #d0d7de; font-weight: 600;",
        "      white-space: nowrap; }",
        "  td { padding: 10px 12px; border: 1px solid #d0d7de;",
        "      text-align: center; font-variant-numeric: tabular-nums;",
        "      vertical-align: middle; }",
        "  td:first-child { text-align: left;",
        "      font-family: 'SFMono-Regular', Consolas,",
        "        'Liberation Mono', Menlo, monospace;",
        "      font-weight: 500; white-space: nowrap; }",
        "  tr:nth-child(even) td { background: #fcfcfd; }",
        "  .legend { font-size: 13px; color: #57606a;",
        "      margin-bottom: 32px; }",
        "  .legend .icon svg, .legend .icon-sm svg {",
        "      vertical-align: middle; }",
        "  .fail-card { border: 1px solid #d0d7de;",
        "      border-radius: 6px; margin: 16px 0; overflow: hidden; }",
        "  .fail-card summary { padding: 12px 16px; cursor: pointer;",
        "      font-weight: 600; font-size: 15px; background: #fcfcfd;",
        "      border-bottom: 1px solid #d0d7de; }",
        "  .fail-card summary:hover { background: #f6f8fa; }",
        "  .fail-card[open] summary { border-bottom: 1px solid #d0d7de; }",
        "  .fail-card .body { padding: 16px; }",
        "  .fail-card .body p { margin: 4px 0; font-size: 14px; }",
        "  .fail-card .body .label { font-weight: 600; color: #57606a; }",
        "  .fail-card .cmd {",
        "    background: #f6f8fa; border-radius: 6px;",
        "    padding: 10px 14px; font-size: 12px; line-height: 1.5;",
        "    margin: 8px 0; overflow-x: auto; white-space: pre-wrap;",
        "    overflow-wrap: break-word;",
        "    font-family: 'SFMono-Regular', Consolas,",
        "      'Liberation Mono', Menlo, monospace;",
        "  }",
        "  .fail-card pre { margin: 8px 0 0; padding: 12px 16px; }",
        "</style>",
        "</head>",
        "<body>",
        "",
        "<h1>Flink SQL App &mdash; Compatibility Report</h1>",
        "",
        '<div class="meta">',
        f"<p><strong>Last updated:</strong> {now}</p>",
        f"<p><strong>Commit:</strong> <code>{commit}</code></p>",
        f"<p><strong>Version:</strong> <code>{pom_version}</code></p>",
        f"<p><strong>Java:</strong> <code>{java_version}</code></p>",
        "</div>",
        "",
        # ── 测试配置 ──────────────────────────────────────────────────────────────
        "<h2>Test Configuration</h2>",
        "",
        "<div>",
        "    <h4>Modes:</h4>",
        f"    <p>{', '.join(html_escape(label) for _, label in MODES)}</p>",
        "</div>",
    ]
    # 追加格式化后的命令
    if MODES:
        sample_version = versions[0]
        sample_mode = MODES[0][0]
        formatted = _format_cmd(sample_version, sample_mode)
        escaped = (
            formatted
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        lines += [
            "<div>",
            "    <h4>Command:</h4>",
            f"    <pre>{escaped}</pre>",
            "</div>",
        ]
    if sql_content:
        escaped_sql = (
            sql_content
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        lines += [
            "<div>",
            "    <h4>Test SQL:</h4>",
            f"    <pre>{escaped_sql}</pre>",
            "</div>",
        ]

    lines += [
        "<h2>Compatibility Matrix</h2>",
        "",
        "<table>",
        "  <thead>",
        "    <tr>",
        '      <th align="left">Flink Version</th>',
    ]
    for _, label in MODES:
        lines.append(f'      <th align="center">{html_escape(label)}</th>')
    lines += ["    </tr>", "  </thead>", "  <tbody>"]

    for version in versions:
        lines.append("    <tr>")
        lines.append(f'      <td align="left">{html_escape(version)}</td>')
        for mode_key, _ in MODES:
            result = load_result(version, mode_key)
            status = status_of(result)
            lines.append(f'      <td align="center">{_status_cell(status, "")}</td>')
            if status == "FAIL":
                failures.append((version, mode_key, error_of(result)))
        lines.append("    </tr>")

    lines += ["  </tbody>", "</table>", ""]

    # ── 图例 ────────────────────────────────────────────────────────────────────
    lines.append(
        '<p class="legend">'
        f'{_icon("PASS", small=True)} Pass &nbsp;&middot;&nbsp; '
        f'{_icon("FAIL", small=True)} Fail &nbsp;&middot;&nbsp; '
        f'{_icon("SKIP", small=True)} Skip &nbsp;&middot;&nbsp; '
        f'{_icon(None, small=True)} Not tested &nbsp;|&nbsp; '
        "Cell shows latest result + run time (MM-DD HH:MM UTC)"
        "</p>"
    )

    # ── 失败详情 ────────────────────────────────────────────────────────────────
    if failures:
        lines.append("<h2>Failure Details</h2>")
        lines.append("")
        for version, mode, error in failures:
            result = load_result(version, mode)
            ts = result.get("latest", {}).get("datetime", "unknown") if result else "unknown"
            error_type = classify_error(error)
            err_emoji = error_type.split()[0] if error_type else "🔴"
            exc_class = extract_exception_class(error)

            lines.append('<details class="fail-card">')
            lines.append(
                f"  <summary>{html_escape(version)} &times; {html_escape(mode)} &mdash; "
                f"{err_emoji} {html_escape(exc_class)}</summary>"
            )
            lines.append("  <div class=\"body\">")
            lines.append(
                f'    <p><span class="label">Error Type:</span> '
                f'{error_type}</p>'
            )
            lines.append(f'    <p><span class="label">Last Run:</span> {html_escape(ts)}</p>')

            lines.append('    <p><span class="label">Error:</span></p>')
            lines.append("    <pre>")
            if error:
                for line in error.split("\n"):
                    escaped = (
                        line
                        .replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;")
                    )
                    lines.append(f"      {escaped}")
            else:
                lines.append("      No error captured")
            lines.append("    </pre>")
            lines.append("  </div>")
            lines.append("</details>")
            lines.append("")
    else:
        lines.append("<h2>Failure Details</h2>")
        lines.append('<p><em>No failures recorded.</em></p>')
        lines.append("")

    lines.append("</body>")
    lines.append("</html>")
    return "\n".join(lines)

def main():
    """入口函数：加载版本、生成报告、写入文件。"""
    versions = load_versions()
    if not versions:
        print("[ERROR] No versions found", file=sys.stderr)
        sys.exit(1)

    RESULTS_RAW.mkdir(parents=True, exist_ok=True)
    report = generate_report(versions)

    pom_version = get_pom_version()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report_file = REPORT_DIR / f"flink-compat-test-{pom_version}.html"

    with open(report_file, "w") as f:
        f.write(report)

    print(f"[INFO] Report written to {report_file}")

if __name__ == "__main__":
    main()
