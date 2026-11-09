import { createHash } from "node:crypto";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

let ts;
let postcss;

const COLLECTOR_VERSION = 1;
const PNG_SIGNATURE = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const ALLOWED_ASSET_TYPES = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".avif": "image/avif",
  ".svg": "image/svg+xml",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".ttf": "font/ttf",
  ".otf": "font/otf",
};

class CollectError extends Error {
  constructor(exitCode, diagnostic) {
    super(diagnostic.message || diagnostic.code);
    this.name = "CollectError";
    this.exitCode = exitCode;
    this.diagnostic = diagnostic;
  }
}

function moduleMajor(req, name, mod) {
  const versions = [];
  if (mod && mod.version) {
    versions.push(mod.version);
  }
  try {
    versions.push(req(`${name}/package.json`).version);
  } catch {
    // ignore
  }
  for (const version of versions) {
    const major = Number.parseInt(String(version).split(".")[0], 10);
    if (Number.isInteger(major)) {
      return major;
    }
  }
  return 0;
}

function loadToolchain(workspaceRoot) {
  const sources = [
    () => createRequire(import.meta.url),
    () => createRequire(path.join(workspaceRoot, "package.json")),
  ];
  for (const makeRequire of sources) {
    try {
      const req = makeRequire();
      const loadedTs = req("typescript");
      const loadedPostcss = req("postcss");
      if (moduleMajor(req, "typescript", loadedTs) >= 5 && moduleMajor(req, "postcss", loadedPostcss) >= 8) {
        ts = loadedTs;
        postcss = loadedPostcss;
        return;
      }
    } catch {
      // try next source
    }
  }
  throw new CollectError(4, {
    code: "deps_missing",
    message: "typescript>=5 and postcss>=8 not found beside the collector or in the workspace",
  });
}

function invalid(message, extra = {}) {
  return new CollectError(2, { code: extra.code || "invalid_request", message, ...extra });
}

function parseError(relPath, line, message) {
  return new CollectError(3, {
    code: "parse_error",
    path: relPath,
    line: Number.isInteger(line) && line >= 1 ? line : 1,
    message: String(message || "parse error"),
  });
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function asBuffer(data) {
  if (Buffer.isBuffer(data)) {
    return data;
  }
  if (data instanceof Uint8Array) {
    return Buffer.from(data);
  }
  return Buffer.from(String(data), "utf8");
}

function isInsideRoot(realPath, rootReal) {
  const rel = path.relative(rootReal, realPath);
  return rel === "" || (!rel.startsWith(`..${path.sep}`) && rel !== ".." && !path.isAbsolute(rel));
}

function confinePath(relPath, rootReal, io) {
  if (typeof relPath !== "string" || relPath.length === 0 || relPath.includes("\0")) {
    throw invalid("listed path is invalid", { path: relPath || "" });
  }
  const abs = path.resolve(rootReal, relPath);
  let real;
  try {
    real = io.realpath(abs);
  } catch {
    throw invalid("listed path does not exist", { code: "missing", path: relPath });
  }
  if (!isInsideRoot(real, rootReal)) {
    throw invalid("realpath leaves workspace_root", { code: "escape", path: relPath });
  }
  let stat;
  try {
    stat = io.lstat(real);
  } catch {
    throw invalid("listed path is not readable", { code: "unreadable", path: relPath });
  }
  return { real, stat };
}

function readConfined(relPath, rootReal, io, maxBytes) {
  const { real, stat } = confinePath(relPath, rootReal, io);
  const size = Number(stat.size);
  if (Number.isFinite(size) && size > maxBytes) {
    throw invalid("listed path exceeds byte cap", { code: "oversized", path: relPath });
  }
  const bytes = asBuffer(io.readFile(real));
  if (bytes.length > maxBytes) {
    throw invalid("listed path exceeds byte cap", { code: "oversized", path: relPath });
  }
  return { real, stat, bytes };
}

function hasModifier(node, kind) {
  const modifiers = node.modifiers;
  if (!modifiers) {
    return false;
  }
  for (const modifier of modifiers) {
    if (modifier.kind === kind) {
      return true;
    }
  }
  return false;
}

function walkTs(node, visit) {
  visit(node);
  ts.forEachChild(node, (child) => {
    walkTs(child, visit);
  });
}

function exportedNames(sourceFile) {
  const names = new Set();
  walkTs(sourceFile, (node) => {
    if (ts.isExportDeclaration(node) && node.exportClause && ts.isNamedExports(node.exportClause)) {
      for (const element of node.exportClause.elements) {
        names.add(element.name.text);
      }
      return;
    }
    if (hasModifier(node, ts.SyntaxKind.DefaultKeyword) || !hasModifier(node, ts.SyntaxKind.ExportKeyword)) {
      return;
    }
    if ((ts.isFunctionDeclaration(node) || ts.isClassDeclaration(node) || ts.isInterfaceDeclaration(node) || ts.isTypeAliasDeclaration(node) || ts.isEnumDeclaration(node)) && node.name) {
      names.add(node.name.text);
      return;
    }
    if (ts.isVariableStatement(node)) {
      for (const decl of node.declarationList.declarations) {
        if (ts.isIdentifier(decl.name)) {
          names.add(decl.name.text);
        }
      }
    }
  });
  return names;
}

function membersToProps(sourceFile, members) {
  const props = {};
  for (const member of members) {
    if (!(ts.isPropertySignature(member) || ts.isPropertyDeclaration(member))) {
      continue;
    }
    if (!member.name || !ts.isIdentifier(member.name) || !member.type) {
      continue;
    }
    props[member.name.text] = member.type.getText(sourceFile);
  }
  return props;
}

function propsForExport(sourceFile, exportName) {
  const typeName = `${exportName}Props`;
  let props = null;
  walkTs(sourceFile, (node) => {
    if (props) {
      return;
    }
    if (ts.isInterfaceDeclaration(node) && node.name.text === typeName) {
      props = membersToProps(sourceFile, node.members);
      return;
    }
    if (ts.isTypeAliasDeclaration(node) && node.name.text === typeName && node.type && ts.isTypeLiteralNode(node.type)) {
      props = membersToProps(sourceFile, node.type.members);
    }
  });
  return props;
}

function scriptKindFor(relPath) {
  if (relPath.endsWith(".tsx")) {
    return ts.ScriptKind.TSX;
  }
  if (relPath.endsWith(".jsx")) {
    return ts.ScriptKind.JSX;
  }
  if (relPath.endsWith(".js") || relPath.endsWith(".mjs") || relPath.endsWith(".cjs")) {
    return ts.ScriptKind.JS;
  }
  return ts.ScriptKind.TS;
}

function parseModule(relPath, text) {
  const sourceFile = ts.createSourceFile(relPath, text, ts.ScriptTarget.Latest, true, scriptKindFor(relPath));
  const diagnostics = sourceFile.parseDiagnostics;
  if (diagnostics && diagnostics.length > 0) {
    const diag = diagnostics[0];
    const { line } = sourceFile.getLineAndCharacterOfPosition(diag.start);
    const message = ts.flattenDiagnosticMessageText(diag.messageText, "\n");
    throw parseError(relPath, line + 1, message);
  }
  return sourceFile;
}

const CONDITIONAL_AT_RULES = new Set(["media", "supports", "container"]);

function tokenContext(decl) {
  let current = decl.parent;
  let selector = "";
  const prefixes = [];
  while (current) {
    if (!selector && current.type === "atrule" && current.name === "theme") {
      const params = String(current.params || "").trim();
      return params ? `@theme ${params}` : "@theme";
    }
    if (current.type === "rule") {
      if (!selector) {
        selector = current.selector;
      }
    } else if (current.type === "atrule" && CONDITIONAL_AT_RULES.has(current.name)) {
      const params = String(current.params || "").trim().replace(/\s+/g, " ");
      prefixes.push(`@${current.name}${params ? ` ${params}` : ""}`);
    }
    current = current.parent;
  }
  prefixes.reverse();
  if (prefixes.length === 0) {
    return selector;
  }
  return selector ? `${prefixes.join(" ")} ${selector}` : prefixes.join(" ");
}

function collectCss(relPath, cssText) {
  let root;
  try {
    root = postcss.parse(cssText, { from: relPath });
  } catch (err) {
    const line = err && typeof err.line === "number" ? err.line : 1;
    const message = (err && (err.reason || err.message)) || "css parse error";
    throw parseError(relPath, line, message);
  }
  const tokens = [];
  root.walkDecls((decl) => {
    if (!decl.prop.startsWith("--")) {
      return;
    }
    const line = decl.source && decl.source.start && decl.source.start.line ? decl.source.start.line : 1;
    tokens.push({
      name: decl.prop,
      expression: decl.value,
      context: tokenContext(decl),
      source_ref: `${relPath}:${line}`,
    });
  });
  const fontFaces = [];
  root.walkAtRules(/^font-face$/i, (rule) => {
    const descriptors = {};
    // Read descriptors, never load a URL or import another stylesheet.
    rule.each((node) => {
      if (node.type === "decl") {
        descriptors[node.prop.toLowerCase()] = node.value;
      }
    });
    const sources = [];
    const sourcePattern = /^url\(\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[^\s()"']+)\s*\)(?:\s*format\(\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|[\w-]+)\s*\))?/i;
    for (const candidate of postcss.list.comma(descriptors.src || "")) {
      const match = candidate.trim().match(sourcePattern);
      if (!match) continue;
      const source = { url: match[1] };
      if (match[2]) source.format = match[2];
      sources.push(source);
    }
    const face = {
      family: descriptors["font-family"] || "",
      src: sources,
      weight: descriptors["font-weight"] || "normal",
      style: descriptors["font-style"] || "normal",
      source_path: relPath,
    };
    if (descriptors["unicode-range"]) face.unicode_range = descriptors["unicode-range"];
    if (descriptors["font-display"]) face.display = descriptors["font-display"];
    fontFaces.push(face);
  });
  return { tokens, fontFaces };
}

function pngDimensions(bytes) {
  if (bytes.length < 24 || !bytes.subarray(0, 8).equals(PNG_SIGNATURE)) {
    return { width: null, height: null };
  }
  return {
    width: bytes.readUInt32BE(16),
    height: bytes.readUInt32BE(20),
  };
}

function numericAttr(tag, name) {
  const quoted = tag.match(new RegExp(`\\b${name}\\s*=\\s*(["'])([\\s\\S]*?)\\1`, "i"));
  const raw = quoted ? quoted[2] : (tag.match(new RegExp(`\\b${name}\\s*=\\s*([^\\s>]+)`, "i")) || [])[1];
  if (raw == null) {
    return null;
  }
  const match = String(raw).trim().match(/^[+-]?\d+/);
  if (!match) {
    return null;
  }
  const value = Number(match[0]);
  return Number.isFinite(value) ? value : null;
}

function svgDimensions(bytes) {
  const text = bytes.toString("utf8");
  const tag = text.match(/<svg\b[^>]*>/i);
  if (!tag) {
    return { width: null, height: null };
  }
  return {
    width: numericAttr(tag[0], "width"),
    height: numericAttr(tag[0], "height"),
  };
}

function assetDimensions(relPath, bytes) {
  const ext = path.extname(relPath).toLowerCase();
  if (ext === ".png") {
    return pngDimensions(bytes);
  }
  if (ext === ".svg") {
    return svgDimensions(bytes);
  }
  return { width: null, height: null };
}

function requireVersion1(request) {
  if (request == null || typeof request !== "object" || Array.isArray(request)) {
    throw invalid("request must be an object");
  }
  if (request.request_version !== 1) {
    throw invalid("request_version must be 1");
  }
  if (typeof request.workspace_root !== "string" || request.workspace_root.length === 0 || request.workspace_root.includes("\0")) {
    throw invalid("workspace_root must be a path");
  }
}

function listField(request, name) {
  const value = request[name];
  if (value == null) {
    return [];
  }
  if (!Array.isArray(value)) {
    throw invalid(`${name} must be an array`);
  }
  return value;
}

function maxBytes(request, name, fallback) {
  const value = request[name];
  if (value == null) {
    return fallback;
  }
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) {
    throw invalid(`${name} must be a non-negative number`);
  }
  return n;
}

export function collect(request, io) {
  requireVersion1(request);
  const readFile = io && io.readFile;
  const realpath = io && io.realpath;
  const lstat = io && io.lstat;
  if (typeof readFile !== "function" || typeof realpath !== "function" || typeof lstat !== "function") {
    throw invalid("collect io adapters are required");
  }
  const fsIo = { readFile, realpath, lstat };
  let rootReal;
  try {
    rootReal = realpath(request.workspace_root);
  } catch {
    throw invalid("workspace_root does not exist");
  }
  loadToolchain(rootReal);

  const maxSource = maxBytes(request, "max_source_file_bytes", 1 << 20);
  const maxCss = maxBytes(request, "max_css_bytes", 8 << 20);
  const maxAsset = maxBytes(request, "max_asset_file_bytes", 8 << 20);

  const digestMap = new Map();
  function rememberSource(relPath, bytes) {
    if (!digestMap.has(relPath)) {
      digestMap.set(relPath, sha256Hex(bytes));
    }
  }

  const parsedModules = new Map();
  function loadModule(relPath) {
    if (parsedModules.has(relPath)) {
      return parsedModules.get(relPath);
    }
    const { bytes } = readConfined(relPath, rootReal, fsIo, maxSource);
    rememberSource(relPath, bytes);
    const sourceFile = parseModule(relPath, bytes.toString("utf8"));
    const record = { sourceFile, names: exportedNames(sourceFile) };
    parsedModules.set(relPath, record);
    return record;
  }

  const exportsOut = [];
  for (const row of listField(request, "exports")) {
    if (row == null || typeof row !== "object") {
      throw invalid("exports entries must be objects");
    }
    const exportName = row.export_name;
    const modulePath = row.module_path;
    if (typeof exportName !== "string" || typeof modulePath !== "string") {
      throw invalid("exports require export_name and module_path");
    }
    const { sourceFile, names } = loadModule(modulePath);
    const status = names.has(exportName) ? "verified" : "unresolved";
    const entry = {
      export_name: exportName,
      module_path: modulePath,
      status,
    };
    if (status === "verified") {
      const props = propsForExport(sourceFile, exportName);
      if (props) {
        entry.props = props;
      }
    }
    exportsOut.push(entry);
  }

  const tokensOut = [];
  const fontFacesOut = [];
  for (const relPath of listField(request, "css_inputs")) {
    if (typeof relPath !== "string") {
      throw invalid("css_inputs entries must be strings");
    }
    const { bytes } = readConfined(relPath, rootReal, fsIo, maxCss);
    rememberSource(relPath, bytes);
    const css = collectCss(relPath, bytes.toString("utf8"));
    tokensOut.push(...css.tokens);
    fontFacesOut.push(...css.fontFaces);
  }

  const assetsOut = [];
  for (const relPath of listField(request, "assets")) {
    if (typeof relPath !== "string") {
      throw invalid("assets entries must be strings");
    }
    const { bytes } = readConfined(relPath, rootReal, fsIo, maxAsset);
    const ext = path.extname(relPath).toLowerCase();
    const mime = ALLOWED_ASSET_TYPES[ext] || "";
    const dims = assetDimensions(relPath, bytes);
    assetsOut.push({
      path: relPath,
      mime,
      bytes: bytes.length,
      sha256: sha256Hex(bytes),
      width: dims.width,
      height: dims.height,
      status: "present",
    });
  }

  const sourceDigests = [...digestMap.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([relPath, digest]) => ({ path: relPath, sha256: digest }));

  return {
    collector_version: COLLECTOR_VERSION,
    exports: exportsOut,
    tokens: tokensOut,
    assets: assetsOut,
    // Preserve the v1 collector wire shape for CSS without faces. The normalized
    // inventory producer always emits v2, including an empty font_faces list.
    ...(fontFacesOut.length ? { font_faces: fontFacesOut } : {}),
    source_digests: sourceDigests,
  };
}

function isDirectCli() {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  try {
    return import.meta.url === pathToFileURL(entry).href || import.meta.url === pathToFileURL(path.resolve(entry)).href;
  } catch {
    return false;
  }
}

function writeDiagnostic(diagnostic) {
  process.stderr.write(`${JSON.stringify(diagnostic)}\n`);
}

function runCli() {
  if (!process.argv.slice(2).includes("--request-stdin")) {
    writeDiagnostic({ code: "invalid_request", message: "usage: node collect-design-inventory.mjs --request-stdin" });
    process.exitCode = 2;
    return;
  }
  let raw;
  try {
    raw = fs.readFileSync(0, "utf8");
  } catch (err) {
    writeDiagnostic({ code: "invalid_request", message: String(err && err.message ? err.message : err) });
    process.exitCode = 2;
    return;
  }
  let request;
  try {
    request = JSON.parse(raw);
  } catch (err) {
    writeDiagnostic({ code: "invalid_request", message: String(err && err.message ? err.message : "invalid json") });
    process.exitCode = 2;
    return;
  }
  try {
    const result = collect(request, {
      readFile: fs.readFileSync,
      realpath: fs.realpathSync,
      lstat: fs.lstatSync,
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
    process.exitCode = 0;
    return;
  } catch (err) {
    if (err instanceof CollectError) {
      writeDiagnostic(err.diagnostic);
      process.exitCode = err.exitCode;
      return;
    }
    writeDiagnostic({ code: "invalid_request", message: String(err && err.message ? err.message : err) });
    process.exitCode = 2;
  }
}

if (isDirectCli()) {
  runCli();
}
