import { type ChildProcess, spawn } from "node:child_process";
import type { Dirent, Stats } from "node:fs";
import { glob, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { join, normalize } from "node:path";
import { PortScanner } from "./portScanner/portScanner.ts";
import process from "node:process";
import type { Server, ServerWebSocket } from "bun";
import chokidar, { type FSWatcher } from "chokidar";
import type {
  AuthOperation,
  BufferEncoding,
  ContainerEventMessage,
  ContainerRequest,
  ContainerResponse,
  ContainerResponseWithId,
  FileSystemOperation,
  FileSystemTree,
  ProcessEventMessage,
  ProcessOperation,
  ProcessResponse,
  WatchOperation,
  WatchPathsOperation,
  WatchResponse,
} from "../protocol/src/index.ts";
import { FlyClient, initializeFlyClient } from "./fly";
import type { DirectConnectionData, ProxyData } from "./types.ts";
import { CandidatePort } from "./portScanner";
import { AuthManager } from './auth';

type WebSocketData = ProxyData | DirectConnectionData;

// Type guards
function isProxyConnection(data: WebSocketData): data is ProxyData {
  return data && "targetUrl" in data;
}

function isDirectConnection(data: WebSocketData): data is DirectConnectionData {
  return data && "wsId" in data;
}

// CORS 미들웨어 함수
function corsMiddleware(handler: (req: Request, server?: any) => Promise<Response | undefined> | Response | undefined) {
  return async (req: Request, server?: any) => {
    // OPTIONS 요청 처리
    if (req.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
          'Access-Control-Max-Age': '86400'
        }
      });
    }

    // 실제 요청 처리
    const response = await handler(req, server);
    if (!response) return;

    // CORS 헤더 추가
    const headers = new Headers(response.headers);
    headers.set('Access-Control-Allow-Origin', '*');
    headers.set('Access-Control-Allow-Methods', 'POST, OPTIONS');
    headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization');

    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers
    });
  };
}

export class ContainerServer {
  private readonly server: Server;
  private readonly processes: Map<number, ChildProcess>;
  private readonly fileSystemWatchers: Map<string, FSWatcher>;
  private readonly fileWatchClients: Map<string, Set<ServerWebSocket<unknown>>>;
  private readonly clientWatchers: Map<ServerWebSocket<unknown>, Set<string>>;
  private readonly activeWs: Map<string, ServerWebSocket<WebSocketData>>;
  private readonly portScanner: PortScanner;
  private readonly processClients: Map<number, Set<ServerWebSocket<unknown>>>;
  private readonly config: {
    port: number;
    workdirName: string;
    coep: string;
    forwardPreviewErrors: boolean;
    appHostName: string;
    machineId: string;
  };
  private authToken: string | undefined;
  private appHostName: string;
  private machineId: string;
  private flyClientPromise: Promise<FlyClient>;
  private readonly authManager: AuthManager;

  constructor(config: {
    port: number;
    workdirName: string;
    coep: string;
    forwardPreviewErrors: boolean;
    appHostName: string;
    machineId: string;
  }) {
    this.config = config;
    this.processes = new Map();
    this.fileSystemWatchers = new Map();
    this.activeWs = new Map();
    this.fileWatchClients = new Map();
    this.processClients = new Map();
    this.clientWatchers = new Map();
    this.appHostName = config.appHostName;
    this.machineId = config.machineId;
    this.authManager = new AuthManager({
      authServerUrl: process.env.AUTH_SERVER_URL || 'https://v8-meme-api.verse8.io'
    });

    this.portScanner = new PortScanner({
      scanIntervalMs: 2000,
      enableLogging: false
    });

    this.flyClientPromise = initializeFlyClient({
      apiToken: process.env.FLY_API_TOKEN || '',
      appName: process.env.FLY_APP_NAME || '',
      imageRef: process.env.FLY_IMAGE_REF || '',
    });

    this.portScanner.start().then(() => {
      console.log('스캐너가 시작되었습니다.');
    });

    this.portScanner.on('portAdded', (event: CandidatePort) => {
      console.log("🔓 포트가 열렸습니다!" + event.port);
      const url = `https://${this.appHostName}/proxy/${this.machineId}/preview/?port=${event.port}`;
      const message = JSON.stringify({
        data: {
          success: true,
          data: {
            type: 'port',
            data: { port: event.port, type: 'open', url }
          }
        }
      });

      for (const socket of this.activeWs.values()) {
        socket.send(message);
      }
    });

    this.portScanner.on('portRemoved', (event: CandidatePort) => {
      console.log("🔓 포트가 닫혔습니다!" + event.port);
      const url = `https://${this.appHostName}/proxy/${this.machineId}/preview/?port=${event.port}`;
      const message = JSON.stringify({
        data: {
          success: true,
          data: {
            type: 'port',
            data: { port: event.port, type: 'close', url }
          }
        }
      });

      for (const socket of this.activeWs.values()) {
        socket.send(message);
      }
    });

    console.info("Starting server on port", config.port);

    this.server = globalThis.Bun.serve({
      port: config.port,
      routes: {
        "/api/machine": {
          POST: corsMiddleware(async (req: Request) => {
            const token = this.authManager.extractTokenFromHeader(req.headers.get("authorization"));
            if (!token || !(await this.authManager.verifyToken(token))) {
              return Response.json({ error: "Invalid or missing authorization token" }, { status: 401 });
            }

            const flyClient = await this.flyClientPromise;
            const image = await flyClient.getImageRef();
            if (!image) {
              return Response.json({ error: "No image found" }, { status: 500 });
            }

            const options = {
              name: generateRandomName(),
              image,
              region: "nrt",
              env: {
                FLY_PROCESS_GROUP: "app",
                PORT: "3000",
                PRIMARY_REGION: "nrt"
              },
              services: [
                {
                  protocol: "tcp",
                  internal_port: 3000,
                  ports: [
                    { port: 80, handlers: ["http"] },
                    { port: 443, handlers: ["tls", "http"] }
                  ]
                }
              ],
              resources: {
                cpu_kind: "shared",
                cpus: 1,
                memory_mb: 1024
              },
              restart: {
                policy: "on-failure",
                max_retries: 10
              }
            };

            const machine = await flyClient.createMachine(options, token);
            return Response.json({ machine_id: machine.id });
          }),
          OPTIONS: corsMiddleware((req: Request) => {
            return new Response(null, { status: 204 });
          })
        },
        "/api/machine/:id": {
          GET: corsMiddleware(async (req: Request) => {
            const token = this.authManager.extractTokenFromHeader(req.headers.get("authorization"));
            if (!token || !(await this.authManager.verifyToken(token))) {
              return Response.json({ error: "Invalid or missing authorization token" }, { status: 401 });
            }

            const machineId = (req as any).params.id;

            try {
              const flyClient = await this.flyClientPromise;

              // Get machine status directly from Fly API
              const machine = await flyClient.getMachineStatus(machineId);
              if (!machine) {
                return Response.json({ error: "Machine not found" }, { status: 404 });
              }

              return Response.json({
                success: true,
                machine,
              });
            } catch (error) {
              console.error("Error retrieving machine:", error);
              return Response.json({
                error: "Error occurred while retrieving machine",
                details: error instanceof Error ? error.message : "Unknown error"
              }, { status: 500 });
            }
          }),
          OPTIONS: corsMiddleware((req: Request) => {
            return new Response(null, { status: 204 });
          })
        }
      },
      fetch: corsMiddleware(async (req, server) => {
        const { pathname } = new URL(req.url);

        if (pathname.startsWith("/proxy/")) {
          const url = new URL(req.url);
          const portParam = url.searchParams.get('port');
          const httpPort = portParam ? parseInt(portParam, 10) : 5174;
          const [, , target, ...rest] = pathname.split("/");
          const flyClient = await this.flyClientPromise;
          const ip = await flyClient.getMachineIp(target);
          if (!ip) {
            return new Response("Machine not found", { status: 404 });
          }
          const isPreview = rest[0] === "preview";
          const targetUrl = isPreview
            ? `http://[${ip}]:${httpPort}/${rest.slice(1).join("/")}`
            : `ws://[${ip}]:3000/${rest.join("/")}`;

          if (server.upgrade(req, { data: { targetUrl } })) {
            return;
          }
          if (isPreview) {
            const proxiedResponse = await fetch(targetUrl, {
              method: req.method,
              headers: req.headers,
              body: req.body,
            });
            return new Response(proxiedResponse.body, {
              status: proxiedResponse.status,
              statusText: proxiedResponse.statusText,
              headers: new Headers(proxiedResponse.headers)
            });
          }
        } else if (
          server.upgrade(req, {
            data: {
              wsId: Math.random().toString(36).substring(7),
            },
          })
        ) {
          return;
        }
        return new Response("Upgrade failed", { status: 400 });
      }),
      websocket: {
        message: (ws: ServerWebSocket<WebSocketData>, message) => {
          if (isDirectConnection(ws.data)) {
            this.handleMessage(ws, message);
          } else if (isProxyConnection(ws.data)) {
            ws.data.targetSocket?.send(message);
          }
        },
        open: (ws: ServerWebSocket<WebSocketData>) => {
          // WebSocket connection opened
          // Register websocket based on its type
          if (isDirectConnection(ws.data)) {
            this.activeWs.set(ws.data.wsId, ws);
          } else if (isProxyConnection(ws.data)) {
            const targetUrl = ws.data.targetUrl;
            const targetSocket = new WebSocket(targetUrl);
            ws.data.targetSocket = targetSocket;

            targetSocket.onmessage = (ev) => {
              if (typeof ev.data === "string" || ev.data instanceof Uint8Array) {
                ws.send(ev.data);
              }
            };
            targetSocket.onclose = () => {
              ws.close();
            };
            targetSocket.onerror = () => {
              ws.close();
            };
          }
        },
        close: (ws: ServerWebSocket<WebSocketData>) => {
          // Remove client from all watch lists
          const data = ws.data;
          if (isDirectConnection(data)) {
            this.activeWs.delete(data.wsId);

            if (this.clientWatchers.has(ws)) {
              const watcherIds = this.clientWatchers.get(ws);
              if (watcherIds) {
                for (const watcherId of watcherIds) {
                  const clients = this.fileWatchClients.get(watcherId);
                  if (clients) {
                    clients.delete(ws);

                    if (clients.size === 0) {
                      const fsWatcher = this.fileSystemWatchers.get(watcherId);
                      if (fsWatcher) {
                        fsWatcher.close();
                        this.fileSystemWatchers.delete(watcherId);
                      }
                      this.fileWatchClients.delete(watcherId);
                    }
                  }
                }

                this.clientWatchers.delete(ws);
              }
            }
          }
        },
      },
    });
  }

  private async handleMessage(
    ws: ServerWebSocket<WebSocketData>,
    message: string | Buffer,
  ): Promise<void> {
    console.debug(message);

    try {
      const { id, operation } = JSON.parse(message.toString()) as ContainerRequest;
      const { type } = operation;
      let response: ContainerResponse;

      try {
        switch (type) {
          case "readFile":
          case "writeFile":
          case "rm":
          case "readdir":
          case "mkdir":
          case "stat":
          case "mount":
            response = await this.handleFileSystemOperation(operation);
            break;
          case "spawn":
          case "input":
          case "kill":
          case "resize":
            response = await this.handleProcessOperation(operation, ws);
            break;
          case "watch":
          case "watch-paths":
            response = await this.handleWatchOperation(operation, ws);
            break;
          case "auth":
            response = await this.handleAuthOperation(operation);
            break;
          default:
            response = {
              success: false,
              error: {
                code: "INVALID_OPERATION",
                message: `Invalid operation type: ${type}`,
              },
            };
        }

        const serverResponse: ContainerResponseWithId = {
          id,
          ...response,
        };

        ws.send(JSON.stringify(serverResponse));
      } catch (error) {
        const errorResponse: ContainerResponseWithId = {
          id,
          success: false,
          error: {
            code: "INTERNAL_ERROR",
            message: error instanceof Error ? error.message : "Unknown error",
          },
        };
        ws.send(JSON.stringify(errorResponse));
      }
    } catch (error) {
      console.error(error);
    }
  }

  private async handleFileSystemOperation(
    operation: FileSystemOperation,
  ): Promise<ContainerResponse<{ content: string } | { entries: Dirent[] } | Stats | null>> {
    try {
      const path = operation.path || "";
      const fullPath = ensureSafePath(this.config.workdirName, path);

      switch (operation.type) {
        case "readFile": {
          const content = await readFile(fullPath, {
            encoding: (operation.options?.encoding as BufferEncoding) || "utf-8",
          });
          return { success: true, data: { content: content.toString() } };
        }
        case "writeFile": {
          if (!operation.content) {
            throw new Error("Content is required for write operation");
          }
          const content =
            typeof operation.content === "string"
              ? operation.content
              : new TextDecoder().decode(operation.content);
          await writeFile(fullPath, content, {
            encoding: (operation.options?.encoding as BufferEncoding) || "utf-8",
          });
          return { success: true, data: null };
        }
        case "rm": {
          await rm(fullPath, {
            recursive: operation.options?.recursive,
          });
          return { success: true, data: null };
        }
        case "readdir": {
          const files = await readdir(fullPath, { withFileTypes: true });
          return { success: true, data: { entries: files } };
        }
        case "mkdir": {
          await mkdir(fullPath, {
            recursive: operation.options?.recursive,
          });
          return { success: true, data: null };
        }
        case "stat": {
          const stats = await stat(fullPath);
          return { success: true, data: stats };
        }
        case "mount": {
          let content = operation.content;
          if (!content) {
            throw new Error("Content is required for mount operation");
          }

          if (typeof content !== "string") {
            content = new TextDecoder().decode(content);
          }

          const tree = JSON.parse(content) as FileSystemTree;

          await mount(fullPath, tree);
          return { success: true, data: null };
        }
        default:
          throw new Error(`Unsupported file system operation: ${operation.type}`);
      }
    } catch (error) {
      console.error("error", error);
      return {
        success: false,
        error: {
          code: "FILESYSTEM_OPERATION_FAILED",
          message: error instanceof Error ? error.message : "Unknown error occurred",
        },
      };
    }
  }

  private handleProcessOperation(
    operation: ProcessOperation,
    ws: ServerWebSocket<WebSocketData>,
  ): Promise<ContainerResponse<ProcessResponse | null>> {
    try {
      switch (operation.type) {
        case "spawn": {
          if (!operation.command) {
            throw new Error("Command is required for spawn operation");
          }
          return Promise.resolve(this.spawnProcess(operation.command, operation.args || [], ws));
        }
        case "input": {
          if (!(operation.pid && operation.data)) {
            throw new Error("PID and data are required for input operation");
          }
          return Promise.resolve(this.sendInput(operation.pid, operation.data));
        }
        case "resize": {
          if (!(operation.pid && operation.cols && operation.rows)) {
            throw new Error("PID, cols, and rows are required for resize operation");
          }
          return Promise.resolve(
            this.resizeTerminal(operation.pid, operation.cols, operation.rows),
          );
        }
        case "kill": {
          if (!operation.pid) {
            throw new Error("PID is required for kill operation");
          }
          return Promise.resolve(this.killProcess(operation.pid));
        }
        default:
          throw new Error(`Unsupported process operation type: ${operation.type}`);
      }
    } catch (error) {
      console.error("error", error);
      return Promise.resolve({
        success: false,
        error: {
          code: "PROCESS_OPERATION_FAILED",
          message: error instanceof Error ? error.message : "Unknown error occurred",
        },
      });
    }
  }

  private async handleWatchOperation(
    operation: WatchOperation | WatchPathsOperation,
    ws: ServerWebSocket<WebSocketData>,
  ): Promise<ContainerResponse<WatchResponse>> {
    try {
      const watcherId = Math.random().toString(36).substring(7);

      if (operation.type === "watch-paths") {
        // Handle watch-paths operation
        const options = operation.options || {};
        if (options.include && options.include.length > 0) {
          // Watch included patterns
          for (const pattern of options.include) {
            const fsWatcher = await this.watchFiles(watcherId, pattern, { persistent: true });
            this.fileSystemWatchers.set(watcherId, fsWatcher);
            this.registerWatchClient(watcherId, ws);
          }
        }
      } else {
        for (const pattern of operation.options?.patterns || []) {
          const fsWatcher = await this.watchFiles(watcherId, pattern, operation.options || {});
          this.fileSystemWatchers.set(watcherId, fsWatcher);
          this.registerWatchClient(watcherId, ws);
        }
      }

      return {
        success: true,
        data: { watcherId },
      };
    } catch (error) {
      console.error("error", error);
      return {
        success: false,
        error: {
          code: "WATCH_OPERATION_FAILED",
          message: error instanceof Error ? error.message : "Unknown error occurred",
        },
      };
    }
  }

  private async handleAuthOperation(operation: AuthOperation): Promise<ContainerResponse<null>> {
    try {
      const { type, token } = operation;

      if (type === "auth" && token) {
        if (await this.authManager.verifyToken(token)) {
          this.authToken = token;
          return { success: true, data: null };
        }
        return {
          success: false,
          error: {
            code: "auth_error",
            message: "Invalid token",
          },
        };
      }

      throw new Error(`Unsupported auth operation: ${type}`);
    } catch (error) {
      return {
        success: false,
        error: {
          code: "auth_error",
          message: error instanceof Error ? error.message : "Unknown error",
        },
      };
    }
  }

  stop(): void {
    // Cleanup all processes
    for (const [pid, process] of this.processes.entries()) {
      process.kill();
      this.processes.delete(pid);
    }

    // Cleanup watchers
    this.cleanup();

    // Close the server
    this.server.stop();
  }

  private spawnProcess(
    command: string,
    args: string[],
    ws: ServerWebSocket<WebSocketData>,
  ): ContainerResponse<ProcessResponse> {
    // Use the Node.js PTY wrapper for terminal emulation
    // First try the container path, then fallback to local development path
    let ptyWrapperPath = '/app/pty-wrapper/dist/index.js';

    // Check if file exists using Node.js methods - more reliable across environments
    try {
      require.resolve(ptyWrapperPath);
    } catch (error) {
      // Fallback to local development path
      ptyWrapperPath = join(process.cwd(), 'pty-wrapper/dist/index.js');
    }

    // Default terminal size
    const cols = 80;
    const rows = 24;

    // Create command for PTY wrapper
    const ptyArgs = [
      ptyWrapperPath,
      `--cols=${cols}`,
      `--rows=${rows}`,
      command,
      ...args
    ];

    const childProcess = spawn('node', ptyArgs, {
      cwd: this.config.workdirName,
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env, coep: this.config.coep },
    });

    if (!(childProcess.stdin && childProcess.stdout && childProcess.pid)) {
      throw new Error("Failed to create process streams");
    }

    const { pid } = childProcess;
    const textDecoder = new TextDecoder();

    this.processes.set(pid, childProcess);
    this.registerProcessClient(pid, ws);

    childProcess.stdout.on("data", (chunk) => {
      const decoded = textDecoder.decode(chunk);
      this.notifyProcess(pid, "stdout", decoded);
    });

    childProcess.stderr.on("data", (chunk) => {
      const decoded = textDecoder.decode(chunk);
      this.notifyProcess(pid, "stderr", decoded);
    });

    childProcess.on("exit", (code) => {
      this.notifyProcess(pid, "exit", String(code ?? 0));
      this.processClients.delete(pid);
    });

    return {
      success: true,
      data: {
        pid: childProcess.pid,
      },
    };
  }

  private notifyProcess(pid: number, stream: string, data: string): void {
    const clients = this.processClients.get(pid);

    if (!clients || clients.size === 0) {
      return;
    }

    // Create stdout notification message
    const message: ContainerEventMessage<ProcessEventMessage> = {
      id: `process-${stream}-${Date.now()}`,
      event: "process",
      data: {
        pid,
        stream,
        data,
      },
    };

    // Send stdout notification to all clients watching this process
    for (const client of clients) {
      client.send(JSON.stringify(message));
    }
  }

  private registerProcessClient(pid: number, ws: ServerWebSocket<unknown>): void {
    if (!this.processClients.has(pid)) {
      this.processClients.set(pid, new Set());
    }

    const clients = this.processClients.get(pid);
    clients?.add(ws);
  }

  private sendInput(pid: number, data: string): ContainerResponse<null> {
    const targetProcess = this.processes.get(pid);
    if (!targetProcess) {
      throw new Error(`Process ${pid} not found`);
    }
    if (!targetProcess.stdin) {
      throw new Error(`Process ${pid} has no stdin`);
    }

    targetProcess.stdin.write(data);
    return { success: true, data: null };
  }

  private resizeTerminal(pid: number, cols: number, rows: number): ContainerResponse<null> {
    const targetProcess = this.processes.get(pid);
    if (!targetProcess) {
      throw new Error(`Process ${pid} not found`);
    }

    // Send resize message to the process
    if (targetProcess.send) {
      targetProcess.send({
        type: 'resize',
        cols,
        rows
      });
    }

    return { success: true, data: null };
  }

  private killProcess(pid: number): ContainerResponse<null> {
    const targetProcess = this.processes.get(pid);
    if (!targetProcess) {
      throw new Error(`Process ${pid} not found`);
    }

    targetProcess.kill();
    this.processes.delete(pid);
    return { success: true, data: null };
  }

  private async watchFiles(watcherId: string, pattern: string, options: { persistent?: boolean }): Promise<FSWatcher> {
    const files = await Array.fromAsync(glob(pattern, { cwd: this.config.workdirName }));
    const watcher = chokidar.watch(files, {
      persistent: options.persistent ?? true,
      ignoreInitial: true,
      cwd: this.config.workdirName,
      awaitWriteFinish: {
        stabilityThreshold: 300,
        pollInterval: 100,
      },
    });

    watcher.on("all", (eventName, filePath) => {
      const eventType = this.mapChokidarEventToNodeEvent(eventName);
      const filename = filePath.replace(`${this.config.workdirName}/`, "");

      this.notifyFileChange(watcherId, eventType, filename);
    });

    return watcher;
  }

  private mapChokidarEventToNodeEvent(chokidarEvent: string): string {
    // Map chokidar events to Node.js fs.watch events
    switch (chokidarEvent) {
      case "add":
      case "change":
        return "change";
      case "unlink":
      case "unlinkDir":
        return "rename";
      default:
        return chokidarEvent;
    }
  }

  private notifyFileChange(watcherId: string, eventType: string, filename: string | null): void {
    const clients = this.fileWatchClients.get(watcherId);

    if (!clients || clients.size === 0) {
      return;
    }

    // Create change notification message
    const changeMessage: ContainerEventMessage = {
      id: `watch-${Date.now()}`,
      event: "file-change",
      data: {
        watcherId,
        eventType,
        filename,
      },
    };

    // Send change notification to all clients watching this path
    for (const client of clients) {
      client.send(JSON.stringify(changeMessage));
    }
  }

  private registerWatchClient(watcherId: string, ws: ServerWebSocket<unknown>): void {
    const clients = this.fileWatchClients.get(watcherId);
    if (clients) {
      clients.add(ws);
    } else {
      this.fileWatchClients.set(watcherId, new Set([ws]));
    }

    if (!this.clientWatchers.has(ws)) {
      this.clientWatchers.set(ws, new Set([watcherId]));
    } else {
      this.clientWatchers.get(ws)?.add(watcherId);
    }
  }

  private cleanup() {
    // Abort all watchers
    for (const fsWatcher of this.fileSystemWatchers.values()) {
      fsWatcher.close();
    }
    this.fileSystemWatchers.clear();
    this.fileWatchClients.clear();
    this.clientWatchers.clear();
    this.processClients.clear();
  }
}

export function ensureSafePath(workdir: string, userPath: string): string {
  const normalizedPath = normalize(join(workdir, userPath));
  const normalizedWorkdir = normalize(workdir);

  // Check if the path is within the workspace directory
  if (normalizedPath.startsWith(normalizedWorkdir)) {
    return normalizedPath;
  }

  // Remove dangerous path components and keep the path within workspace
  return join(
    normalizedWorkdir,
    userPath
      .split(/[\/\\]/)
      .filter((segment) => segment !== "..")
      .join("/"),
  );
}

async function mount(mountPath: string, tree: FileSystemTree) {
  await mkdir(mountPath, { recursive: true });

  for (const [name, item] of Object.entries(tree)) {
    const fullPath = join(mountPath, name);

    if ("file" in item) {
      await writeFile(fullPath, item.file.contents);
    } else if ("directory" in item) {
      await mount(fullPath, item.directory);
    }
  }
}

function generateRandomName() {
  // Generate a simple, meaningless 8-character random alphanumeric string
  return Math.random().toString(36).substring(2, 10);
}
