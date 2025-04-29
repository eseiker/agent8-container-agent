import { type ChildProcess, spawn } from "node:child_process";
import type { Dirent, FSWatcher as NodeFileSystemWatcher, Stats } from "node:fs";
import { mkdir, readFile, readdir, rm, stat, watch, writeFile } from "node:fs/promises";
import { type IncomingMessage, type ServerResponse, createServer } from "node:http";
import { join } from "node:path";
import process from "node:process";
import { Server as SocketServer } from "socket.io";
import type { ContainerProcess } from "../types/interfaces.ts";
import type {
  AuthOperation,
  ContainerConfigType,
  ContainerRequest,
  ContainerResponse,
  FileSystemOperation,
  PreviewOperation,
  ProcessOperation,
  ServerMessage,
  WatchOperation,
} from "../types/types.ts";
import { getMachineIpMap } from "./fly.ts";

export class ContainerServer {
  private readonly server: SocketServer;
  private readonly processes: Map<number, ChildProcess>;
  private readonly watchers: Map<string, NodeFileSystemWatcher>;
  private readonly previewPorts: Map<string, number>;
  private readonly config: ContainerConfigType;
  private authToken: string | undefined;
  private readonly processSubscriptions: Map<number, Set<string>>;

  private handlePreviewProxy(
    req: IncomingMessage,
    res: ServerResponse,
    target: string,
    rest: string[],
  ) {
    const machinemap = getMachineIpMap();
    const targetUrl = `http://[${machinemap[target]}]:5174/${rest.slice(1).join("/")}`;
    const headers: HeadersInit = {};
    for (const [key, value] of Object.entries(req.headers)) {
      if (value && typeof value === "string") {
        headers[key] = value;
      }
    }

    if (req.method !== "GET" && req.method !== "HEAD") {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const body = Buffer.concat(chunks);
        this.forwardRequest(req, res, targetUrl, headers, body);
      });
    } else {
      this.forwardRequest(req, res, targetUrl, headers);
    }
  }

  private async forwardRequest(
    req: IncomingMessage,
    res: ServerResponse,
    targetUrl: string,
    headers: HeadersInit,
    body?: BodyInit,
  ) {
    try {
      const response = await fetch(targetUrl, {
        method: req.method,
        headers,
        body,
      });
      response.headers.forEach((value, key) => {
        res.setHeader(key, value);
      });
      res.statusCode = response.status;
      const buffer = await response.arrayBuffer();
      res.end(Buffer.from(buffer));
    } catch (_error) {
      res.statusCode = 500;
      res.end("Internal Server Error");
    }
  }

  private handleWebSocketProxy(
    _req: IncomingMessage,
    res: ServerResponse,
    target: string,
    rest: string[],
  ) {
    const machinemap = getMachineIpMap();
    const _targetUrl = `ws://[${machinemap[target]}]:3000/${rest.join("/")}`;
    res.writeHead(400);
    res.end("WebSocket proxy not supported with Socket.IO");
  }

  constructor(config: ContainerConfigType) {
    this.config = config;
    this.processes = new Map();
    this.watchers = new Map();
    this.previewPorts = new Map();
    this.processSubscriptions = new Map();

    console.info("Starting server on port", config.port);

    const httpServer = createServer((req, res) => {
      const { pathname } = new URL(req.url || "", `http://${req.headers.host}`);

      if (pathname.startsWith("/proxy/")) {
        const [, , target, ...rest] = pathname.split("/");
        const isPreview = rest[0] === "preview";

        if (isPreview) {
          this.handlePreviewProxy(req, res, target, rest);
        } else {
          this.handleWebSocketProxy(req, res, target, rest);
        }
      } else {
        res.writeHead(400);
        res.end("Not found");
      }
    });

    this.server = new SocketServer(httpServer, {
      cors: {
        origin: "*",
        methods: ["GET", "POST"],
      },
    });

    this.server.on("connection", (socket) => {
      console.info("Client connected");

      socket.on("subscribe", (topics: string[]) => {
        for (const topic of topics) {
          const [type, pid] = topic.split(":");
          if (type === "process") {
            const processId = Number.parseInt(pid);
            if (!this.processSubscriptions.has(processId)) {
              this.processSubscriptions.set(processId, new Set());
            }
            this.processSubscriptions.get(processId)?.add(socket.id);
          }
        }
      });

      socket.on("unsubscribe", (topic: string) => {
        const [type, pid] = topic.split(":");
        if (type === "process") {
          const processId = Number.parseInt(pid);
          this.processSubscriptions.get(processId)?.delete(socket.id);
        }
      });

      socket.on("message", async (message: string | Buffer) => {
        try {
          const { id, operation } = JSON.parse(message.toString()) as {
            id: string;
            operation: ContainerRequest;
          };

          console.debug(message);

          const { type } = operation;

          let response: ContainerResponse<unknown>;

          switch (type) {
            case "readFile":
            case "writeFile":
            case "rm":
            case "readdir":
            case "mkdir":
            case "stat":
            case "watch":
              response = await this.handleFileSystemOperation(operation as FileSystemOperation);
              break;
            case "spawn":
            case "input":
            case "kill":
            case "resize":
              response = await this.handleProcessOperation(operation as ProcessOperation);
              break;
            case "server-ready":
            case "port":
            case "preview-message":
              response = this.handlePreviewOperation(operation as PreviewOperation);
              break;
            case "watch-paths":
            case "stop":
              response = await this.handleWatchOperation(operation as WatchOperation);
              break;
            case "auth":
              response = this.handleAuthOperation(operation as AuthOperation);
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

          const serverMessage: ServerMessage = {
            id,
            ...response,
          };

          socket.emit("message", JSON.stringify(serverMessage));
        } catch (error) {
          const errorResponse: ServerMessage = {
            id: "",
            success: false,
            error: {
              code: "INTERNAL_ERROR",
              message: error instanceof Error ? error.message : "Unknown error",
            },
          };
          socket.emit("message", JSON.stringify(errorResponse));
        }
      });

      socket.on("disconnect", () => {
        console.info("Client disconnected");
        // Clean up subscriptions
        this.processSubscriptions.forEach((subscribers, pid) => {
          subscribers.delete(socket.id);
          if (subscribers.size === 0) {
            this.processSubscriptions.delete(pid);
          }
        });
      });
    });

    httpServer.listen(config.port);
  }

  private async handleFileSystemOperation(
    operation: FileSystemOperation,
  ): Promise<ContainerResponse<{ content: string } | { entries: Dirent[] } | Stats | null>> {
    const fullPath = operation.path.startsWith(this.config.workdirName)
      ? operation.path
      : join(this.config.workdirName, operation.path);

    try {
      switch (operation.type) {
        case "readFile": {
          const content = await readFile(fullPath, {
            encoding: "utf-8",
          });
          return { success: true, data: { content: content.toString() } };
        }
        case "writeFile": {
          if (!operation.content) {
            throw new Error("Content is required for write operation");
          }
          await writeFile(fullPath, operation.content, {
            encoding: "utf-8",
          });
          return { success: true, data: null };
        }
        case "rm": {
          await rm(fullPath, {
            recursive: operation.options?.recursive,
            force: operation.options?.force,
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
        case "watch": {
          if (this.watchers.has(fullPath)) {
            this.watchers.get(fullPath)?.close();
          }
          const watcher = watch(fullPath, {
            persistent: operation.options?.persistent,
            recursive: operation.options?.recursive,
          });
          this.watchers.set(fullPath, watcher as unknown as NodeFileSystemWatcher);
          return { success: true, data: null };
        }
        default:
          throw new Error(`Unsupported file system operation: ${operation.type}`);
      }
    } catch (error) {
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
  ): ContainerResponse<ContainerProcess | null> {
    const { type } = operation;

    switch (type) {
      case "spawn": {
        const { command, args = [] } = operation;
        if (!command) {
          return {
            success: false,
            error: {
              code: "INVALID_OPERATION",
              message: "Command is required for spawn operation",
            },
          };
        }
        return this.spawnProcess(command, args);
      }
      case "input": {
        const { pid, data } = operation;
        if (!(pid && data)) {
          return {
            success: false,
            error: {
              code: "INVALID_OPERATION",
              message: "PID and data are required for input operation",
            },
          };
        }
        return this.sendInput(pid, data);
      }
      case "resize": {
        const { pid, dimensions } = operation;
        if (!(pid && dimensions)) {
          return {
            success: false,
            error: {
              code: "INVALID_OPERATION",
              message: "PID and dimensions are required for resize operation",
            },
          };
        }
        return this.resizeTerminal(pid, dimensions.cols, dimensions.rows);
      }
      case "kill": {
        const { pid } = operation;
        if (!pid) {
          return {
            success: false,
            error: {
              code: "INVALID_OPERATION",
              message: "PID is required for kill operation",
            },
          };
        }
        return this.killProcess(pid);
      }
      default:
        return {
          success: false,
          error: {
            code: "INVALID_OPERATION",
            message: `Invalid process operation type: ${type}`,
          },
        };
    }
  }

  private handlePreviewOperation(operation: PreviewOperation): ContainerResponse<null> {
    try {
      const { type, data } = operation;

      switch (type) {
        case "server-ready":
          return { success: true, data: null };
        case "port": {
          if (data?.port && data?.previewId) {
            this.previewPorts.set(data.previewId, data.port);
          }
          return { success: true, data: null };
        }
        case "preview-message": {
          if (data?.error && this.config.forwardPreviewErrors) {
            process.stderr.write(`Preview error: ${data.error}\n`);
          }
          return { success: true, data: null };
        }
        default:
          throw new Error(`Unsupported preview operation: ${type}`);
      }
    } catch (error) {
      return {
        success: false,
        error: {
          code: "preview_error",
          message: error instanceof Error ? error.message : "Unknown error",
        },
      };
    }
  }

  private handleWatchOperation(
    operation: WatchOperation,
  ): Promise<ContainerResponse<{ watcher: string }>> {
    try {
      const watcherId = Math.random().toString(36).substring(7);
      const path = operation.path || ".";

      if (operation.type === "watch-paths") {
        // Create watcher but don't store it since it's not used
        this.watchFiles(path, operation.options || {});
        return Promise.resolve({
          success: true,
          data: { watcher: watcherId },
        });
      }

      return Promise.resolve({
        success: true,
        data: { watcher: watcherId },
      });
    } catch (error) {
      return Promise.resolve({
        success: false,
        error: {
          code: "WATCH_OPERATION_FAILED",
          message: error instanceof Error ? error.message : "Unknown error occurred",
        },
      });
    }
  }

  private handleAuthOperation(operation: AuthOperation): ContainerResponse<null> {
    try {
      const { type, token } = operation;

      if (type === "auth" && token) {
        this.authToken = token;
        return { success: true, data: null };
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
    for (const [path, watcher] of this.watchers.entries()) {
      watcher.close();
      this.watchers.delete(path);
    }

    // Close the server
    this.server.close();
  }

  private spawnProcess(command: string, args: string[]): ContainerResponse<ContainerProcess> {
    try {
      const childProcess = spawn(command, args, {
        stdio: ["pipe", "pipe", "pipe"],
      });

      const pid = childProcess.pid;
      if (!pid) {
        throw new Error("Failed to get process ID");
      }

      this.processes.set(pid, childProcess);

      // Set up stdio handlers
      childProcess.stdout?.on("data", (data: Buffer) => {
        const subscribers = this.processSubscriptions.get(pid);
        if (subscribers) {
          for (const socketId of subscribers) {
            const socket = this.server.sockets.sockets.get(socketId);
            if (socket) {
              socket.emit("process:stdout", { pid, data: data.toString() });
            }
          }
        }
      });

      childProcess.stderr?.on("data", (data: Buffer) => {
        const subscribers = this.processSubscriptions.get(pid);
        if (subscribers) {
          for (const socketId of subscribers) {
            const socket = this.server.sockets.sockets.get(socketId);
            if (socket) {
              socket.emit("process:stderr", { pid, data: data.toString() });
            }
          }
        }
      });

      childProcess.on("exit", (code: number | null) => {
        const subscribers = this.processSubscriptions.get(pid);
        if (subscribers) {
          for (const socketId of subscribers) {
            const socket = this.server.sockets.sockets.get(socketId);
            if (socket) {
              socket.emit("process:exit", { pid, code });
            }
          }
        }
        this.processes.delete(pid);
        this.processSubscriptions.delete(pid);
      });

      const writer = new WritableStream<string>({
        write: (chunk) => {
          childProcess.stdin?.write(chunk);
        },
        close: () => {
          childProcess.stdin?.end();
        },
      });

      const reader = new ReadableStream<string>({
        start: (controller) => {
          childProcess.stdout?.on("data", (data: Buffer) => {
            controller.enqueue(data.toString());
          });
          childProcess.stderr?.on("data", (data: Buffer) => {
            controller.enqueue(data.toString());
          });
          childProcess.on("exit", () => {
            controller.close();
          });
        },
      });

      return {
        success: true,
        data: {
          input: {
            getWriter: () => writer.getWriter(),
          },
          output: reader,
          exit: new Promise<number>((resolve) => {
            childProcess.on("exit", (code) => {
              resolve(code ?? 0);
            });
          }),
          resize: (_dimensions: { cols: number; rows: number }) => {
            // Implement resize logic here if needed
          },
        },
      };
    } catch (error) {
      return {
        success: false,
        error: {
          code: "SPAWN_ERROR",
          message: error instanceof Error ? error.message : "Failed to spawn process",
        },
      };
    }
  }

  private sendInput(pid: number, data: string): ContainerResponse<null> {
    const process = this.processes.get(pid);
    if (!process) {
      return {
        success: false,
        error: {
          code: "PROCESS_NOT_FOUND",
          message: `Process with PID ${pid} not found`,
        },
      };
    }

    if (!process.stdin) {
      return {
        success: false,
        error: {
          code: "STDIN_NOT_AVAILABLE",
          message: "Process stdin is not available",
        },
      };
    }

    process.stdin.write(data);
    return { success: true, data: null };
  }

  private resizeTerminal(pid: number, _cols: number, _rows: number): ContainerResponse<null> {
    const targetProcess = this.processes.get(pid);
    if (!targetProcess) {
      throw new Error(`Process ${pid} not found`);
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

  private watchFiles(
    path: string,
    options: { recursive?: boolean; persistent?: boolean },
  ): NodeFileSystemWatcher {
    const fullPath = join(this.config.workdirName, path);
    const watcher = watch(fullPath, {
      persistent: options.persistent ?? true,
      recursive: options.recursive,
    });
    return watcher as unknown as NodeFileSystemWatcher;
  }

  private cleanup() {
    for (const watcher of this.watchers.values()) {
      watcher.close();
    }
    this.watchers.clear();
  }
}
