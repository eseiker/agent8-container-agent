import type { ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import type { FSWatcher as NodeFileSystemWatcher } from "node:fs";
import { type Socket, io } from "socket.io-client";
import type {
  Container,
  ContainerOptions,
  ContainerProcess,
  ErrorListener,
  FileEntry,
  FileSystem,
  FileSystemTree,
  FileSystemWatcher,
  PathWatcherEvent,
  PortListener,
  PreviewMessage,
  PreviewMessageListener,
  ServerReadyListener,
  ShellOptions,
  ShellSession,
  SpawnOptions,
  Terminal,
  Unsubscribe,
  WatchPathsOptions,
} from "../types/interfaces.ts";

class FileWatcherImpl implements FileSystemWatcher {
  private listeners: Map<string, Set<(event: PathWatcherEvent) => void>> = new Map();
  private readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
  }

  addEventListener(event: string, listener: (event: PathWatcherEvent) => void): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    const listeners = this.listeners.get(event);
    if (listeners) {
      listeners.add(listener);
      const wrappedListener = (data: unknown) => {
        if (data && typeof data === "object" && "type" in data) {
          listener(data as PathWatcherEvent);
        }
      };
      this.socket.on(`fs:${event}`, wrappedListener);
    }
  }

  close(): void {
    for (const [event, listeners] of this.listeners.entries()) {
      for (const listener of listeners) {
        this.socket.off(`fs:${event}`, listener);
      }
    }
    this.listeners.clear();
  }
}

export class ContainerAgentImpl implements Container {
  private readonly processes: Map<number, ChildProcess> = new Map();
  private readonly watchers: Map<string, NodeFileSystemWatcher> = new Map();
  private readonly eventListeners: Map<
    string,
    Set<PortListener | ServerReadyListener | PreviewMessageListener | ErrorListener>
  > = new Map();
  private readonly socket: Socket;
  private readonly subscriptions: Set<string> = new Set();
  public readonly workdir: string;
  public readonly fs: FileSystem;
  private readonly config: ContainerOptions;
  private readonly eventEmitter: EventEmitter;

  constructor(options: ContainerOptions) {
    this.socket = io("http://localhost:3000", {
      transports: ["websocket"],
    });
    this.workdir = options.workdirName || "/workspace";
    this.config = options;
    this.eventEmitter = new EventEmitter();
    this.fs = this.createFileSystem();

    this.socket.on("connect", () => {
      console.info("Connected to server");
      // Restore subscriptions
      if (this.subscriptions.size > 0) {
        this.socket.emit("subscribe", Array.from(this.subscriptions));
      }
    });

    this.socket.on("disconnect", () => {
      console.info("Disconnected from server");
    });

    this.socket.on("message", (message: string) => {
      try {
        const response = JSON.parse(message);
        if (response.type === "port" && typeof response.data === "number") {
          this.emit("port", response.data);
        } else if (response.type === "server-ready") {
          this.emit("server-ready");
        } else if (response.type === "preview-message" && typeof response.data === "object") {
          this.emit("preview-message", response.data as PreviewMessage);
        } else if (response.type === "error" && response.data instanceof Error) {
          this.emit("error", response.data);
        }
      } catch (error) {
        console.error("Error parsing message:", error);
      }
    });

    // Set up process stdio event handlers
    this.socket.on("process:stdout", ({ pid, data }: { pid: number; data: string }) => {
      const process = this.processes.get(pid);
      if (process) {
        process.stdout?.emit("data", Buffer.from(data));
      }
      this.eventEmitter.emit(`process:${pid}:stdout`, data);
    });

    this.socket.on("process:stderr", ({ pid, data }: { pid: number; data: string }) => {
      const process = this.processes.get(pid);
      if (process) {
        process.stderr?.emit("data", Buffer.from(data));
      }
      this.eventEmitter.emit(`process:${pid}:stderr`, data);
    });

    this.socket.on("process:exit", ({ pid, code }: { pid: number; code: number | null }) => {
      const process = this.processes.get(pid);
      if (process) {
        process.emit("exit", code);
        this.processes.delete(pid);
        this.unsubscribeFromProcess(pid);
      }
      this.eventEmitter.emit(`process:${pid}:exit`, code);
    });
  }

  private createFileSystem(): FileSystem {
    const socket = this.socket;
    const readFileImpl = async (path: string, encoding?: string): Promise<Uint8Array | string> => {
      const response = await socket.emitWithAck("fs:read", { path, encoding });
      if (encoding) {
        return response.toString();
      }
      return new Uint8Array(response);
    };

    const writeFileImpl = async (
      path: string,
      content: string | Uint8Array,
      options?: { encoding?: string },
    ): Promise<void> => {
      await socket.emitWithAck("fs:write", { path, content, options });
    };

    const mkdirImpl = async (path: string, options?: { recursive?: boolean }): Promise<void> => {
      await socket.emitWithAck("fs:mkdir", { path, options });
    };

    const readdirImpl = async (
      path: string,
      options?: { withFileTypes?: boolean },
    ): Promise<FileEntry[]> => {
      const response = await socket.emitWithAck("fs:readdir", { path, options });
      return response.map((entry: { name: string; isFile: boolean; isDirectory: boolean }) => ({
        name: entry.name,
        isFile: () => entry.isFile,
        isDirectory: () => entry.isDirectory,
      }));
    };

    const rmImpl = async (
      path: string,
      options?: { force?: boolean; recursive?: boolean },
    ): Promise<void> => {
      await socket.emitWithAck("fs:rm", { path, options });
    };

    const watchImpl = (pattern: string, options?: { persistent?: boolean }): FileSystemWatcher => {
      const watcher = new FileWatcherImpl(socket);
      socket.emit("fs:watch", { pattern, options });
      return watcher;
    };

    const _watchPathsImpl = (
      options: WatchPathsOptions,
      callback: (events: PathWatcherEvent[]) => void,
    ): void => {
      socket.emit("fs:watchPaths", { options });
      socket.on("fs:pathWatcherEvent", callback);
    };

    return {
      readFile: readFileImpl as FileSystem["readFile"],
      writeFile: writeFileImpl,
      mkdir: mkdirImpl,
      readdir: readdirImpl,
      rm: rmImpl,
      watch: watchImpl,
      watchPaths: (options: WatchPathsOptions, callback: (events: PathWatcherEvent[]) => void) => {
        socket.emit("fs:watchPaths", { options });
        socket.on("fs:pathWatcherEvent", callback);
      },
    };
  }

  private async subscribeToProcess(pid: number): Promise<void> {
    const subscriptionId = `process:${pid}`;
    this.subscriptions.add(subscriptionId);
    await this.socket.emitWithAck("subscribe", { pid });
  }

  private async unsubscribeFromProcess(pid: number): Promise<void> {
    const subscriptionId = `process:${pid}`;
    this.subscriptions.delete(subscriptionId);
    await this.socket.emitWithAck("unsubscribe", { pid });
  }

  on(
    event: "port" | "server-ready" | "preview-message" | "error",
    listener: PortListener | ServerReadyListener | PreviewMessageListener | ErrorListener,
  ): Unsubscribe {
    if (!this.eventListeners.has(event)) {
      this.eventListeners.set(event, new Set());
    }
    this.eventListeners.get(event)?.add(listener);

    return () => {
      this.eventListeners.get(event)?.delete(listener);
    };
  }

  private emit(event: string, ...args: unknown[]) {
    const listeners = this.eventListeners.get(event);
    if (listeners) {
      for (const listener of listeners) {
        try {
          (listener as (...args: unknown[]) => void)(...args);
        } catch (error) {
          console.error(`Error in ${event} listener:`, error);
        }
      }
    }
  }

  async mount(data: FileSystemTree): Promise<void> {
    await this.socket.emitWithAck("fs:mount", { data });
  }

  async spawn(
    command: string,
    args: string[] = [],
    options?: SpawnOptions,
  ): Promise<ContainerProcess> {
    const response = await this.socket.emitWithAck("process:spawn", {
      type: "spawn",
      command,
      args,
      options,
    });
    if (!response.pid) {
      throw new Error("Process ID not found in response");
    }

    await this.subscribeToProcess(response.pid);

    const writer = new WritableStream<string>({
      write: async (chunk) => {
        await this.socket.emitWithAck("process:input", {
          type: "input",
          pid: response.pid,
          data: chunk,
        });
      },
    });

    const reader = new ReadableStream<string>({
      start: (controller) => {
        this.socket.on("process:stdout", ({ pid, data }) => {
          if (pid === response.pid) {
            controller.enqueue(data);
          }
        });

        this.socket.on("process:stderr", ({ pid, data }) => {
          if (pid === response.pid) {
            controller.enqueue(data);
          }
        });

        this.socket.on("process:exit", ({ pid }) => {
          if (pid === response.pid) {
            controller.close();
            this.unsubscribeFromProcess(pid);
          }
        });
      },
    });

    return {
      input: {
        getWriter: () => writer.getWriter(),
      },
      output: reader,
      exit: new Promise((resolve) => {
        this.socket.once("process:exit", ({ pid, code }) => {
          if (pid === response.pid) {
            resolve(code ?? 0);
          }
        });
      }),
      resize: ({ cols, rows }) => {
        this.socket.emit("process:resize", {
          type: "resize",
          pid: response.pid,
          dimensions: { cols, rows },
        });
      },
    };
  }

  async spawnShell(terminal: Terminal, options?: ShellOptions): Promise<ShellSession> {
    const process = await this.spawn(options?.args?.[0] || "bash", options?.args?.slice(1) || [], {
      terminal: {
        cols: terminal.cols || 80,
        rows: terminal.rows || 24,
      },
    });

    const input = process.input.getWriter();
    const output = process.output;

    terminal.onData((data) => {
      input.write(data);
    });

    const reader = new ReadableStream<string>({
      start: (controller) => {
        const onData = (data: string) => {
          controller.enqueue(data);
          terminal.write(data);
        };

        output.pipeTo(
          new WritableStream({
            write: onData,
          }),
        );
      },
    });

    return {
      process,
      input,
      output: reader,
      ready: Promise.resolve(),
    };
  }

  async cleanup(): Promise<void> {
    // Clean up resources
    for (const [pid, process] of this.processes.entries()) {
      process.kill();
      this.processes.delete(pid);
    }

    // Clean up watchers
    for (const [path, watcher] of this.watchers.entries()) {
      watcher.close();
      this.watchers.delete(path);
    }

    // Disconnect socket
    this.socket.disconnect();

    // Unsubscribe from all processes
    for (const subscription of this.subscriptions) {
      const [type, pid] = subscription.split(":");
      if (type === "process") {
        await this.unsubscribeFromProcess(Number.parseInt(pid));
      }
    }
    this.subscriptions.clear();
  }

  async watch(paths: string[], options: WatchPathsOptions = {}): Promise<FileSystemWatcher> {
    const watcher = new FileWatcherImpl(this.socket);
    await this.socket.emitWithAck("message", {
      type: "watch",
      operation: {
        type: "watch-paths",
        paths,
        options: {
          include: options.include,
          exclude: options.exclude,
          includeContent: options.includeContent,
        },
      },
    });
    return watcher;
  }
}
