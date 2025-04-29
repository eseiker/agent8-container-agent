import { z } from "zod";

// Config schema
export const ContainerConfigSchema = z.object({
  port: z.number(),
  workdirName: z.string(),
  coep: z.string().optional(),
  forwardPreviewErrors: z.boolean().optional(),
});

export type ContainerConfigType = z.infer<typeof ContainerConfigSchema>;

// Operation types
export const FileSystemOperationTypes = [
  "readFile",
  "writeFile",
  "rm",
  "readdir",
  "mkdir",
  "stat",
  "watch",
] as const;

export type FileSystemOperation = {
  type: (typeof FileSystemOperationTypes)[number];
  path: string;
  content?: string | Uint8Array;
  options?: {
    encoding?: string;
    recursive?: boolean;
    withFileTypes?: boolean;
    force?: boolean;
    persistent?: boolean;
  };
};

export const ProcessOperationTypes = ["spawn", "input", "kill", "resize"] as const;

export type ProcessOperation = {
  type: (typeof ProcessOperationTypes)[number];
  command?: string;
  args?: string[];
  data?: string;
  pid?: number;
  dimensions?: {
    cols: number;
    rows: number;
  };
};

export const PreviewOperationTypes = ["server-ready", "port", "preview-message"] as const;

export type PreviewOperation = {
  type: (typeof PreviewOperationTypes)[number];
  data?: {
    port?: number;
    previewId?: string;
    error?: string;
    message?: string;
    stack?: string;
    pathname?: string;
    search?: string;
    hash?: string;
  };
};

export const WatchOperationTypes = ["watch-paths", "stop"] as const;

export type WatchOperation = {
  type: (typeof WatchOperationTypes)[number];
  path?: string;
  options?: {
    include?: string[];
    exclude?: string[];
    includeContent?: boolean;
    persistent?: boolean;
  };
};

export const AuthOperationTypes = ["auth", "login", "logout"] as const;

export type AuthOperation = {
  type: (typeof AuthOperationTypes)[number];
  token?: string;
};

export type ContainerRequest =
  | FileSystemOperation
  | ProcessOperation
  | PreviewOperation
  | WatchOperation
  | AuthOperation;

// Response types
export interface ContainerResponse<T = void> {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
}

// Message types
export interface ClientMessage {
  type:
    | (typeof FileSystemOperationTypes)[number]
    | (typeof ProcessOperationTypes)[number]
    | (typeof PreviewOperationTypes)[number]
    | (typeof WatchOperationTypes)[number]
    | (typeof AuthOperationTypes)[number];
  id: string;
  operation: ContainerRequest;
}

export interface ServerMessage extends ContainerResponse<unknown> {
  id: string;
}
