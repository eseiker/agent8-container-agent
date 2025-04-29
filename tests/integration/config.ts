import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import process from "node:process";
import type { ContainerConfigType as ContainerConfig } from "../../types/types.ts";

export const testConfig: ContainerConfig = {
  port: 3000,
  workdirName: join(process.cwd(), "test-workspace"),
  coep: "credentialless",
  forwardPreviewErrors: true,
};

export async function setupTestEnvironment() {
  await mkdir(testConfig.workdirName, { recursive: true });
}
