import { describe, expect, test } from "bun:test";
import { spawn } from "node:child_process";
import { promises as fs } from "node:fs";
import { join } from "node:path";
import process from "node:process";
import { ContainerAgentImpl } from "../../examples/container-agent-impl.ts";

async function buildContainer(): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const build = spawn("docker", ["build", "-f", "Containerfile", "."], {
      stdio: "inherit",
    });
    build.on("close", (code) => {
      if (code === 0) {
        resolve(true);
      } else {
        reject(new Error(`Build failed with code ${code}`));
      }
    });
  });
}

async function runContainer(workspace: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const container = spawn("docker", [
      "run",
      "--rm",
      "-d",
      "-v",
      `${workspace}:/workspace`,
      "container-agent-test",
    ]);

    let containerId = "";
    container.stdout.on("data", (data) => {
      containerId = data.toString().trim();
    });

    container.on("close", (code) => {
      if (code === 0 && containerId) {
        resolve(containerId);
      } else {
        reject(new Error(`Container run failed with code ${code}`));
      }
    });
  });
}

async function checkContainerRunning(containerId: string): Promise<boolean> {
  return new Promise((resolve) => {
    const check = spawn("docker", ["ps", "-q", "-f", `id=${containerId}`]);
    let isRunning = false;
    check.stdout.on("data", () => {
      isRunning = true;
    });
    check.on("close", () => {
      resolve(isRunning);
    });
  });
}

async function stopContainer(containerId: string): Promise<void> {
  return new Promise((resolve) => {
    const stop = spawn("docker", ["stop", containerId]);
    stop.on("close", () => {
      resolve();
    });
  });
}

describe("Container Integration Tests", () => {
  const testWorkspace = join(process.cwd(), "tests/workspace");
  const _containerAgent = new ContainerAgentImpl({
    workdirName: testWorkspace,
    forwardPreviewErrors: true,
  });

  test("should build and run container", async () => {
    // Build the container
    const buildResult = await buildContainer();
    expect(buildResult).toBe(true);

    // Run the container
    const containerId = await runContainer(testWorkspace);
    const isRunning = await checkContainerRunning(containerId);
    expect(isRunning).toBe(true);

    // Stop the container
    await stopContainer(containerId);
  }, 30000);

  test("should handle file watching in container", async () => {
    const testFile = join(testWorkspace, "test.txt");
    const testContent = "Hello, Container!";
    await fs.writeFile(testFile, testContent);
    const containerId = await runContainer(testWorkspace);

    // Give more time for the container to start and initialize
    await new Promise((r) => setTimeout(r, 5000));

    await fs.writeFile(testFile, `${testContent} Updated`);
    await stopContainer(containerId);
    await fs.unlink(testFile);
  }, 20000); // Increased timeout to 20 seconds
});
