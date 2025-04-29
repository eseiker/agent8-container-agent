import type { ClientMessage, ServerMessage } from "../../types/types.ts";

export class TestClient {
  private ws: WebSocket;
  private messageId = 0;

  constructor(url: string) {
    this.ws = new WebSocket(url);
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws.onopen = () => resolve();
      this.ws.onerror = (error) => reject(error);
    });
  }

  async send(message: Omit<ClientMessage, "id">): Promise<ServerMessage> {
    const id = (this.messageId++).toString();
    const fullMessage: ClientMessage = { ...message, id };

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("Message timeout"));
      }, 5000);

      const handler = (event: MessageEvent) => {
        const response = JSON.parse(event.data) as ServerMessage;
        if (response.id === id) {
          clearTimeout(timeout);
          this.ws.removeEventListener("message", handler);
          resolve(response);
        }
      };

      this.ws.addEventListener("message", handler);
      this.ws.send(JSON.stringify(fullMessage));
    });
  }

  close(): void {
    this.ws.close();
  }
}
