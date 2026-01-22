import type { WebClient } from "@slack/web-api";
import { describe, expect, it, vi } from "vitest";

import { loadConfig } from "../config/config.js";
import { loadWebMedia } from "../web/media.js";
import { sendMessageSlack } from "./send.js";

vi.mock("../config/config.js", () => ({
  loadConfig: vi.fn(),
}));

vi.mock("../web/media.js", () => ({
  loadWebMedia: vi.fn(),
}));

const loadConfigMock = vi.mocked(loadConfig);
const loadWebMediaMock = vi.mocked(loadWebMedia);

describe("slack send", () => {
  it("omits filetype in files.uploadV2 payload", async () => {
    loadConfigMock.mockReturnValue({ channels: { slack: {} } } as never);
    loadWebMediaMock.mockResolvedValue({
      buffer: Buffer.from("data"),
      contentType: "image/png",
      fileName: "test.png",
      kind: "image",
    });

    const uploadV2 = vi.fn().mockResolvedValue({ files: [{ id: "F123" }] });
    const postMessage = vi.fn().mockResolvedValue({ ts: "123.456" });
    const client = {
      files: { uploadV2 },
      chat: { postMessage },
    } as unknown as WebClient;

    const result = await sendMessageSlack("channel:C123", "hello", {
      mediaUrl: "https://example.com/test.png",
      token: "xoxb-test",
      client,
    });

    expect(uploadV2).toHaveBeenCalledTimes(1);
    const payload = uploadV2.mock.calls[0]?.[0] as Record<string, unknown>;
    expect(payload).toMatchObject({
      channel_id: "C123",
      filename: "test.png",
      initial_comment: "hello",
    });
    expect("filetype" in payload).toBe(false);
    expect(result).toEqual({ messageId: "F123", channelId: "C123" });
  });
});
