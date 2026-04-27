import { createReadStream, createWriteStream, promises as fsp } from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";

export interface FilePart {
  filePath: string;
  fileName: string;
  size: number;
  index: number; // 1-based
  total: number;
}

/**
 * Splits a file into N pieces at most `chunkSize` bytes each.
 * Each part is written to its own file and named like:
 *   `<base>.part01of03<ext>`
 */
export async function splitFile(opts: {
  filePath: string;
  baseName: string;
  destDir: string;
  chunkSize: number;
  onProgress?: (bytesWritten: number, totalBytes: number) => void;
}): Promise<FilePart[]> {
  const { filePath, baseName, destDir, chunkSize, onProgress } = opts;
  const stat = await fsp.stat(filePath);
  const totalSize = stat.size;
  const partCount = Math.max(1, Math.ceil(totalSize / chunkSize));

  const ext = path.extname(baseName);
  const stem = path.basename(baseName, ext);
  const pad = String(partCount).length;

  const parts: FilePart[] = [];
  let written = 0;

  for (let i = 0; i < partCount; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize - 1, totalSize - 1);
    const partName = `${stem}.part${String(i + 1).padStart(pad, "0")}of${String(partCount).padStart(pad, "0")}${ext}`;
    const partPath = path.join(destDir, partName);

    const reader = createReadStream(filePath, { start, end });
    const writer = createWriteStream(partPath);

    if (onProgress) {
      reader.on("data", (chunk: Buffer | string) => {
        written += Buffer.byteLength(chunk);
        onProgress(written, totalSize);
      });
    }

    await pipeline(reader, writer);

    const partStat = await fsp.stat(partPath);
    parts.push({
      filePath: partPath,
      fileName: partName,
      size: partStat.size,
      index: i + 1,
      total: partCount,
    });
  }

  return parts;
}
