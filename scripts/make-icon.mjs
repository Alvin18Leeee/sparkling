// 生成 Sparkling 应用图标：四角星（与前端品牌标同形）青色 + 透明底，
// 光栅化后打包为多尺寸 PNG-in-ICO。零依赖（node:zlib 内建）。
// 用法：node scripts/make-icon.mjs  →  覆盖 src-tauri/icons/icon.ico
import { deflateSync } from 'node:zlib';
import { writeFileSync } from 'node:fs';

// ── 四角星（单位坐标，与 src/App.tsx 的 SVG 同形） ────────────────
const PTS = [
  [0.5, 0], [0.60625, 0.39375], [1, 0.5], [0.60625, 0.60625],
  [0.5, 1], [0.39375, 0.60625], [0, 0.5], [0.39375, 0.39375],
];
// 略内缩留边，避免尖端贴边被裁
const SCALE = 0.94, OFF = 0.03;

function insideStar(x, y) {
  const px = (x - OFF) / SCALE, py = (y - OFF) / SCALE;
  let c = false;
  for (let i = 0, j = PTS.length - 1; i < PTS.length; j = i++) {
    const [xi, yi] = PTS[i], [xj, yj] = PTS[j];
    if ((yi > py) !== (yj > py) && px < ((xj - xi) * (py - yi)) / (yj - yi) + xi) c = !c;
  }
  return c;
}

// ── 光栅化（4×4 超采样抗锯齿） ────────────────────────────────────
const SPARK = [86, 204, 242]; // #56CCF2
function rasterize(size) {
  const rgba = new Uint8Array(size * size * 4);
  const SS = 4, step = 1 / SS;
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      let cov = 0;
      for (let sy = 0; sy < SS; sy++) {
        for (let sx = 0; sx < SS; sx++) {
          if (insideStar((x + (sx + 0.5) * step) / size, (y + (sy + 0.5) * step) / size)) cov++;
        }
      }
      const i = (y * size + x) * 4;
      rgba[i] = SPARK[0]; rgba[i + 1] = SPARK[1]; rgba[i + 2] = SPARK[2];
      rgba[i + 3] = Math.round((cov / (SS * SS)) * 255);
    }
  }
  return rgba;
}

// ── 极简 PNG 编码器（RGBA8） ──────────────────────────────────────
const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();
function crc32(buf) {
  let c = 0xffffffff;
  for (const b of buf) c = CRC_TABLE[(c ^ b) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}
function chunk(type, data) {
  const out = Buffer.alloc(8 + data.length + 4);
  out.writeUInt32BE(data.length, 0);
  out.write(type, 4, 'ascii');
  data.copy(out, 8);
  out.writeUInt32BE(crc32(Buffer.concat([Buffer.from(type, 'ascii'), data])), 8 + data.length);
  return out;
}
function encodePng(size, rgba) {
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(size, 0);
  ihdr.writeUInt32BE(size, 4);
  ihdr[8] = 8; ihdr[9] = 6; // 8bit RGBA
  const raw = Buffer.alloc(size * (size * 4 + 1));
  for (let y = 0; y < size; y++) {
    raw[y * (size * 4 + 1)] = 0; // filter: none
    Buffer.from(rgba.buffer, y * size * 4, size * 4).copy(raw, y * (size * 4 + 1) + 1);
  }
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk('IHDR', ihdr),
    chunk('IDAT', deflateSync(raw, { level: 9 })),
    chunk('IEND', Buffer.alloc(0)),
  ]);
}

// ── ICO 组装（PNG 条目，Vista+ 通用） ─────────────────────────────
const SIZES = [16, 24, 32, 48, 64, 128, 256];
const pngs = SIZES.map((s) => encodePng(s, rasterize(s)));
const header = Buffer.alloc(6);
header.writeUInt16LE(1, 2); // type: icon
header.writeUInt16LE(SIZES.length, 4);
const entries = Buffer.alloc(SIZES.length * 16);
let offset = header.length + entries.length;
SIZES.forEach((s, i) => {
  const e = i * 16;
  entries[e] = s === 256 ? 0 : s;
  entries[e + 1] = s === 256 ? 0 : s;
  entries.writeUInt16LE(1, e + 4);  // planes
  entries.writeUInt16LE(32, e + 6); // bpp
  entries.writeUInt32LE(pngs[i].length, e + 8);
  entries.writeUInt32LE(offset, e + 12);
  offset += pngs[i].length;
});
const ico = Buffer.concat([header, entries, ...pngs]);
writeFileSync(new URL('../src-tauri/icons/icon.ico', import.meta.url), ico);
console.log(`icon.ico 写入完成: ${SIZES.join('/')} 共 ${ico.length} 字节`);
