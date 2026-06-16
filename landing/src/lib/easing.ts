export const clamp01 = (x: number) => Math.min(1, Math.max(0, x));

export const easeOutExpo = (x: number) =>
  x >= 1 ? 1 : 1 - Math.pow(2, -10 * x);

export const easeInOutCubic = (x: number) =>
  x < 0.5 ? 4 * x * x * x : 1 - Math.pow(-2 * x + 2, 3) / 2;
