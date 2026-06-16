import { useLayoutEffect, useMemo, useRef } from "react";
import * as THREE from "three";
import { useFrame } from "@react-three/fiber";
import { vertexShader, fragmentShader } from "./shaders";
import { clamp01, easeInOutCubic } from "../lib/easing";

const GRID_X = 64;
const GRID_Y = 5;
const GRID_Z = 64;
const SPACING = 1.0;
const JITTER = 0.34;
const COUNT = GRID_X * GRID_Y * GRID_Z;
const REVEAL_DURATION = 4.2;

const srgb = (hex: string) => new THREE.Color(hex).convertSRGBToLinear();

type Props = {
  reducedMotion: boolean;
  background: string;
};

export function CosmicField({ reducedMotion, background }: Props) {
  const meshRef = useRef<THREE.InstancedMesh>(null!);
  const startTime = useRef<number | null>(null);

  const { positions, seeds, maxDist } = useMemo(() => {
    const positions = new Float32Array(COUNT * 3);
    const seeds = new Float32Array(COUNT);
    let maxDist = 0;
    let i = 0;
    const ox = (GRID_X - 1) / 2;
    const oy = (GRID_Y - 1) / 2;
    const oz = (GRID_Z - 1) / 2;
    for (let x = 0; x < GRID_X; x++) {
      for (let y = 0; y < GRID_Y; y++) {
        for (let z = 0; z < GRID_Z; z++) {
          const px = (x - ox) * SPACING + (Math.random() - 0.5) * JITTER;
          const py = (y - oy) * SPACING + (Math.random() - 0.5) * JITTER;
          const pz = (z - oz) * SPACING + (Math.random() - 0.5) * JITTER;
          positions[i * 3] = px;
          positions[i * 3 + 1] = py;
          positions[i * 3 + 2] = pz;
          seeds[i] = Math.random();
          maxDist = Math.max(maxDist, Math.hypot(px, py, pz));
          i++;
        }
      }
    }
    return { positions, seeds, maxDist };
  }, []);

  const geometry = useMemo(() => {
    const geo = new THREE.BoxGeometry(0.2, 0.2, 0.2);
    geo.setAttribute("aSeed", new THREE.InstancedBufferAttribute(seeds, 1));
    return geo;
  }, [seeds]);

  const uniforms = useMemo(
    () => ({
      uTime: { value: 0 },
      uActivationRadius: { value: reducedMotion ? maxDist * 2 : 0.5 },
      uActiveEdge: { value: 2.6 },
      uColorOff: { value: srgb("#07111a") },
      uColorOn: { value: srgb("#bfeaf2") },
      uEmissive: { value: srgb("#7ECBDD") },
      uBackground: { value: srgb(background) },
      uFogNear: { value: 9 },
      uFogFar: { value: 44 },
    }),
    [reducedMotion, maxDist, background],
  );

  const material = useMemo(
    () =>
      new THREE.ShaderMaterial({
        vertexShader,
        fragmentShader,
        uniforms,
      }),
    [uniforms],
  );

  useLayoutEffect(() => {
    const mesh = meshRef.current;
    const matrix = new THREE.Matrix4();
    for (let i = 0; i < COUNT; i++) {
      matrix.makeTranslation(
        positions[i * 3],
        positions[i * 3 + 1],
        positions[i * 3 + 2],
      );
      mesh.setMatrixAt(i, matrix);
    }
    mesh.instanceMatrix.needsUpdate = true;
  }, [positions]);

  useFrame((state) => {
    uniforms.uTime.value = state.clock.elapsedTime;
    if (reducedMotion) return;
    if (startTime.current === null) startTime.current = state.clock.elapsedTime;
    const t = clamp01((state.clock.elapsedTime - startTime.current) / REVEAL_DURATION);
    uniforms.uActivationRadius.value = 0.5 + easeInOutCubic(t) * (maxDist + 6);
  });

  return (
    <instancedMesh
      ref={meshRef}
      args={[geometry, material, COUNT]}
      frustumCulled={false}
    />
  );
}
