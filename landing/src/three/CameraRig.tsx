import { useEffect, useMemo, useRef } from "react";
import * as THREE from "three";
import { useFrame, useThree } from "@react-three/fiber";
import { clamp01, easeOutExpo } from "../lib/easing";

const REVEAL_DURATION = 4.0;

type Props = {
  reducedMotion: boolean;
};

// Opens extremely close to a single glowing core, then dollies back and up to
// reveal the full lattice. After the reveal, the camera breathes with a slow
// idle drift and gentle cursor parallax.
export function CameraRig({ reducedMotion }: Props) {
  const { camera, pointer } = useThree();
  const startTime = useRef<number | null>(null);

  const startPos = useMemo(() => new THREE.Vector3(0, 0.1, 1.7), []);
  const endPos = useMemo(() => new THREE.Vector3(0, 7.4, 41), []);
  const lookTarget = useMemo(() => new THREE.Vector3(0, -1.2, 0), []);
  const desired = useMemo(() => new THREE.Vector3(), []);

  useEffect(() => {
    camera.position.copy(reducedMotion ? endPos : startPos);
    camera.lookAt(lookTarget);
  }, [camera, reducedMotion, startPos, endPos, lookTarget]);

  useFrame((state, delta) => {
    if (startTime.current === null) startTime.current = state.clock.elapsedTime;
    const elapsed = state.clock.elapsedTime - startTime.current;
    const t = reducedMotion ? 1 : clamp01(elapsed / REVEAL_DURATION);
    const p = easeOutExpo(t);

    desired.lerpVectors(startPos, endPos, p);

    const settle = p * p;
    const drift = Math.sin(state.clock.elapsedTime * 0.16) * 0.7 * settle;
    desired.x += pointer.x * 2.4 * settle + drift;
    desired.y += pointer.y * 1.3 * settle;

    const damp = 1 - Math.pow(0.0016, delta);
    camera.position.lerp(desired, damp);
    camera.lookAt(lookTarget);
  });

  return null;
}
