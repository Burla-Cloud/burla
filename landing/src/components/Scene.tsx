import { Canvas } from "@react-three/fiber";
import { CosmicField } from "../three/CosmicField";
import { CameraRig } from "../three/CameraRig";
import { Effects } from "../three/Effects";

const BACKGROUND = "#04070a";

type Props = {
  reducedMotion: boolean;
};

export function Scene({ reducedMotion }: Props) {
  return (
    <Canvas
      className="!absolute inset-0"
      dpr={[1, 2]}
      gl={{ antialias: false, powerPreference: "high-performance" }}
      camera={{ position: [0, 0.1, 1.7], fov: 42, near: 0.1, far: 220 }}
    >
      <color attach="background" args={[BACKGROUND]} />
      <CosmicField reducedMotion={reducedMotion} background={BACKGROUND} />
      <CameraRig reducedMotion={reducedMotion} />
      <Effects reducedMotion={reducedMotion} />
    </Canvas>
  );
}
